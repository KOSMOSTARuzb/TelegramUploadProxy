import asyncio
import os
import secrets
import shutil
import urllib.parse
from abc import ABC, abstractmethod
from typing import Optional, Tuple, AsyncGenerator, Dict, Any

import aiofiles
import httpx
import libtorrent

from progress_speed import ProgressStream


class BaseSourceProcessor(ABC):
    """Abstract base class for all file sources (HTTP, Torrent, Local)."""
    def __init__(self, processor_type: str, speed_manager, temp_dir: str = "./downloads"):
        self.processor_type = processor_type
        self.speed_manager = speed_manager
        self.temp_dir = temp_dir
        self.session_id = secrets.token_hex(6)
        os.makedirs(self.temp_dir, exist_ok=True)

    @abstractmethod
    async def prepare(self) -> Tuple[str, Optional[int]]:
        """Inspects the source and returns (filename, total_size)."""
        pass

    @abstractmethod
    async def yield_chunks(self, chunk_size_limit: int) -> AsyncGenerator[Tuple[str, int, bool], None]:
        """
        Processes the source and yields ready-to-upload chunk files.
        Yields: (filepath, part_index, is_last_part)
        """
        pass

    @abstractmethod
    def get_processor_metadata(self) -> Dict[str, Any]:
        """
        Returns a dictionary of metadata fields specific to this processor type.
        This dictionary can be appended to files, written to database logs,
        or dynamically injected into Telegram upload captions.
        """
        pass

    @abstractmethod
    async def close(self):
        """Clean up resources."""
        pass


# --- FUTURE STUBS FOR EXPANSION ---

class TorrentProcessor(BaseSourceProcessor):
    """
    Torrents
    """
    def __init__(self, magnet_url: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("TorrentProcessor", speed_manager, temp_dir)
        self.magnet_url = magnet_url
        self.torrent_session = libtorrent.session()

        # Configure session settings for DHT and Alerts ---
        settings = self.torrent_session.get_settings()
        settings['enable_dht'] = True  # Required for finding trackerless magnet peers
        settings['alert_mask'] = libtorrent.alert_category.all  # Required for read_piece_alert
        self.torrent_session.apply_settings(settings)

        self.torrent_params = libtorrent.parse_magnet_uri(self.magnet_url)
        self.torrent_info_hash = str(self.torrent_params.info_hashes.get_best())
        self.save_path = os.path.join(temp_dir, self.torrent_info_hash)
        self.torrent_params.save_path = self.save_path
        self.torrent_handle = self.torrent_session.add_torrent(self.torrent_params)
        self.torrent_info: Any = None
        self.torrent_files: Any = []
        self.torrent_total_size = -1

    async def prepare(self):
        print("[TorrentProcessor] Retrieving torrent metadata from peers (this can take a moment)...")
        while not self.torrent_handle.status().has_metadata:
            await asyncio.sleep(1)
        self.torrent_info = self.torrent_handle.torrent_file()
        self.torrent_files = self.torrent_info.files()
        print("[TorrentProcessor] Received torrent metadata from peers.")
        self.torrent_total_size = self.torrent_files.total_size()
        return self.torrent_info.name(), self.torrent_total_size

    async def yield_chunks(self, chunk_size_limit: int):
        total_bytes = 0
        part_index = 1

        num_pieces = self.torrent_info.num_pieces()
        piece_length = self.torrent_info.piece_length()

        while total_bytes < self.torrent_total_size:
            part_filepath = os.path.join(self.save_path, f"{self.session_id}.kpart{part_index}")
            start_byte = total_bytes
            end_byte = min(start_byte + chunk_size_limit - 1, self.torrent_total_size - 1)
            chunk_len = end_byte - start_byte + 1
            is_last_part = (end_byte == self.torrent_total_size - 1)

            start_piece = start_byte // self.torrent_info.piece_length()
            end_piece = end_byte // self.torrent_info.piece_length()

            self.speed_manager.download = ProgressStream(total_size=(end_byte - start_byte) + 1)

            print(f"[TorrentProcessor] Downloading Part {part_index}; Pieces: {start_piece}-{end_piece} (Range: {start_byte}-{end_byte})...")

            def pre_allocate_kpart():
                os.makedirs(os.path.dirname(part_filepath), exist_ok=True)
                with open(part_filepath, "wb") as f:
                    f.truncate(chunk_len)

            await asyncio.to_thread(pre_allocate_kpart)

            priorities = [0] * num_pieces
            for p in range(start_piece, end_piece + 1):
                priorities[p] = 4  # Normal download priority
            self.torrent_handle.prioritize_pieces(priorities)

            # Stream and assemble piece-by-piece directly to disk as they complete
            target_pieces = list(range(start_piece, end_piece + 1))
            requested_pieces = set()
            written_pieces = set()

            status_init = self.torrent_handle.status()
            initial_payload = status_init.total_payload_download

            while len(written_pieces) < len(target_pieces):
                # Request memory buffers for newly finished pieces
                for p in target_pieces:
                    if p not in requested_pieces and self.torrent_handle.have_piece(p):
                        self.torrent_handle.read_piece(p)
                        requested_pieces.add(p)

                # Process piece buffer arrivals
                alerts = self.torrent_session.pop_alerts()
                for alert in alerts:
                    if isinstance(alert, libtorrent.read_piece_alert):
                        p = alert.piece

                        # Verify this piece belongs to our active target window
                        if p in target_pieces and p not in written_pieces:
                            piece_data = bytes(alert.buffer)
                            piece_size = alert.size

                            # Calculate the global byte boundaries of this piece
                            piece_start_global = p * piece_length
                            piece_end_global = piece_start_global + piece_size - 1

                            # Find the intersection between the piece and our current chunk boundaries
                            inter_start = max(piece_start_global, start_byte)
                            inter_end = min(piece_end_global, end_byte)

                            if inter_start <= inter_end:
                                # Map the intersection to local coordinates inside the piece buffer
                                slice_local_start = inter_start - piece_start_global
                                slice_local_end = inter_end - piece_start_global
                                slice_bytes = piece_data[slice_local_start: slice_local_end + 1]

                                # Determine the offset within our temporary .kpart file
                                kpart_offset = inter_start - start_byte

                                # Write the slice to the .kpart file on a background thread
                                def write_slice_to_kpart(path, offset, data):
                                    with open(path, "r+b") as f:
                                        f.seek(offset)
                                        f.write(data)

                                await asyncio.to_thread(write_slice_to_kpart, part_filepath, kpart_offset, slice_bytes)

                            written_pieces.add(p)
                            # Garbage collect the piece buffer from memory immediately
                            del piece_data

                # update the speed counter
                status = self.torrent_handle.status()
                downloaded_bytes_realtime = min(status.total_payload_download - initial_payload, chunk_len)
                self.speed_manager.download.update(downloaded_bytes_realtime, increment=False)
                self.speed_manager.display()

                await asyncio.sleep(0.1)

            print(f"[TorrentProcessor] Successfully assembled Part {part_index} at: {part_filepath}")

            # Releasing the storage of libtorrent
            priorities = self.torrent_handle.get_piece_priorities()
            for p in target_pieces:
                priorities[p] = 0
            self.torrent_handle.prioritize_pieces(priorities)

            # Tell libtorrent to close its OS file handles for this torrent
            self.torrent_handle.flush_cache()

            await asyncio.sleep(1)

            def wipe_raw_files_except_kparts():
                if os.path.exists(self.save_path):
                    for item in os.listdir(self.save_path):
                        item_path = os.path.join(self.save_path, item)
                        try:
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                                print(f"[TorrentProcessor] Deleted scratch folder: {item}")
                            else:
                                os.remove(item_path)
                                print(f"[TorrentProcessor] Deleted raw scratch file: {item}")
                        except Exception as e:
                            # Fallback: if OS locks still prevent deletion, truncate file to 0 bytes
                            try:
                                if os.path.isfile(item_path):
                                    with open(item_path, "wb") as f:
                                        f.truncate(0)
                                    print(f"[TorrentProcessor] Truncated locked raw file: {item}")
                                else:
                                    print(f"[TorrentProcessor] Exception while deleting: {item} > {e}")
                            except Exception as ex:
                                print(f"[TorrentProcessor] Could not wipe {item} yet: {ex}")

            # Offload directory scanning and deletion to a background thread
            await asyncio.to_thread(wipe_raw_files_except_kparts)

            yield part_filepath, part_index, is_last_part

            # Increment our global byte cursor and part index
            total_bytes += chunk_len
            part_index += 1

    def get_processor_metadata(self) -> Dict[str, Any]:
        return {
            "magnet_url": self.magnet_url,
            "name": self.torrent_info.name(),
        }

    async def close(self):
        pass


class LocalFileProcessor(BaseSourceProcessor):
    async def prepare(self):
        # TODO: Read local os.path.getsize()
        return "local_video.mp4", 5000000

    async def yield_chunks(self, chunk_size_limit: int):
        # TODO: Split local file and yield chunks
        pass

    async def close(self):
        pass


class HttpProcessor(BaseSourceProcessor):
    """
    HttpProcessor
    """
    def __init__(self, url: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("HttpProcessor", speed_manager, temp_dir)
        self.url = url
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)
        self.supports_ranges = False
        self.filename = None
        self.total_size = None
        self.buffer_size = 64 * 1024

    async def close(self):
        await self.client.aclose()

    def get_processor_metadata(self) -> Dict[str, Any]:
        """Returns HTTP-specific metadata fields."""
        return {
            "url": self.url,
            "supports_ranges": self.supports_ranges,
            "filename": self.filename,
        }

    async def prepare(self) -> Tuple[str, Optional[int]]:
        """Inspects the server for filename, size, and range support."""
        try:
            print('[HttpProcessor] Inspecting headers...')
            response = await self.client.head(self.url)

            # 1. Parse Filename
            content_disp = response.headers.get("content-disposition", "")
            if "filename=" in content_disp:
                self.filename = content_disp.split("filename=")[1].strip('"\'')
            if not self.filename:
                self.filename = os.path.basename(urllib.parse.urlparse(self.url).path) or f"file_{self.session_id}"

            # 2. Parse Size
            content_length = response.headers.get("content-length")
            if content_length:
                self.total_size = int(content_length)

            # 3. Test Range Support
            async with self.client.stream("GET", self.url, headers={"Range": "bytes=0-10"}) as range_resp:
                if range_resp.status_code == 206:
                    self.supports_ranges = True

        except Exception as e:
            print(f"[HttpProcessor] Metadata issue: {e}. Falling back.")
            self.filename = self.filename or f"file_{self.session_id}"

        return self.filename, self.total_size

    async def _download_range(self, filepath: str, start: int, end: int) -> Optional[int]:
        """Internal helper for robust range downloading."""


        # Check if we already have a partial file from a previous interrupted attempt
        existing_bytes = 0
        if os.path.exists(filepath):
            existing_bytes = os.path.getsize(filepath)
            print(f"[HttpProcessor] Found partial file. Resuming from byte {start + existing_bytes}...")

        current_start = start + existing_bytes
        if current_start >= end:
            return existing_bytes

        self.speed_manager.download = ProgressStream(total_size=(end - start) + 1)
        self.speed_manager.download.update(existing_bytes)

        retries = 3
        for attempt in range(retries):
            try:
                async with self.client.stream("GET", self.url, headers={"Range": f"bytes={current_start}-{end}"}) as response:
                    response.raise_for_status()
                    async with aiofiles.open(filepath, "ab") as f:
                        async for chunk in response.aiter_bytes(chunk_size=self.buffer_size):
                            await f.write(chunk)
                            existing_bytes += len(chunk)
                            if self.speed_manager.download:
                                self.speed_manager.download.update(len(chunk))
                                self.speed_manager.display()

                    self.speed_manager.display(force=True)
                    self.speed_manager.download = None
                    print()
                    return existing_bytes
            except Exception as e:
                print(f"[HttpProcessor] Issue on attempt {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2.0)
                if attempt == 2: raise
                await asyncio.sleep(2.0)
        return None

    async def yield_chunks(self, chunk_size_limit: int) -> AsyncGenerator[Tuple[str, int, bool], None]:
        """Encapsulates both Range-supported and Streaming downloads."""

        # --- PATH A: SERVER SUPPORTS RANGE REQUESTS ---
        if self.supports_ranges and self.total_size:
            total_bytes = 0
            part_index = 1
            while total_bytes < self.total_size:
                part_filepath = os.path.join(self.temp_dir, f"{self.filename}.{self.session_id}.kpart{part_index}")
                start_byte = total_bytes
                end_byte = min(start_byte + chunk_size_limit - 1, self.total_size - 1)
                is_last_part = (end_byte == self.total_size - 1)

                print(f"[HttpProcessor] Downloading Part {part_index} (Range: {start_byte}-{end_byte})...")
                total_bytes += await self._download_range(part_filepath, start_byte, end_byte)

                yield part_filepath, part_index, is_last_part
                part_index += 1

        # --- PATH B: NO RANGE SUPPORT / UNKNOWN SIZE ---
        else:
            print("[HttpProcessor] Streaming continuously...")
            part_index = 1
            part_filepath = os.path.join(self.temp_dir, f"{self.filename}.{self.session_id}.kpart{part_index}")

            async with self.client.stream("GET", self.url) as response:
                response.raise_for_status()
                bytes_written = 0
                f = await aiofiles.open(part_filepath, "wb")
                self.speed_manager.download = ProgressStream(total_size=chunk_size_limit)

                try:
                    async for chunk in response.aiter_bytes(chunk_size=self.buffer_size):
                        await f.write(chunk)
                        bytes_written += len(chunk)
                        self.speed_manager.download.update(len(chunk))
                        self.speed_manager.display()

                        if bytes_written >= chunk_size_limit:
                            self.speed_manager.display(force=True)
                            self.speed_manager.download = None
                            await f.close()

                            yield part_filepath, part_index, False

                            part_index += 1
                            bytes_written = 0
                            part_filepath = os.path.join(self.temp_dir,
                                                         f"{self.filename}.{self.session_id}.kpart{part_index}")
                            f = await aiofiles.open(part_filepath, "wb")
                            self.speed_manager.download = ProgressStream(total_size=chunk_size_limit)
                finally:
                    if not f.closed:
                        await f.close()

                if bytes_written > 0:
                    self.speed_manager.display(force=True)
                    self.speed_manager.download = None
                    yield part_filepath, part_index, True
                else:
                    if os.path.exists(part_filepath): os.remove(part_filepath)