import asyncio
import hashlib
import os
import secrets
import shutil
import urllib.parse
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple, AsyncGenerator, Dict, Any

import aiofiles
import httpx
import libtorrent
from pathvalidate import sanitize_filename
# noinspection PyProtectedMember
from pathvalidate._filename import _DEFAULT_MAX_FILENAME_LEN

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
            _filename_last_part = f".{self.session_id}.kpart{part_index}"
            part_filepath = os.path.join(self.temp_dir, str(sanitize_filename(f"{self.torrent_info.name()}", max_len=_DEFAULT_MAX_FILENAME_LEN-len(_filename_last_part)))+_filename_last_part)
            start_byte = total_bytes
            end_byte = min(start_byte + chunk_size_limit - 1, self.torrent_total_size - 1)
            chunk_len = end_byte - start_byte + 1
            is_last_part = (end_byte == self.torrent_total_size - 1)

            start_piece = start_byte // self.torrent_info.piece_length()
            end_piece = end_byte // self.torrent_info.piece_length()

            self.speed_manager.download = ProgressStream(total_size=(end_byte - start_byte) + 1)

            print(f"\n[TorrentProcessor] Downloading Part {part_index}; Pieces: {start_piece}-{end_piece} (Range: {start_byte}-{end_byte})...")

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
                        if self.speed_manager.download and self.speed_manager.download.total_size == self.speed_manager.download.processed:
                            print(f'\n[TorrentProcessor] requested piece {p}({len(requested_pieces)})')
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
                                if self.speed_manager.download and self.speed_manager.download.total_size == self.speed_manager.download.processed:
                                    print(f'\n[TorrentProcessor] wrote piece {p}({len(written_pieces)})')

                            written_pieces.add(p)
                            # Garbage collect the piece buffer from memory immediately
                            del piece_data

                    # --- AUTO-RECOVERY FOR SEEDING READ ERRORS ---
                    elif isinstance(alert, libtorrent.file_error_alert):
                        msg = alert.message()

                        # When libtorrent fails to read a deleted piece for a peer,
                        # the error message will explicitly contain 'reading' or 'read'
                        if "reading" in msg or "read" in msg or 'file_open' in msg:
                            # print(f"\n[TorrentProcessor] Peer requested a deleted piece. Auto-resuming download...")
                            self.torrent_handle.clear_error()
                            self.torrent_handle.resume()
                        else:
                            # If it's a write error (e.g., Disk Full on current chunk), print the true error
                            print(f"\n[TorrentProcessor] Disk Write/Access Error: {msg}")

                # update the speed counter
                status = self.torrent_handle.status()
                downloaded_bytes_realtime = min(status.total_payload_download - initial_payload, chunk_len)
                self.speed_manager.download.update(downloaded_bytes_realtime, increment=False)
                self.speed_manager.display()

                await asyncio.sleep(0.1)

            print(f"\n[TorrentProcessor] Successfully assembled Part {part_index} at: {part_filepath}")
            self.speed_manager.download = None

            # Releasing the storage of libtorrent
            priorities = self.torrent_handle.get_piece_priorities()
            for p in target_pieces:
                priorities[p] = 0
            self.torrent_handle.prioritize_pieces(priorities)

            # Tell libtorrent to close its OS file handles for this torrent
            self.torrent_handle.flush_cache()

            await asyncio.sleep(1)

            def wipe_raw_files():
                if os.path.exists(self.save_path):
                    for item in os.listdir(self.save_path):
                        item_path = os.path.join(self.save_path, item)
                        try:
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                                print(f"\n[TorrentProcessor] Deleted scratch folder: {item}")
                            else:
                                os.remove(item_path)
                                print(f"\n[TorrentProcessor] Deleted raw scratch file: {item}")
                        except Exception as e:
                            # Fallback: if OS locks still prevent deletion, truncate file to 0 bytes
                            try:
                                if os.path.isfile(item_path):
                                    with open(item_path, "wb") as f:
                                        f.truncate(0)
                                    print(f"\n[TorrentProcessor] Truncated locked raw file: {item}")
                                else:
                                    print(f"\n[TorrentProcessor] Exception while deleting: {item} > {e}")
                            except Exception as ex:
                                print(f"\n[TorrentProcessor] Could not wipe {item} yet: {ex}")

            # Offload directory scanning and deletion to a background thread
            await asyncio.to_thread(wipe_raw_files)

            yield part_filepath, part_index, is_last_part

            # Increment our global byte cursor and part index
            total_bytes += chunk_len
            part_index += 1

    def get_processor_metadata(self) -> Dict[str, Any]:
        file_index = []
        files_storage = self.torrent_info.files()
        is_v2 = files_storage.v2()  # Check if this is a v2 or hybrid torrent

        for idx in range(files_storage.num_files()):
            file_hash = None

            if is_v2:
                # Retrieve the SHA-256 Merkle root hash
                root_hash = files_storage.root(idx)
                # Convert to string and verify it's not a dummy zero-hash
                if root_hash and str(root_hash) != "0000000000000000000000000000000000000000000000000000000000000000":
                    file_hash = str(root_hash)
            else:
                # Retrieve the optional SHA-1 file hash
                v1_hash = files_storage.hash(idx)
                if v1_hash and str(v1_hash) != "0000000000000000000000000000000000000000":
                    file_hash = str(v1_hash)

            file_index.append({
                "path": files_storage.file_path(idx),
                "size": files_storage.file_size(idx),
                "is_pad": bool(files_storage.file_flags(idx) & files_storage.flag_pad_file),
                "hash": file_hash  # Hexadecimal string or None
            })

        return {
            "magnet_url": self.magnet_url,
            "name": self.torrent_info.name(),
            "file_index": file_index,
        }

    async def close(self):
        pass


class LocalFileProcessor(BaseSourceProcessor):
    """
    Torrents
    """
    def __init__(self, path: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("LocalFileProcessor", speed_manager, temp_dir)
        self.name = ""
        self.path = path
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"\n[LocalFileProcessor] Path does not exist: {self.path}")

        # Determine if the target is a file or a directory
        self.is_file = os.path.isfile(self.path)
        # Use the parent directory if path is a file, otherwise use the path itself
        self.base_dir = os.path.dirname(self.path) if self.is_file else self.path

        self.files = self._get_path_info()

    def _get_path_info(self):
        if self.is_file:
            return [self._get_file_info(self.path)]

        file_list = []
        # Walk through the directory
        for root, dirs, files in os.walk(self.path):
            for file in files:
                full_path = os.path.join(root, file)
                file_list.append(self._get_file_info(full_path))
        return file_list

    def _get_file_info(self, full_path):
        # Calculate relative path with respect to the base directory
        relative_path = os.path.relpath(full_path, self.base_dir)

        # Get file size
        size = os.path.getsize(full_path)

        # Calculate SHA256 hash
        sha256_hash = hashlib.sha256()
        try:
            with open(full_path, "rb") as f:
                # Read in chunks to handle large files efficiently
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            file_hash = sha256_hash.hexdigest()
        except (PermissionError, OSError):
            file_hash = "ERROR_READING_FILE"
        return {
            "path": relative_path,
            "size": size,
            "hash": file_hash
        }

    async def prepare(self):
        total_size = 0
        for file in self.files:
            total_size += file['size']
        self.name = os.path.basename(self.path.strip('/'))
        return self.name, total_size

    async def yield_chunks(self, chunk_size_limit: int):
        total_size = sum(file['size'] for file in self.files)
        os.makedirs(self.temp_dir, exist_ok=True)

        # Edge Case: The source contains no files or only 0-byte files
        if total_size == 0:
            part_filepath = os.path.join(self.temp_dir, f"{self.name}.{self.session_id}.kpart1")
            # Create an empty file
            await asyncio.to_thread(lambda: open(part_filepath, "wb").close())
            yield part_filepath, 1, True
            return

        total_written_bytes = 0
        part_index = 1
        bytes_written_in_current_part = 0
        part_filepath = os.path.join(self.temp_dir, f"{self.name}.{self.session_id}.kpart{part_index}")

        # Open the first part file asynchronously
        part_f = await asyncio.to_thread(open, part_filepath, "wb")
        buffer_size = 64 * 1024  # Read/write in efficient 64KB increments

        try:
            for file in self.files:
                # Reconstruct full path using base_dir instead of path
                file_path = os.path.join(self.base_dir, file['path'])
                file_size = file['size']

                # Skip empty/pad files since they don't contribute bytes to the virtual stream
                if file_size == 0:
                    continue

                # Open the source file safely
                in_f = await asyncio.to_thread(open, file_path, "rb")
                try:
                    bytes_read_from_file = 0
                    while bytes_read_from_file < file_size:
                        # Determine boundaries
                        space_left_in_part = chunk_size_limit - bytes_written_in_current_part
                        left_in_file = file_size - bytes_read_from_file

                        to_read = min(buffer_size, space_left_in_part, left_in_file)

                        # Read and write on worker threads to avoid blocking the event loop
                        chunk = await asyncio.to_thread(in_f.read, to_read)
                        if not chunk:
                            break  # Unexpected early EOF

                        await asyncio.to_thread(part_f.write, chunk)

                        bytes_read_from_file += len(chunk)
                        bytes_written_in_current_part += len(chunk)
                        total_written_bytes += len(chunk)

                        # Check if our current .kpart file is full
                        if bytes_written_in_current_part >= chunk_size_limit:
                            await asyncio.to_thread(part_f.close)

                            is_last_part = (total_written_bytes >= total_size)
                            yield part_filepath, part_index, is_last_part

                            if is_last_part:
                                break

                            # Set up the next kpart file
                            part_index += 1
                            bytes_written_in_current_part = 0
                            part_filepath = os.path.join(
                                self.temp_dir,
                                f"{self.name}.{self.session_id}.kpart{part_index}"
                            )
                            part_f = await asyncio.to_thread(open, part_filepath, "wb")

                    # Break the outer loop if we've successfully written everything
                    if total_written_bytes >= total_size:
                        break

                finally:
                    await asyncio.to_thread(in_f.close)

            # Close the last part file if it contains residual bytes and is still open
            if part_f and not getattr(part_f, 'closed', True):
                await asyncio.to_thread(part_f.close)
                yield part_filepath, part_index, True

        except Exception as e:
            # Ensure file descriptors are cleaned up in case of a pipeline crash
            if part_f and not getattr(part_f, 'closed', True):
                await asyncio.to_thread(part_f.close)
            raise e

    def get_processor_metadata(self) -> Dict[str, Any]:
        return {
            "root_path": self.path,
            "name": self.name,
            "file_index": self.files,
        }

    async def close(self):
        pass


class HttpProcessor(BaseSourceProcessor):
    """
    HttpProcessor
    """

    def __init__(self, url: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("HttpProcessor", speed_manager, temp_dir)

        # 1. Hardcode the Google Takeout download URL from the curl command
        self.url = (
            "https://takeout-download.usercontent.google.com/download/"
            "takeout-20260629T165836Z-3-001.zip?j=e21e1824-e16c-4823-a762-34334f644912"
            "&i=0&user=841068456632&authuser=0"
        )

        # 2. Hardcode your active Session ID to ensure files have identical names across runs
        self.session_id = "e2c4dfff5961"

        # 3. Hardcode the HTTP headers and cookies from the curl command
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,uz;q=0.8,ru;q=0.7',
            'priority': 'u=0, i',
            'referer': 'https://takeout.google.com/',
            'sec-ch-ua': '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version-list': '"Chromium";v="148.0.7778.167", "Google Chrome";v="148.0.7778.167", "Not/A)Brand";v="99.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Linux"',
            'sec-ch-ua-platform-version': '""',
            'sec-ch-ua-wow64': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-site',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
            'x-browser-channel': 'stable',
            'x-browser-copyright': 'Copyright 2026 Google LLC. All Rights Reserved.',
            'x-browser-validation': 'eJd5Tw+MWpGUJD0D/pwqH9jwh9w=',
            'x-browser-year': '2026',
            'x-client-data': 'CJC2yQEIo7bJAQipncoBCO7kygEIkqHLAQiHoM0BCOzJlDAIxc+UMAj10ZQwCKbUlDA=',
            'cookie': '__Secure-BUCKET=CJsC; SEARCH_SAMESITE=CgQIlaEB; AEC=AdJVEauvL4dIl7vbhitMxHt9j1ajSa_Ju22zKxYALnXHG48-iFCqYe4gfA; __Secure-1PSIDTS=sidts-CjEByojQU_i4r6g7WnCMqTDePqUqBYPtf0CUr_9TXta9OYyi_3HcFxscH_LAaPhuZjqiEAA; __Secure-3PSIDTS=sidts-CjEByojQU_i4r6g7WnCMqTDePqUqBYPtf0CUr_9TXta9OYyi_3HcFxscH_LAaPhuZjqiEAA; __Secure-STRP=ANmZwa2TGr9P0zYoEzXtT_aLNcYkkjmRCWOL5raEL4Fimc4sG6yF9deA0mEcEk5y2so06v7ivjFVBM4tNWROmpoUWEcq6BLa88m1; S=billing-ui-v3=pHtfDFISI3iyYJ1DEBejZaDtcZYUCrV5jVH8ufJZq0s:billing-ui-v3-efe=pHtfDFISI3iyYJ1DEBejZaDtcZYUCrV5jVH8ufJZq0s; NID=532=xFPFgTTtRM6dn41SbgtnpOYaNJqDMOmcGxpMOKn4Ps4NoZLgQwfoxmvbzSEiu-hoYGIjIqeRTSTiF8a-Ia-v9TpGi5inZE0gnKz1FV5PPbwkbC2QS4SsvNr6mpZKSGa2fzBsK4ihbbEnnIWpLflwTidT3ep7FyCxDPmNFHjilQUBVQzK7BhFwewY27BRYzgA0kPd5gto7HTg6EIhPl-wGm2SeXEOBhcHYMtj54sRsb4ov5fsq8FsBKPOiL1fbEy2mmF5F7f0yvrRrrhzREAAB8E21AWGuDPritiCo4yUN5eYmdWXmZvCoSKiFsJBoNHYit656-VKzF9XyBNLAyD15bLnXau47zHOiswRaf9-Tkvty4ZPsfYjTSmPOxDplcM75uNgYuf4P62JlPq4QU5QYI4UgBlXf2HJb4alXuAezoXmNtkD45YiazOykSRZNcmOUhdwhU5p1yNa9wmVrhsamXyGRgFC3EVGlh6W_FBrZUTe5YbPOY1Yj7tCPx8cS5LwZCxJ2wYOVdhnx3L1P6iPedOtzlPlkruuEPiEnd64bIYv0oDmP0LRRS6EP7oWN6R4p3jz47Mg-TaLP_AtqMpeGr8nYQmto2CkUkRoM5tnzWNUlIkd_LDXLHmy1gFmimq-PoDuJGo8RQ3ieKIasOHqS0Z2gzR-2VK7mYgtr2pWco9GM1jEOi52gdO-gVZBcn3HYdqI6n8I92Hqy627rYxmGzgJ9IO9IPgJDBW2XsAtIfyWnwq2Vd0Q1BUI9X2XLSOqIQJw-GDE6vD0l44ewdndyiwlJczGyC7C2ZuXNODvAzsa-3g_oU4deONOtVETut1BzRz8I76h0CweMxXGkYXHb472z0P47rciyWz5i-dsJ357I4kjNhmh2wcadCWmSvcUnO5P143m5siQQEUM-d1vA3yEn3p56vfP043Mgwxo-_5WqJEa_B8kOwdbn4p1jtv3C1Lrwa0D26M-6xxfO7gTSmd5NtIjUkdoAyN-T_EliBny8w9GaLNgt9muFUABYrEf2CAPORWFzIJmwXzbikfm0rrRmj4cGimjTH3femzjSwy6S7mdEJdwX6sWnlnf58dkbyO8JF7tSHOJg51Pw-K84B3o0aUkCVuVJSrVg5Pa8yvg7l6wlDytaNuBsPZZaPhTq_opR0oCXWt12ggoSWUtBAXS_Wlg9_iJzJDJaw4EJJVFCzk2CIXia1vFW_40UdkMUzwX6BNKmA-G_BJpYY9W_sYA2UD8gthjiS9u8sfUa7_uiy7grSxzfZwr1ZoYgh2neUz1CJR3C_BGE36j8vqFL8G6DS3jul0Zt-FXcoD8Aa5F4OnddC8ajzGGSIW4kP-graQ8ej3_h-C7W9H5ZrRA1Prl_S73ts0umXSgIdUWi2qw0eXLBjvBL17lmurJRdXntG0nDRWbKFIEYSjXRwC1qC5DyjjEiADbcLTPbgY15YdeCeIqIBlexJeidRwa3dKooZObOumfPXAtGWrCJvdbDoXRdLEmdxXmdOEnBa0sUU-V1EINixecWjmcvEU4MFzzQ50ZM2KpdbAkEs-7_lUJ9tlEG_U_qaYIOGlFuNYh6wB9ZBdyGcU-6RId92-vVCGt_bHw-ImgOOZVivqR_GjJ0OGN2NhZpzx0zO9HcEJGiQdFpbVaZJMPO6BN7t4acpXf4smCAs5Gif7v_kC_Je2RuRqkECHeNx7sRkIjh2sdqqr3PLJeLdjtqrC2igmxnRkLgps_qi3GqQJ-_LqXJodXDJam-7KONhj0bN3HCVivj5MduoqyeOlvFnbRce7ICzDioAX2xVGAKFciqQn0JsVQk_hYIWwKev1K3cbVb9QW1MH-3f7LWf1Al4P-zw_Xnb8n-rFykmYdg8MdlmukgK3E6FIUPHIxtXZsZ6B9JVkpF9hR-Jlrr9hdvCu8xdPeKnShf6ZMFr32tQq6b74bAi2IwrqiFSEgEvrp4OMiuOHAGg08POZq4MTLVryxk_VP3Q2La-4j55R4vCwKxoG8r8UIhHy96nKGA-yEYR8WFxMCZ4Tx_emRXigFjP_3xmjv8IQLapirC8nZ-jNAas8kBYntG9o447Ki88LO3x8dHqzLNYALiPgAu6ZxsAG0RtZV9YgwGEKkinH40owzPlbe447mOymc14uT4HtBtd4HzSLPNawh-XeNwKTX1UvSKwP4r4DHD7u0TW2azehf0mHKxKyzQPAR4vqcPiNiMMadsTqcptLf16KNoZgb96q_PnhsPbeC13ivBPzjIy9tozrnVU_hEXgKWbvjT_DmkmFgdO18sQvgsFI7J9t1z2fboF6bUeVkVzBr5X6162tbIo8yhppzUDSox-uCl2IVnIzNr9EYZHK0DPXEOiIdqaSkXJYedFSmnJfyNDNUVcj_B3Ch7tbWuGXuQGL-CcWlHQj5Zz9OjDnL9Q; SID=g.a000_gi4KhvXMgPggZGn0bsgzQy3WdOfyOQi7EgjB8VDPS3zrTK40_uihaAqviowPJANy0JYkwACgYKAXQSARcSFQHGX2MiOkrr4VA4_tbuDUCkT2gRxhoVAUF8yKpln0iDTKvTL4ACfAna_Gi70076; __Secure-1PSID=g.a000_gi4KhvXMgPggZGn0bsgzQy3WdOfyOQi7EgjB8VDPS3zrTK4Bx8HjlRwz0z3wT_v_Gc_NgACgYKAbcSARcSFQHGX2Miqx-f4R0TnUIzM55yWLOjehoVAUF8yKoK0i-IGtasHL4sTlD7QG8U0076; __Secure-3PSID=g.a000_gi4KhvXMgPggZGn0bsgzQy3WdOfyOQi7EgjB8VDPS3zrTK4VuHy3R-H98HylwNCOp3tBAACgYKAQQSARcSFQHGX2MilbWykbpW4Wp5Kyeq3LZbLxoVAUF8yKopPQLOYQTqpVNjo2XVZ08w0076; HSID=A3vEgXbdrNxyi1DAi; SSID=ACdhUVkokfHVxBEPD; APISID=XmfGzMLOJv06d3Iw/A0h5ZnzvrzNODH-RL; SAPISID=wVHE4IykvQP-9bKy/AlDykRea-Dv8Y-c9h; __Secure-1PAPISID=wVHE4IykvQP-9bKy/AlDykRea-Dv8Y-c9h; __Secure-3PAPISID=wVHE4IykvQP-9bKy/AlDykRea-Dv8Y-c9h; SIDCC=AKEyXzXrv5HdTiYRwtbmAbbq6u4nBNd-WLP1D6YIwwqIiT6ZRFj8NP7j1mcbpMdZ5fS3we2mNaJT; __Secure-1PSIDCC=AKEyXzWSC_u1vc56kK4TT9hK-p2vnK8yU0nSSG0zvJ34RhPWcDCZGWFhuBfHcmqKszUojwN0des; __Secure-3PSIDCC=AKEyXzVpeLHan3uaPP7tIlOS8IH7CCM8zMN3ziX-FFYkaU1J3Xgv7GX1HNYtaAHHYe_2rm7_xEM'
        }

        self.client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0)
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

            # Force target properties to guarantee HTTP ranges are evaluated
            self.supports_ranges = True
            self.total_size = 33432001557
            self.filename = "takeout-20260629T165836Z-3-001.zip"

            # Connect to server to parse actual metadata dynamically if available
            response = await self.client.head(self.url)

            # 1. Parse Filename
            content_disp = response.headers.get("content-disposition", "")
            if "filename=" in content_disp:
                self.filename = content_disp.split("filename=")[1].strip('"\'')

            # 2. Parse Size
            content_length = response.headers.get("content-length")
            if content_length:
                self.total_size = int(content_length)

        except Exception as e:
            print(f"[HttpProcessor] Metadata issue: {e}. Falling back to default hardcoded details.")
            self.filename = self.filename or "takeout-20260629T165836Z-3-001.zip"
            self.supports_ranges = True
            self.total_size = 33432001557

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
                async with self.client.stream("GET", self.url,
                                              headers={"Range": f"bytes={current_start}-{end}"}) as response:
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

            # Scan local folder to find the lowest incomplete/existing part number.
            # This allows skipping already uploaded/deleted parts upon script restart.
            start_part_index = 1
            existing_parts = []
            if os.path.exists(self.temp_dir):
                for f in os.listdir(self.temp_dir):
                    if f.startswith(f"{self.filename}.{self.session_id}.kpart"):
                        try:
                            part_num = int(f.split(".kpart")[-1])
                            existing_parts.append(part_num)
                        except ValueError:
                            pass

            if existing_parts:
                start_part_index = min(existing_parts)
                print(
                    f"[HttpProcessor] Detected existing parts. Skipping completed ones and resuming pipeline from Part {start_part_index}...")

            total_bytes = 0
            part_index = 1
            while total_bytes < self.total_size:
                part_filepath = os.path.join(self.temp_dir, f"{self.filename}.{self.session_id}.kpart{part_index}")
                start_byte = total_bytes
                end_byte = min(start_byte + chunk_size_limit - 1, self.total_size - 1)
                is_last_part = (end_byte == self.total_size - 1)

                # Skip any part that is earlier than our active resuming part
                if part_index < start_part_index:
                    total_bytes += (end_byte - start_byte) + 1
                    part_index += 1
                    continue

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