import asyncio
import os
import secrets
import urllib.parse
from abc import ABC, abstractmethod
from typing import Optional, Tuple, AsyncGenerator

import aiofiles
import httpx

from progress_speed import ProgressStream


class BaseSourceProcessor(ABC):
    """Abstract base class for all file sources (HTTP, Torrent, Local)."""
    def __init__(self, speed_manager, temp_dir: str = "./downloads"):
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
    async def close(self):
        """Clean up resources."""
        pass


# --- FUTURE STUBS FOR EXPANSION ---

class TorrentProcessor(BaseSourceProcessor):
    async def prepare(self):
        # TODO: Initialize libtorrent, get metadata
        return "torrent_file.zip", 1000000

    async def yield_chunks(self, chunk_size_limit: int):
        # TODO: Yield chunks as pieces finish downloading
        pass

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
        super().__init__(speed_manager, temp_dir)
        self.url = url
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)
        self.supports_ranges = False
        self.filename = None
        self.total_size = None
        self.buffer_size = 64 * 1024

    async def close(self):
        await self.client.aclose()

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