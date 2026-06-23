import asyncio
import os
import secrets
import time
import urllib.parse
from asyncio import Task
from collections import deque
from typing import Optional

import aiofiles
import httpx
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from typing_inspection.typing_objects import NoneType

from config import settings


class ProgressTracker:
    """Handles a single stream of progress (either upload or download)."""

    def __init__(self, total_size: Optional[int] = None, window_seconds: float = 3.0):
        self.total_size = total_size
        self.processed = 0
        self.window_seconds = window_seconds
        self.history = deque()
        self.last_print_time = 0.0

    def update(self, bytes_count: int):
        self.processed += bytes_count
        now = time.monotonic()
        self.history.append((now, self.processed))

        # Prune elements in history older than the sliding window limit
        while self.history and (now - self.history[0][0]) > self.window_seconds:
            self.history.popleft()

    def get_recent_speed(self) -> float:
        """Returns the recent average speed in bytes per second."""
        if len(self.history) < 2:
            return 0.0
        time_diff = self.history[-1][0] - self.history[0][0]
        if time_diff <= 0:
            return 0.0
        return (self.history[-1][1] - self.history[0][1]) / time_diff


class ProgressManager:
    """Manages both Upload and Download trackers simultaneously."""

    def __init__(self, download_size: Optional[int] = None, upload_size: Optional[int] = None):
        self.download = ProgressTracker(download_size) if download_size is not None else None
        self.upload = ProgressTracker(upload_size) if upload_size is not None else None
        self.last_print_time = 0.0

    def display(self, force: bool = False):
        """Prints a throttled progress bar to the terminal to avoid CPU overhead."""
        now = time.monotonic()
        # Throttles printing to a maximum of once every 0.3 seconds
        if not force and (now - self.last_print_time) < 0.3:
            return
        self.last_print_time = now

        output = "\r"

        # Helper to format tracker output
        def format_stream(tracker, label):
            speed = tracker.get_recent_speed() / (1024 * 1024)
            mb = tracker.processed / (1024 * 1024)
            if tracker.total_size and tracker.total_size > 0:
                pct = (tracker.processed / tracker.total_size) * 100
                total = tracker.total_size / (1024 * 1024)
                return f"{label}: {pct:5.1f}% | {mb:7.1f}/{total:7.1f} MB | {speed:6.1f} MB/s"
            return f"{label}: {mb:7.1f} MB | {speed:6.1f} MB/s"

        parts = []
        if self.download:
            parts.append(format_stream(self.download, "DL"))
        if self.upload:
            parts.append(format_stream(self.upload, "UL"))

        print(f"\r{' | '.join(parts)}", end="", flush=True)

class AsyncDownloader:
    def __init__(self, url: str, speed_manager: ProgressManager, temp_dir: str = "./downloads"):
        self.url = url
        self.temp_dir = temp_dir
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)
        self.session_id = secrets.token_hex(6)  # 12-character ID
        self.speed_manager = speed_manager
        os.makedirs(self.temp_dir, exist_ok=True)

    async def close(self):
        await self.client.aclose()

    async def inspect_server(self) -> tuple[str, int | None, bool]:
        """
        Inspects the server to get the filename, total size, and check if it supports HTTP Range requests.
        """
        filename = None
        total_size = None
        supports_ranges = False

        try:
            # 1. Get metadata via HEAD request
            print('sending req.')
            response = await self.client.head(self.url)
            print('got req.')
            content_disp = response.headers.get("content-disposition", "")
            if "filename=" in content_disp:
                parts = content_disp.split("filename=")
                if len(parts) > 1:
                    filename = parts[1].strip('"\'')

            if not filename:
                parsed_url = urllib.parse.urlparse(self.url)
                filename = os.path.basename(parsed_url.path)

            content_length = response.headers.get("content-length")
            if content_length:
                total_size = int(content_length)

            # 2. Actively test Range request support (reliable check)
            # We request just the first byte. If the server returns 206, it supports ranges.
            range_test_headers = {"Range": "bytes=0-10"}
            async with self.client.stream("GET", self.url, headers=range_test_headers) as range_response:
                if range_response.status_code == 206:
                    supports_ranges = True

        except Exception as e:
            print(f"[Downloader] Metadata inspection encountered an issue: {e}. Falling back to defaults.")
            if not filename:
                parsed_url = urllib.parse.urlparse(self.url)
                filename = os.path.basename(parsed_url.path) or f"file_{self.session_id}"

        return filename, total_size, supports_ranges

    async def download_chunk_with_range(self, part_filepath: str, start_byte: int, end_byte: int) -> int | None:
        """
        Downloads a specific byte range with active progress and speed tracking.
        """
        buffer_size = 64 * 1024

        # Check if we already have a partial file from a previous interrupted attempt
        existing_bytes = 0
        if os.path.exists(part_filepath):
            existing_bytes = os.path.getsize(part_filepath)
            print(f"[Downloader] Found partial file. Resuming from byte {start_byte + existing_bytes}...")

        current_start = start_byte + existing_bytes
        if current_start >= end_byte:
            return existing_bytes

        # Calculate total size expected to be fetched during this request session
        total_to_fetch = (end_byte - start_byte) + 1
        self.speed_manager.download = ProgressTracker(total_size=total_to_fetch)
        self.speed_manager.download.update(existing_bytes)

        headers = {"Range": f"bytes={current_start}-{end_byte}"}

        # We wrap in a retry loop for robustness
        retries = 3
        for attempt in range(retries):
            try:
                async with self.client.stream("GET", self.url, headers=headers) as response:
                    response.raise_for_status()

                    # Open in append mode 'ab' to allow resuming
                    async with aiofiles.open(part_filepath, "ab") as f:
                        async for chunk in response.aiter_bytes(chunk_size=buffer_size):
                            await f.write(chunk)
                            existing_bytes += len(chunk)

                            # Update and display the progress
                            self.speed_manager.download.update(len(chunk))
                            self.speed_manager.display()

                    # Force final print on successful completion to show 100%
                    self.speed_manager.display(force=True)
                    print()  # Print a clean newline
                    return existing_bytes

            except (httpx.HTTPError, OSError) as e:
                print(f"[Downloader] Connection issue on attempt {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2.0)


# --- Mock Upload Task (Same as before) ---
async def mock_telegram_upload(file_path: str, part_index: int):
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"[Uploader] Starting upload of Part {part_index} ({file_path}) - {file_size_mb:.2f} MB...")
    simulated_upload_time = secrets.SystemRandom().uniform(4.0, 8.0)
    await asyncio.sleep(simulated_upload_time)
    print(f"[Uploader] Successfully uploaded Part {part_index}!")
    try:
        raise
        os.remove(file_path)
        print(f"[Uploader] Cleaned up: {file_path}")
    except OSError as e:
        print(f"[Uploader] Error deleting {file_path}: {e}")


# --- Pipeline Coordination with Range-Support Intelligence ---
# --- Real Pipeline Coordination ---
async def run_pipeline(bot: TelegramClient, url: str, target_chat: int):
    speed_manager = ProgressManager()
    downloader = AsyncDownloader(url, speed_manager=speed_manager)

    # Initialize and start our real Telethon uploader
    uploader = TelegramUploader(bot, speed_manager=speed_manager, target_chat=target_chat)

    filename, total_size, supports_ranges = await downloader.inspect_server()

    print(f"\nStarting pipeline for: {filename}")
    print(f"Total Size: {f'{total_size / (1024 * 1024):.2f} MB' if total_size else 'Unknown'}")
    print(f"Server Supports Range Requests: {supports_ranges}")
    print(f"Session ID: {downloader.session_id}\n" + "-" * 50)

    active_upload_task: Task | None = None
    part_index = 1
    total_bytes_written = 0

    # --- Helper functions to reduce repetition ---
    async def append_metadata(filepath: str, idx: int):
        """Encodes and appends metadata to the end of the final part file."""
        metadata = (
            f'{{"filename": "{filename}", '
            f'"session_id": "{downloader.session_id}", '
            f'"total_parts": {idx}}}'
        )
        metadata_bytes = metadata.encode("utf-8")
        metadata_len = len(metadata_bytes)
        async with aiofiles.open(filepath, "ab") as f:
            await f.write(metadata_bytes)
            await f.write(metadata_len.to_bytes(4, byteorder="big"))
        print(f"[Pipeline] Appended metadata to final Part {idx}")

    def handle_single_part(_part_filepath: str):
        """Renames a single-part file back to its original name and returns the path and caption."""
        _final_path = os.path.join(downloader.temp_dir, filename)
        os.rename(_part_filepath, _final_path)
        _caption = (
            f"📁 **File:** `{filename}`\n"
            f"📊 **Size:** {total_bytes_written / (1024 * 1024):.2f} MB\n"
            f"ℹ️ _Single file - ready to open._"
        )
        return _final_path, _caption

    async def queue_upload(filepath: str, _caption: str, idx: int):
        """Enforces upload limit of 1; awaits active uploads before starting the next."""
        nonlocal active_upload_task
        if active_upload_task:
            print(f"[Pipeline] Waiting for Part {idx - 1} upload to complete...")
            await active_upload_task
        active_upload_task = asyncio.create_task(
            uploader.upload_file(filepath, _caption, idx)
        )

    try:
        if supports_ranges and total_size:
            # --- PATH A: SERVER SUPPORTS RANGE REQUESTS ---
            # We process files chunk-by-chunk with separate connections.
            # No idle sockets are kept open during slow uploads!
            while total_bytes_written < total_size:
                part_filename = f"{filename}.{downloader.session_id}.kpart{part_index}"
                part_filepath = os.path.join(downloader.temp_dir, part_filename)

                # Determine start and end bytes for the current chunk
                start_byte = total_bytes_written
                end_byte = min(start_byte + settings.CHUNK_SIZE_LIMIT - 1, total_size - 1)
                expected_chunk_size = (end_byte - start_byte) + 1
                is_last_part = (end_byte == total_size - 1)

                print(f"[Pipeline] Downloading Part {part_index} (Range: {start_byte}-{end_byte})...")

                # Fetch only this chunk. Connection is opened and immediately closed afterward.
                bytes_downloaded = await downloader.download_chunk_with_range(
                    part_filepath, start_byte, end_byte
                )
                total_bytes_written += bytes_downloaded

                # Prepare the metadata caption for user readability on Telegram
                if is_last_part and part_index == 1:
                    # Case 1: Single-part file. No merging needed.
                    final_path, caption = handle_single_part(part_filepath)
                    await queue_upload(final_path, caption, part_index)
                    break
                else:
                    # Case 2: Multi-part file.
                    caption = f"📦 Part {part_index} of `{filename}`\nSession ID: `{downloader.session_id}`"

                if is_last_part:
                    await append_metadata(part_filepath, part_index)
                    # TODO: What if the last filesize exceeds the limit only after adding the metadata and 4-bit metadata metadata

                await queue_upload(part_filepath, caption, part_index)
                part_index += 1

        else:
            # --- PATH B: NO RANGE SUPPORT / UNKNOWN SIZE ---
            print("[Pipeline] Server does not support Range requests or size is unknown. Streaming continuously...")
            # Start a single continuous streaming request
            async with downloader.client.stream("GET", url) as response:
                response.raise_for_status()
                buffer_size = 64 * 1024
                bytes_written_this_chunk = 0
                part_filepath = os.path.join(downloader.temp_dir,
                                             f"{filename}.{downloader.session_id}.kpart{part_index}")
                # Open the first chunk file
                f_descriptor = await aiofiles.open(part_filepath, "wb")
                speed_manager.download = ProgressTracker(total_size=settings.CHUNK_SIZE_LIMIT)
                try:
                    async for chunk in response.aiter_bytes(chunk_size=buffer_size):
                        await f_descriptor.write(chunk)
                        bytes_written_this_chunk += len(chunk)
                        total_bytes_written += len(chunk)
                        speed_manager.download.update(len(chunk))
                        speed_manager.display()
                        # Once we reach the chunk limit, rotate to the next part
                        if bytes_written_this_chunk >= settings.CHUNK_SIZE_LIMIT:
                            speed_manager.display(force=True)
                            print(f"\n[Pipeline] Reached limit for Part {part_index}. Rotating file...")
                            # Close current chunk
                            await f_descriptor.close()
                            # Enforce the upload queue constraint of 1
                            if active_upload_task:
                                print(
                                    f"[Pipeline] Waiting for Part {part_index - 1} upload to finish before queuing Part {part_index}...")

                            # Queue upload (the await inside queue_upload handles the TCP backpressure pause)
                            caption = f"📦 Part {part_index} of `{filename}`\nSession ID: `{downloader.session_id}`"
                            await queue_upload(part_filepath, caption, part_index)

                            # Prepare for the next part
                            part_index += 1
                            bytes_written_this_chunk = 0
                            part_filepath = os.path.join(downloader.temp_dir,
                                                         f"{filename}.{downloader.session_id}.kpart{part_index}")
                            # Open new chunk file and reset progress tracker
                            f_descriptor = await aiofiles.open(part_filepath, "wb")
                            speed_manager.download = ProgressTracker(total_size=settings.CHUNK_SIZE_LIMIT)

                finally:
                    # Cleanly close the active file descriptor under any circumstances
                    await f_descriptor.close()

                # Handle the final chunk after the stream hits EOF
                if bytes_written_this_chunk > 0:
                    speed_manager.display(force=True)
                    print(
                        f"\n[Pipeline] Stream EOF reached. Final Part {part_index} size: {bytes_written_this_chunk / (1024 * 1024):.2f} MB")

                    if part_index == 1:
                        final_path, caption = handle_single_part(part_filepath)
                        await queue_upload(final_path, caption, part_index)
                    else:
                        await append_metadata(part_filepath, part_index)
                        caption = f"📦 Part {part_index} of `{filename}`\nSession ID: `{downloader.session_id}`"
                        await queue_upload(part_filepath, caption, part_index)
                else:
                    # If EOF was reached exactly on a boundary, we might have an empty file left on disk
                    if os.path.exists(part_filepath):
                        os.remove(part_filepath)

        # Wait for the very last part to finish uploading before shutting down
        if active_upload_task:
            await active_upload_task

        print(f"\n[Pipeline] Complete! Processed {total_bytes_written / (1024 * 1024):.2f} MB in {part_index} part(s).")

    except Exception as e:
        print(f"\n[Pipeline Error] An error occurred: {e}")
    finally:
        await downloader.close()


class TelegramUploader:
    def __init__(self, bot: TelegramClient, speed_manager: ProgressManager, target_chat: str | int):
        self.target_chat = target_chat
        self.bot = bot
        self.speed_manager = speed_manager

    async def upload_file(self, file_path: str, caption: str, part_index: int) -> bool:
        """
        Uploads a local chunk/file to the target Telegram chat with retry logic for rate limits.
        """
        if not os.path.exists(file_path):
            print(f"[Uploader] Error: File {file_path} not found.")
            return False

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        # Initialize the same ProgressTracker we used for downloading
        # For uploads, we specify a slightly larger window (5s) to smooth out MTProto upload bursts
        self.speed_manager.upload = ProgressTracker(total_size=file_size, window_seconds=5.0)

        print(f"[Uploader] Starting upload of Part {part_index} ({file_name})...")

        # Define the callback that Telethon calls periodically during upload
        def progress_callback(current_bytes, total_bytes):
            if isinstance(self.speed_manager.upload, NoneType): return
            delta = current_bytes - self.speed_manager.upload.processed
            self.speed_manager.upload.update(delta)
            self.speed_manager.display()

        # Retry loop to handle FloodWaitError / network issues
        retries = 5
        for attempt in range(retries):
            try:
                # force_document=True ensures files are sent as raw binaries (not compressed media)
                await self.bot.send_file(
                    entity=self.target_chat,
                    file=file_path,
                    caption=caption,
                    force_document=True,
                    progress_callback=progress_callback
                )

                self.speed_manager.display(force=True)
                print(f"\n[Uploader] Successfully uploaded Part {part_index}!")

                # Immediately clean up the disk space
                try:
                    os.remove(file_path)
                    print(f"[Uploader] Cleaned up local file: {file_path}")
                except OSError as e:
                    print(f"[Uploader] Warning: Could not delete {file_path}: {e}")

                return True

            except FloodWaitError as e:
                # Telegram-enforced rate limit protection
                print(
                    f"\n[Uploader] Rate limit hit! Sleeping for {e.seconds} seconds on attempt {attempt + 1}/{retries}...")
                await asyncio.sleep(e.seconds)

            except Exception as e:
                print(f"\n[Uploader] Error during upload on attempt {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(5.0)

        return False
