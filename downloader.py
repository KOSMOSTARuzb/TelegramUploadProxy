import asyncio
import os
import secrets
import shutil
import time
import urllib.parse
from collections import deque

import httpx
import aiofiles
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from config import settings


class ProgressTracker:
    def __init__(self, total_size: int, already_downloaded: int = 0, window_seconds: float = 3.0):
        self.total_size = total_size
        self.downloaded = already_downloaded
        self.window_seconds = window_seconds
        self.history = deque()  # Stores tuples of (monotonic_timestamp, cumulative_bytes)
        self.last_print_time = 0.0

    def update(self, bytes_count: int):
        self.downloaded += bytes_count
        now = time.monotonic()
        self.history.append((now, self.downloaded))

        # Prune elements in history older than the sliding window limit
        while self.history and (now - self.history[0][0]) > self.window_seconds:
            self.history.popleft()

    def get_recent_speed(self) -> float:
        """Returns the recent average speed in bytes per second."""
        if len(self.history) < 2:
            return 0.0
        first_time, first_bytes = self.history[0]
        last_time, last_bytes = self.history[-1]
        time_diff = last_time - first_time
        if time_diff <= 0:
            return 0.0
        return (last_bytes - first_bytes) / time_diff

    def display(self, force: bool = False):
        """Prints a throttled progress bar to the terminal to avoid CPU overhead."""
        now = time.monotonic()
        # Throttles printing to a maximum of once every 0.3 seconds
        if not force and (now - self.last_print_time) < 0.3:
            return
        self.last_print_time = now

        speed_mb = self.get_recent_speed() / (1024 * 1024)
        downloaded_mb = self.downloaded / (1024 * 1024)

        if self.total_size > 0:
            percentage = (self.downloaded / self.total_size) * 100
            total_mb = self.total_size / (1024 * 1024)
            print(
                f"\r -> {percentage:6.2f}% | {downloaded_mb:8.2f} / {total_mb:8.2f} MB | "
                f"Speed: {speed_mb:6.2f} MB/s",
                end="", flush=True
            )
        else:
            print(f"\r -> {downloaded_mb:8.2f} MB | Speed: {speed_mb:6.2f} MB/s", end="", flush=True)

class AsyncDownloader:
    def __init__(self, url: str, temp_dir: str = "./downloads"):
        self.url = url
        self.temp_dir = temp_dir
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)
        self.session_id = secrets.token_hex(6)  # 12-character ID
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
            response = await self.client.head(self.url)
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
            range_response = await self.client.get(self.url, headers=range_test_headers)
            if range_response.status_code == 206:
                supports_ranges = True

        except Exception as e:
            print(f"[Downloader] Metadata inspection encountered an issue: {e}. Falling back to defaults.")
            if not filename:
                parsed_url = urllib.parse.urlparse(self.url)
                filename = os.path.basename(parsed_url.path) or f"file_{self.session_id}"

        return filename, total_size, supports_ranges

    async def download_chunk_with_range(self, part_filepath: str, start_byte: int, end_byte: int) -> int:
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
        tracker = ProgressTracker(total_size=total_to_fetch, already_downloaded=existing_bytes)

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
                            tracker.update(len(chunk))
                            tracker.display()

                    # Force final print on successful completion to show 100%
                    tracker.display(force=True)
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
        assert False
        os.remove(file_path)
        print(f"[Uploader] Cleaned up: {file_path}")
    except OSError as e:
        print(f"[Uploader] Error deleting {file_path}: {e}")


# --- Pipeline Coordination with Range-Support Intelligence ---
# --- Real Pipeline Coordination ---
async def run_pipeline(bot: TelegramClient, url: str, api_id: int, api_hash: str, bot_token: str, chat_id: str | int):
    downloader = AsyncDownloader(url)

    # Initialize and start our real Telethon uploader
    uploader = TelegramUploader(bot, settings.OWNER_ID[0])

    filename, total_size, supports_ranges = await downloader.inspect_server()

    print(f"\nStarting pipeline for: {filename}")
    print(f"Total Size: {f'{total_size / (1024 * 1024):.2f} MB' if total_size else 'Unknown'}")
    print(f"Server Supports Range Requests: {supports_ranges}")
    print(f"Session ID: {downloader.session_id}\n" + "-" * 50)

    active_upload_task = None
    part_index = 1
    total_bytes_written = 0

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
                    final_path = os.path.join(downloader.temp_dir, filename)
                    os.rename(part_filepath, final_path)

                    caption = (
                        f"📁 **File:** `{filename}`\n"
                        f"📊 **Size:** {total_bytes_written / (1024 * 1024):.2f} MB\n"
                        f"ℹ️ _Single file - ready to open._"
                    )

                    if active_upload_task:
                        await active_upload_task
                    active_upload_task = asyncio.create_task(
                        uploader.upload_file(final_path, caption, part_index)
                    )
                    break
                else:
                    # Case 2: Multi-part file.
                    caption = f"📦 Part {part_index} of `{filename}`\nSession ID: `{downloader.session_id}`"

                if is_last_part:
                    # Append metadata to the end of the final part
                    metadata = (
                        f'{{"filename": "{filename}", '
                        f'"session_id": "{downloader.session_id}", '
                        f'"total_parts": {part_index}}}'
                    )
                    metadata_bytes = metadata.encode("utf-8")
                    metadata_len = len(metadata_bytes)

                    async with aiofiles.open(part_filepath, "ab") as f:
                        await f.write(metadata_bytes)
                        await f.write(metadata_len.to_bytes(4, byteorder="big"))
                    print(f"[Pipeline] Appended metadata to final Part {part_index}")
                    # TODO: What if the last filesize exceeds the limit only after adding the metadata and 4-bit metadata metadata

                # Ensure previous upload is finished before scheduling this one
                if active_upload_task:
                    print(f"[Pipeline] Waiting for Part {part_index - 1} upload to complete...")
                    await active_upload_task

                # Trigger next upload in background
                active_upload_task = asyncio.create_task(
                    uploader.upload_file(part_filepath, caption, part_index)
                )

                part_index += 1

        else:
            # TODO:  --- PATH B: NO RANGE SUPPORT / UNKNOWN SIZE ---
            # Fall back to continuous streaming (subject to backpressure timeouts)
            print("[Pipeline] Server does not support Range requests. Streaming continuously...")
            assert False
            # (Continuous streaming loop code from previous response goes here...)
            # [Omitted here for brevity, but it remains as the fallback route]

        # Wait for the very last part to finish uploading before shutting down
        if active_upload_task:
            await active_upload_task

        print(
            f"\n[Pipeline] Complete! Processed {total_bytes_written / (1024 * 1024):.2f} MB in {part_index if supports_ranges and total_size else part_index} part(s).")

    except Exception as e:
        print(f"\n[Pipeline Error] An error occurred: {e}")
    finally:
        await downloader.close()


class TelegramUploader:
    def __init__(self, bot: TelegramClient, target_chat: str | int):
        self.target_chat = target_chat
        self.bot = bot

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
        tracker = ProgressTracker(total_size=file_size, window_seconds=5.0)

        print(f"[Uploader] Starting upload of Part {part_index} ({file_name})...")

        # Define the callback that Telethon calls periodically during upload
        def progress_callback(current_bytes, total_bytes):
            delta = current_bytes - tracker.downloaded
            tracker.update(delta)
            tracker.display()

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

                tracker.display(force=True)
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


if __name__ == "__main__":
    # Test with a known file host. Most fast hosts support Range Requests.
    test_url = "http://ipv4.download.thinkbroadband.com/5GB.zip"
    asyncio.run(run_pipeline(test_url))