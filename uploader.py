import asyncio
import os
from types import NoneType

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from progress_speed import ProgressSpeedManager, ProgressStream


class TelegramUploader:
    def __init__(self, bot: TelegramClient, speed_manager: ProgressSpeedManager, target_chat: str | int):
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
        self.speed_manager.upload = ProgressStream(total_size=file_size, window_seconds=5.0)

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
                self.speed_manager.upload = None

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