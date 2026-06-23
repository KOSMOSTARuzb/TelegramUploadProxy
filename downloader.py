import asyncio
import os
from typing import Tuple

import aiofiles
from telethon import TelegramClient

from config import settings
from download_processors import BaseSourceProcessor
from uploader import TelegramUploader


async def run_pipeline(bot: TelegramClient, processor: BaseSourceProcessor, target_chat: int):
    uploader = TelegramUploader(bot, speed_manager=processor.speed_manager, target_chat=target_chat)
    filename, total_size = await processor.prepare()

    print(f"\nStarting pipeline for: {filename}")
    print(f"Total Size: {f'{total_size / (1024 * 1024):.2f} MB' if total_size else 'Unknown'}")
    print(f"Session ID: {processor.session_id}\n" + "-" * 50)

    active_upload_task: asyncio.Task | None = None

    async def queue_upload(filepath: str, caption: str, idx: int):
        nonlocal active_upload_task
        if active_upload_task:
            print(f"[Pipeline] Waiting for previous upload to complete...")
            await active_upload_task
        active_upload_task = asyncio.create_task(uploader.upload_file(filepath, caption, idx))

    async def append_metadata(filepath: str, idx: int) -> Tuple[str, int]:
        """Appends metadata to the final file, safely rolling over to a new part if size limits are breached."""
        metadata_str = f'{{"filename": "{filename}", "session_id": "{processor.session_id}", "total_parts": {idx}}}'
        metadata_bytes = metadata_str.encode("utf-8")
        current_size = os.path.getsize(filepath)

        if current_size + len(metadata_bytes) + 4 <= settings.CHUNK_SIZE_LIMIT:
            async with aiofiles.open(filepath, "ab") as f:
                await f.write(metadata_bytes)
                await f.write(len(metadata_bytes).to_bytes(4, byteorder="big"))
            return filepath, idx
        else:
            # Prevent Telegram max size error; create a metadata-only final part
            await queue_upload(filepath, f"📦 Part {idx} of `{filename}`\nSession ID: `{processor.session_id}`", idx)

            new_idx = idx + 1
            new_filepath = os.path.join(processor.temp_dir, f"{filename}.{processor.session_id}.kpart{new_idx}")

            metadata_str = f'{{"filename": "{filename}", "session_id": "{processor.session_id}", "total_parts": {new_idx}}}'
            new_meta_bytes = metadata_str.encode("utf-8")

            async with aiofiles.open(new_filepath, "wb") as f:
                await f.write(new_meta_bytes)
                await f.write(len(new_meta_bytes).to_bytes(4, byteorder="big"))

            return new_filepath, new_idx

    try:
        # Loop effortlessly over whatever chunks the processor provides
        async for part_filepath, part_index, is_last in processor.yield_chunks(settings.CHUNK_SIZE_LIMIT):

            if is_last and part_index == 1:
                # Single-part file: Rename to original and upload directly
                final_path = os.path.join(processor.temp_dir, filename)
                os.rename(part_filepath, final_path)
                file_mb = os.path.getsize(final_path) / (1024 * 1024)
                caption = f"📁 **File:** `{filename}`\n📊 **Size:** {file_mb:.2f} MB\nℹ️ _Single file - ready to open._"
                await queue_upload(final_path, caption, part_index)
                break

            if is_last:
                # Multi-part file final chunk: Safe metadata append
                upload_filepath, upload_idx = await append_metadata(part_filepath, part_index)
                caption = f"📦 Part {upload_idx} of `{filename}`\nSession ID: `{processor.session_id}`"
                await queue_upload(upload_filepath, caption, upload_idx)
            else:
                # Standard chunk
                caption = f"📦 Part {part_index} of `{filename}`\nSession ID: `{processor.session_id}`"
                await queue_upload(part_filepath, caption, part_index)

        if active_upload_task:
            await active_upload_task

        print("\n[Pipeline] Complete!")

    except Exception as e:
        print(f"\n[Pipeline Error] An error occurred: {e}")
    finally:
        await processor.close()