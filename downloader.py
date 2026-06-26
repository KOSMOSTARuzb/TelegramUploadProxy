import asyncio
import json
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
        def parse_metadata(session_id: str, parts_count: int) -> bytes:
            processor_metadata = processor.get_processor_metadata()
            metadata_str = json.dumps({
                "processor_type": processor.processor_type,
                "session_id": session_id,
                "total_parts": parts_count,
                **processor_metadata
            })
            return metadata_str.encode("utf-8")

        metadata_bytes = parse_metadata(processor.session_id, idx)
        current_size = os.path.getsize(filepath)
        assert len(metadata_bytes) + 4 <= settings.CHUNK_SIZE_LIMIT, f"Extremely large metadata size: {len(metadata_bytes) + 4}"

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

            new_meta_bytes = parse_metadata(processor.session_id, new_idx)

            async with aiofiles.open(new_filepath, "wb") as f:
                await f.write(new_meta_bytes)
                await f.write(len(new_meta_bytes).to_bytes(4, byteorder="big"))

            return new_filepath, new_idx

    try:
        # Loop effortlessly over whatever chunks the processor provides
        # noinspection PyTypeChecker
        async for part_filepath, part_index, is_last in processor.yield_chunks(settings.CHUNK_SIZE_LIMIT):

            if is_last and part_index == 1 and processor.processor_type == "HttpProcessor":
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


class PipelineQueueManager:
    def __init__(self, bot):
        self.bot = bot
        self.queue = asyncio.Queue()
        self.active_tasks = {}  # Maps msg_id -> asyncio.Task
        self.queued_items = {}  # Maps msg_id -> item_dict (for fast lookups)
        self.current_msg_id = None  # Tracks the currently running task's message ID
        self.worker_task = None

    def start(self):
        """Starts the sequential queue consumer task."""
        self.worker_task = asyncio.create_task(self._worker())

    async def add_to_queue(self, msg_id: int, chat_id: int, processor, event):
        item = {
            "msg_id": msg_id,
            "chat_id": chat_id,
            "processor": processor,
            "event": event
        }
        self.queued_items[msg_id] = item
        await self.queue.put(item)

        # Notify the user of their queue position
        pos = self.queue.qsize()
        if pos > 0:
            await self.bot.send_message(
                chat_id,
                f"⏳ Task added to queue. Queue Position: {pos}",
                reply_to=msg_id
            )

    async def cancel_task(self, msg_id: int, chat_id: int) -> bool:
        """Attempts to cancel a specific running task or remove a pending queue item."""
        # 1. If it's currently running, cancel the asyncio Task
        if msg_id in self.active_tasks:
            task = self.active_tasks[msg_id]
            task.cancel()  # Raises asyncio.CancelledError inside the task's execution
            await self.bot.send_message(chat_id, "🛑 Active pipeline cancelled successfully.")
            return True

        # 2. If it's waiting in the queue, mark it as removed
        if msg_id in self.queued_items:
            del self.queued_items[msg_id]
            await self.bot.send_message(chat_id, "🗑️ Pending task removed from the queue.")
            return True

        return False

    async def cancel_active_task(self, chat_id: int) -> bool:
        """Cancels whatever is currently running on the pipeline."""
        if self.current_msg_id and self.current_msg_id in self.active_tasks:
            return await self.cancel_task(self.current_msg_id, chat_id)

        await self.bot.send_message(chat_id, "⚠️ No active task is currently running.")
        return False

    async def _worker(self):
        """Sequential queue consumer loop."""
        while True:
            item = await self.queue.get()
            msg_id = item["msg_id"]

            # If the item was cancelled while sitting in the queue, skip it
            if msg_id not in self.queued_items:
                self.queue.task_done()
                continue

            # Remove from queue tracking list
            del self.queued_items[msg_id]

            self.current_msg_id = msg_id
            chat_id = item["chat_id"]
            processor = item["processor"]
            event = item["event"]

            # Create the pipeline execution coroutine
            pipeline_coro = run_pipeline(
                bot=self.bot,
                processor=processor,
                target_chat=chat_id
            )

            # Spawn and track the task
            task = asyncio.create_task(pipeline_coro)
            self.active_tasks[msg_id] = task

            try:
                await task
            except asyncio.CancelledError:
                print(f"[QueueManager] Task {msg_id} was successfully cancelled.")
            except Exception as e:
                await self.bot.send_message(chat_id, f"❌ Pipeline Exception: {e}")
            finally:
                # Cleanup tracking states
                self.active_tasks.pop(msg_id, None)
                self.current_msg_id = None
                self.queue.task_done()