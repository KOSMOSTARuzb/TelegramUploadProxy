import asyncio

from telethon import TelegramClient, events, functions, types

from config import settings
from download_processors import HttpProcessor, TorrentProcessor, LocalFileProcessor
from downloader import run_pipeline, PipelineQueueManager
import sys

from progress_speed import ProgressSpeedManager

bot = TelegramClient(session='anony', api_id=settings.API_ID, api_hash=settings.API_HASH).start(bot_token=settings.BOT_TOKEN)
# TODO: Add \n before all print statements to prevent it to print in the same line as the progress manager.
speed_manager = ProgressSpeedManager()
queue_manager = PipelineQueueManager(bot)
queue_manager.start()


@bot.on(events.NewMessage)
async def main(event: events.NewMessage.Event):
    text: str = event.raw_text
    chat_id = event.chat_id

    # Check permissions
    if chat_id not in settings.OWNER_ID:
        await bot(functions.messages.SendReactionRequest(
            peer=await event.get_input_chat(),
            msg_id=event.message.id,
            reaction=[types.ReactionEmoji(emoticon='🖕')]
        ))
        return

    # --- CANCELLATION COMMAND HANDLER ---
    if text.strip().lower() == "/cancel":
        reply = await event.get_reply_message()
        if reply:
            # If the owner replied to a specific message, cancel that specific task
            cancelled = await queue_manager.cancel_task(reply.id, chat_id)
            if not cancelled:
                await bot.send_message(chat_id, "Could not find an active or queued task for that message.")
        else:
            # If no reply, cancel whatever is currently running
            await queue_manager.cancel_active_task(chat_id)
        return

    # Process and assign the correct processor
    if text.startswith("http"):
        processor = HttpProcessor(text, speed_manager)
    elif text.startswith("magnet:"):
        processor = TorrentProcessor(text, speed_manager)
    elif text.startswith("localfile:"):
        processor = LocalFileProcessor(text.split(':', 1)[1], speed_manager)
    else:
        await bot(functions.messages.SendReactionRequest(
            peer=await event.get_input_chat(),
            msg_id=event.message.id,
            reaction=[types.ReactionEmoji(emoticon='🤷‍♂️')]
        ))
        return

    # Acknowledge the start
    await bot(functions.messages.SendReactionRequest(
        peer=await event.get_input_chat(),
        msg_id=event.message.id,
        reaction=[types.ReactionEmoji(emoticon='🕊')]
    ))

    # --- PUSH TASK TO SEQUENTIAL QUEUE ---
    await queue_manager.add_to_queue(event.message.id, chat_id, processor, event)

with bot:
    bot.run_until_disconnected()