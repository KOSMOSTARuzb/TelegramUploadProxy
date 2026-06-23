import asyncio

from telethon import TelegramClient

from config import settings
from download_processors import HttpProcessor
from downloader import run_pipeline
import sys

from progress_speed import ProgressSpeedManager

bot = TelegramClient(session='anony', api_id=settings.API_ID, api_hash=settings.API_HASH).start(bot_token=settings.BOT_TOKEN)

async def main():
    # Test with a file download
    test_url = sys.argv[1] if len(sys.argv)>1 else "http://aws.kosmostar.us.kg:8085/testfile.bin"
    speed_manager = ProgressSpeedManager()
    await run_pipeline(
        bot=bot,
        processor=HttpProcessor(test_url, speed_manager),
        target_chat=settings.OWNER_ID[0],
    )

with bot:
    bot.loop.run_until_complete(main())