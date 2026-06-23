import asyncio

from telethon import TelegramClient

from config import settings
from downloader import run_pipeline
import sys

bot = TelegramClient(session='anony', api_id=settings.API_ID, api_hash=settings.API_HASH).start(bot_token=settings.BOT_TOKEN)

async def main():
    # Test with a file download
    test_url = sys.argv[1] if len(sys.argv)>1 else "http://aws.kosmostar.us.kg:8085/testfile.bin"
    await run_pipeline(bot, test_url, settings.OWNER_ID[0])

with bot:
    bot.loop.run_until_complete(main())