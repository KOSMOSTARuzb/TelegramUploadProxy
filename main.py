from telethon import TelegramClient

from config import settings

bot = TelegramClient(session='anony', api_id=settings.API_ID, api_hash=settings.API_HASH).start(bot_token=settings.BOT_TOKEN)

async def main():
    def callback(current, total):
        print(f'Uploaded {current} out of {total} bytes: {current / total * 100:.2f}%')
    response = await bot.send_file(5582904747, 'file', progress_callback=callback)
    print(response)

with bot:
    bot.loop.run_until_complete(main())