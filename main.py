import asyncio

from telethon import TelegramClient

from config import settings
from download_processors import HttpProcessor, TorrentProcessor
from downloader import run_pipeline
import sys

from progress_speed import ProgressSpeedManager

bot = TelegramClient(session='anony', api_id=settings.API_ID, api_hash=settings.API_HASH).start(bot_token=settings.BOT_TOKEN)
# TODO: Add \n before all print statements to prevent it to print in the same line as the progress manager.
async def main():
    # Test with a file download
    test_url = sys.argv[1] if len(sys.argv)>1 else "http://aws.kosmostar.us.kg:8085/testfile.bin"
    torrent_url = sys.argv[1] if len(sys.argv)>1 else "magnet:?xt=urn:btih:1C1B34A783189F2686005B0B5EBB027DC45FAF13&dn=Spider-Noir.S01.Complete.1080p.WEBRip.10Bit.DDP5.1.x265-NeoNoir&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&tr=http%3A%2F%2Fopen.tracker.cl%3A1337%2Fannounce&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Fexplodie.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.ololosh.space%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dump.cl%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker-udp.gbitt.info%3A80%2Fannounce&tr=udp%3A%2F%2Fretracker01-msk-virt.corbina.net%3A80%2Fannounce&tr=udp%3A%2F%2Fopen.free-tracker.ga%3A6969%2Fannounce&tr=udp%3A%2F%2Fns-1.x-fins.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=http%3A%2F%2Ftracker.openbittorrent.com%3A80%2Fannounce&tr=udp%3A%2F%2Fopentracker.i2p.rocks%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fcoppersurfer.tk%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.zer0day.to%3A1337%2Fannounce"
    speed_manager = ProgressSpeedManager()
    await run_pipeline(
        bot=bot,
        processor=TorrentProcessor(torrent_url, speed_manager),
        target_chat=settings.OWNER_ID[0],
    )

with bot:
    bot.loop.run_until_complete(main())