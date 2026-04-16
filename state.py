import asyncio
import time
import os
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from constants import MAX_PARALLEL_TASKS  # noqa: F401 — реекспортується для зворотної сумісності

load_dotenv("token.env")

SELECT_SITE, TYPING_KEYWORD, TYPING_COUNT, TYPING_YEAR, SELECT_FORMAT, SELECT_UK_MODE = range(6)

scraping_status: dict = {}
_status_lock = asyncio.Lock()

_bot_start_time = time.time()   # для health-check uptime

# ── Timezone: зчитуємо з token.env, за замовчуванням Europe/Kyiv ──
# Змініть BOT_TIMEZONE у token.env якщо бот розгорнуто в іншому регіоні.
# Список timezone: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
_BOT_TZ = os.getenv("BOT_TIMEZONE", "Europe/Kyiv")
_scheduler = AsyncIOScheduler(timezone=_BOT_TZ)
