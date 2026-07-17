import asyncio
import os
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from constants import MAX_PARALLEL_TASKS  # noqa: F401 — реекспортується для зворотної сумісності

load_dotenv("token.env")

(SELECT_SITE, TYPING_KEYWORD, TYPING_COUNT, TYPING_YEAR,
 SELECT_FORMAT, SELECT_UK_MODE, ASK_VALIDATE, SELECT_INDIA_STATE) = range(8)

scraping_status: dict = {}
_status_lock = asyncio.Lock()

# ── Фонові asyncio-задачі ──
# create_task() повертає слабке посилання в event loop: якщо ніхто не тримає
# Task-об'єкт, GC може скасувати задачу посеред роботи (напр., status_updater
# перестане оновлювати повідомлення). Тримаємо сильні посилання до завершення.
_background_tasks: set = set()


def create_tracked_task(coro) -> asyncio.Task:
    """asyncio.create_task із збереженням посилання (захист від GC)."""
    task = asyncio.get_running_loop().create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task

_bot_start_time = time.time()   # для health-check uptime

# ── Timezone: зчитуємо з token.env, за замовчуванням Europe/Kyiv ──
# Змініть BOT_TIMEZONE у token.env якщо бот розгорнуто в іншому регіоні.
# Список timezone: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
_BOT_TZ = os.getenv("BOT_TIMEZONE", "Europe/Kyiv")
_scheduler = AsyncIOScheduler(timezone=_BOT_TZ)
