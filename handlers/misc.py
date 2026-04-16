"""
handlers_misc.py — Допоміжні хендлери: health-check, статус бота, перезапуск.
"""
import asyncio
import logging
import os
import sys
import time

from aiohttp import web
from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_ID
from state import scraping_status, _status_lock, MAX_PARALLEL_TASKS, _bot_start_time
from handlers.admin import is_admin

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  HEALTH-CHECK HTTP СЕРВЕР
# ─────────────────────────────────────────────

async def health_handler(request: web.Request) -> web.Response:
    """GET /health — повертає статус бота у JSON."""
    async with _status_lock:
        active = sum(1 for s in scraping_status.values() if s.get('is_running'))
    uptime_sec = int(time.time() - _bot_start_time)
    hours, rem = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(rem, 60)
    return web.json_response({
        "status": "ok",
        "uptime": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
        "active_tasks": active,
        "max_tasks": MAX_PARALLEL_TASKS,
        "queue_free": MAX_PARALLEL_TASKS - active,
    })


async def start_health_server(port: int = 8080) -> None:
    """Запускає легкий HTTP сервер для моніторингу."""
    app_http = web.Application()
    app_http.router.add_get("/health", health_handler)
    runner = web.AppRunner(app_http)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health-check сервер запущено: http://0.0.0.0:%d/health", port)


# ─────────────────────────────────────────────
#  КОМАНДА /STATUS
# ─────────────────────────────────────────────

async def show_bot_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /status — показує поточне навантаження бота."""
    if not update.message:
        return

    # ── Snapshot під локом, щоб уникнути читання частково-оновленого стану ──
    async with _status_lock:
        active = sum(1 for s in scraping_status.values() if s.get('is_running'))
        tasks_snapshot = [
            (st.get('site', '?'), st.get('current', 0), st.get('max', 1))
            for st in scraping_status.values()
            if st.get('is_running')
        ]

    uptime_sec = int(time.time() - _bot_start_time)
    hours, rem = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(rem, 60)

    tasks_text = ""
    for site, current, total in tasks_snapshot:
        tasks_text += f"  • `{site}` — {current}/{total}\n"

    bar_filled = "🟢" * active + "⚪️" * (MAX_PARALLEL_TASKS - active)

    text = (
        f"🖥 **Статус бота**\n\n"
        f"⏱ Uptime: `{hours:02d}:{minutes:02d}:{seconds:02d}`\n"
        f"⚡️ Задачі: {bar_filled} `{active}/{MAX_PARALLEL_TASKS}`\n\n"
    )
    if tasks_text:
        text += f"**Активні пошуки:**\n{tasks_text}"
    else:
        text += "_Активних пошуків немає_"

    await update.message.reply_text(text, parse_mode="Markdown")


# ─────────────────────────────────────────────
#  ПЕРЕЗАПУСК БОТА
# ─────────────────────────────────────────────

async def restart_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Перезапускає процес бота. Доступно тільки адміністратору."""
    if not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ У вас немає прав для цієї команди.")
        return

    if update.message:
        await update.message.reply_text("🔄 Бот перезапускається...")

    async def _restart():
        await asyncio.sleep(1)
        os.execl(sys.executable, sys.executable, *sys.argv)

    asyncio.create_task(_restart())
