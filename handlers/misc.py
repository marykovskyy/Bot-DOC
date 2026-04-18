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
from handlers.admin import is_admin, require_auth

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
    """Запускає легкий HTTP сервер для моніторингу.

    Валідуємо порт — уникаємо OSError від aiohttp при неправильному ENV.
    Слухаємо на 127.0.0.1 за замовчуванням (HEALTH_BIND_HOST для перевизначення).
    Публічний ендпоінт /health не повинен приймати конекти з інтернету без reverse proxy.
    """
    if not (1 <= int(port) <= 65535):
        logger.error("Health-check: порт %s поза межами [1..65535], пропускаю запуск.", port)
        return
    bind_host = os.getenv("HEALTH_BIND_HOST", "127.0.0.1")
    app_http = web.Application()
    app_http.router.add_get("/health", health_handler)
    runner = web.AppRunner(app_http)
    await runner.setup()
    site = web.TCPSite(runner, bind_host, port)
    try:
        await site.start()
    except OSError as e:
        logger.error("Health-check: не вдалось стартувати на %s:%d — %s", bind_host, port, e)
        return
    logger.info("Health-check сервер запущено: http://%s:%d/health", bind_host, port)


# ─────────────────────────────────────────────
#  КОМАНДА /STATUS
# ─────────────────────────────────────────────

@require_auth
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

    bar_filled = "🟢" * active + "⚪️" * (MAX_PARALLEL_TASKS - active)

    # site — значення з scraping_status (control data, не user input), але все ж
    # використовуємо HTML як безпечніший parse_mode для будь-яких спецсимволів.
    import html as _html
    html_text = (
        f"🖥 <b>Статус бота</b>\n\n"
        f"⏱ Uptime: <code>{hours:02d}:{minutes:02d}:{seconds:02d}</code>\n"
        f"⚡️ Задачі: {bar_filled} <code>{active}/{MAX_PARALLEL_TASKS}</code>\n\n"
    )
    if tasks_snapshot:
        html_text += "<b>Активні пошуки:</b>\n"
        for site, current, total in tasks_snapshot:
            html_text += f"  • <code>{_html.escape(str(site))}</code> — {current}/{total}\n"
    else:
        html_text += "<i>Активних пошуків немає</i>"
    await update.message.reply_text(html_text, parse_mode="HTML")


# ─────────────────────────────────────────────
#  ПЕРЕЗАПУСК БОТА
# ─────────────────────────────────────────────

async def restart_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Перезапускає процес бота. Доступно тільки адміністратору.

    Graceful shutdown:
      1. Повідомляємо користувача
      2. Зупиняємо планувальник (уникнути паралельної задачі перед exec)
      3. Flush логів
      4. os.execl — замінює процес
    """
    if not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ У вас немає прав для цієї команди.")
        return

    if update.message:
        await update.message.reply_text("🔄 Бот перезапускається...")

    async def _restart():
        await asyncio.sleep(1)
        # Graceful: планувальник
        try:
            from state import _scheduler
            if _scheduler.running:
                _scheduler.shutdown(wait=False)
                logger.info("Scheduler зупинено перед рестартом.")
        except Exception as e:
            logger.warning("Помилка при зупинці scheduler: %s", e)
        # Flush логів — щоб RotatingFileHandler зафіксував останні рядки
        try:
            for handler in logging.getLogger().handlers:
                handler.flush()
        except Exception:
            pass
        logger.info("🔄 os.execl → перезапуск процесу")
        os.execl(sys.executable, sys.executable, *sys.argv)

    asyncio.create_task(_restart())
