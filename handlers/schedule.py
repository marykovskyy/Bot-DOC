"""
handlers_schedule.py — Планувальник задач та дайджест нових компаній.

Містить:
  - CRON_OPTIONS / get_schedule_kb
  - cmd_schedule / handle_schedule_callback
  - _register_scheduled_task / _load_scheduled_tasks
  - send_digest / cmd_digest
"""
import logging
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from apscheduler.triggers.cron import CronTrigger

import database
from scrapers.main import run_scraping
from state import scraping_status, _status_lock, _scheduler, _BOT_TZ

logger = logging.getLogger(__name__)

# ── Варіанти розкладу (label → cron-вираз) ──
CRON_OPTIONS = {
    f"щодня о 08:00 ({_BOT_TZ})":        "0 8 * * *",
    f"щодня о 20:00 ({_BOT_TZ})":        "0 20 * * *",
    f"щопонеділка о 09:00 ({_BOT_TZ})":  "0 9 * * 1",
    "кожні 12 годин":                     "0 */12 * * *",
}


def get_schedule_kb() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(label, callback_data=f"sched_{cron}")]
               for label, cron in CRON_OPTIONS.items()]
    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")])
    return InlineKeyboardMarkup(buttons)


async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /schedule — показати заплановані задачі і кнопку видалення."""
    if not update.message or not update.effective_user:
        return
    chat_id = update.effective_user.id
    tasks = database.get_scheduled_tasks(chat_id)

    text = "⏱ **Заплановані пошуки**\n\n"
    buttons = []
    if tasks:
        for t in tasks:
            flag = {"France": "🇫🇷", "Finland": "🇫🇮", "Denmark": "🇩🇰",
                    "California": "🇺🇸", "UnitedKingdom": "🇬🇧", "Latvia": "🇱🇻",
                    "NewZealand": "🇳🇿", "Thailand": "🇹🇭", "CzechRepublic": "🇨🇿"}.get(t["site"], "📍")
            last = t["last_run"][:16] if t["last_run"] else "ніколи"
            text += f"{flag} **{t['site']}** | `{t['keyword']}` x{t['count']}\n"
            text += f"   🕐 `{t['cron']}` | Останній запуск: {last}\n\n"
            buttons.append([InlineKeyboardButton(
                f"🗑 Видалити: {t['site']}/{t['keyword']}",
                callback_data=f"del_sched_{t['id']}"
            )])
    else:
        text += "_Немає активних задач_\n\n"

    text += "Щоб додати — спочатку налаштуй пошук через 🔍, а потім натисни **Запланувати** замість запуску."
    await update.message.reply_text(
        text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
    )


async def handle_schedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє вибір розкладу та видалення задачі."""
    query = update.callback_query
    if not query or not query.data or not update.effective_user:
        return
    await query.answer()
    chat_id = update.effective_user.id

    if query.data.startswith("del_sched_"):
        task_id = int(query.data.replace("del_sched_", ""))
        database.delete_scheduled_task(task_id, chat_id)
        # Видаляємо з APScheduler
        job_id = f"task_{task_id}"
        if _scheduler.get_job(job_id):
            _scheduler.remove_job(job_id)
        await query.edit_message_text("✅ Задачу видалено.")

    elif query.data.startswith("sched_"):
        cron = query.data.replace("sched_", "")
        ud = context.user_data or {}
        task_id = database.save_scheduled_task(
            chat_id=chat_id,
            site=ud.get("site", ""),
            keyword=ud.get("kw", ""),
            count=int(ud.get("count", 10)),
            year=ud.get("target_year", "0"),
            file_format=ud.get("file_format", "EXCEL"),
            cron=cron
        )
        _register_scheduled_task(task_id, chat_id, ud, cron)
        label = next((l for l, c in CRON_OPTIONS.items() if c == cron), cron)
        await query.edit_message_text(
            f"✅ **Задачу заплановано!**\n\n"
            f"🕐 Розклад: _{label}_\n"
            f"🌍 Країна: **{ud.get('site')}** | Ключ: `{ud.get('kw')}`\n\n"
            f"Переглянути всі задачі: /schedule",
            parse_mode="Markdown"
        )


def _register_scheduled_task(task_id: int, chat_id: int, params: dict, cron: str) -> None:
    """Реєструє задачу в APScheduler.

    Timezone: CronTrigger отримує _BOT_TZ явно.
    Без цього APScheduler v3 використовував би UTC незалежно від налаштування scheduler-а.
    """
    parts = cron.split()
    if len(parts) != 5:
        logger.error("Невалідний cron вираз для задачі %d: '%s'", task_id, cron)
        return
    minute, hour, day, month, dow = parts

    async def _run() -> None:
        import asyncio
        logger.info("Scheduler: запуск задачі %d для chat_id %d (tz=%s)", task_id, chat_id, _BOT_TZ)
        status: dict = {
            "current": 0, "max": params.get("count", 10),
            "last_name": "Scheduler...", "is_running": True,
            "file_path": None, "target_year": params.get("target_year", "0"),
            "uk_download_pdf": False, "site": params.get("site", ""),
        }
        async with _status_lock:
            scraping_status[chat_id] = status
        await asyncio.to_thread(
            run_scraping,
            chat_id, params.get("kw", ""), int(params.get("count", 10)),
            params.get("site"), params.get("file_format", "EXCEL"), status
        )
        database.update_task_last_run(task_id)
        async with _status_lock:
            scraping_status.pop(chat_id, None)

    _scheduler.add_job(
        _run,
        CronTrigger(
            minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
            timezone=_BOT_TZ          # ← явний timezone, не успадковує від scheduler
        ),
        id=f"task_{task_id}",
        replace_existing=True
    )


def _load_scheduled_tasks() -> None:
    """Завантажує всі активні задачі з БД при старті бота."""
    try:
        tasks = database.get_scheduled_tasks()
        for t in tasks:
            _register_scheduled_task(
                t["id"], t["chat_id"],
                {"kw": t["keyword"], "count": t["count"], "site": t["site"],
                 "target_year": t["year"], "file_format": t["file_format"]},
                t["cron"]
            )
        if tasks:
            logger.info("Scheduler: завантажено %d задач з БД", len(tasks))
    except Exception as e:
        logger.error("Помилка завантаження задач: %s", e)


# ─────────────────────────────────────────────
#  ДАЙДЖЕСТ НОВИХ КОМПАНІЙ
# ─────────────────────────────────────────────

async def send_digest(bot, chat_id: int, hours: int = 24) -> None:
    """Надсилає дайджест нових компаній за останні N годин."""
    since = datetime.now() - timedelta(hours=hours)
    companies = database.get_new_companies_since(since)

    if not companies:
        await bot.send_message(
            chat_id=chat_id,
            text=f"📊 **Дайджест за {hours}г**\n\n_За цей час нових компаній не знайдено._",
            parse_mode="Markdown"
        )
        return

    # Групуємо по країнах
    by_country: dict = {}
    for c in companies:
        by_country.setdefault(c["country"], []).append(c)

    flags = {"France": "🇫🇷", "Finland": "🇫🇮", "Denmark": "🇩🇰",
             "California": "🇺🇸", "UnitedKingdom": "🇬🇧", "Latvia": "🇱🇻",
             "NewZealand": "🇳🇿", "Thailand": "🇹🇭", "CzechRepublic": "🇨🇿"}

    text = f"📊 **Дайджест за {hours}г** — {len(companies)} нових компаній\n\n"
    for country, items in by_country.items():
        flag = flags.get(country, "📍")
        text += f"{flag} **{country}**: {len(items)} шт.\n"
        for item in items[:5]:  # показуємо максимум 5 на країну
            name = item["name"][:40] + "..." if len(item["name"]) > 40 else item["name"]
            text += f"  • [{name}]({item['link']})\n"
        if len(items) > 5:
            text += f"  _...і ще {len(items) - 5}_\n"
        text += "\n"

    await bot.send_message(chat_id=chat_id, text=text,
                           parse_mode="Markdown", disable_web_page_preview=True)


async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /digest [hours] — дайджест нових компаній."""
    if not update.message or not update.effective_user:
        return
    args = context.args or []
    hours = int(args[0]) if args and args[0].isdigit() else 24
    await update.message.reply_text(f"⏳ Формую дайджест за {hours} годин...")
    await send_digest(context.bot, update.effective_user.id, hours)
