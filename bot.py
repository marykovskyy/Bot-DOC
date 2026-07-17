"""
bot.py — Точка входу бота. Реєстрація хендлерів та запуск.

Архітектура модулів (пакет handlers/):
  state.py              — спільний стан (scraping_status, _status_lock, _scheduler…)
  keyboards.py          — фабрики клавіатур
  handlers/scraping.py  — ConversationHandler (пошук компаній)
  handlers/proxy.py     — управління проксі
  handlers/admin.py     — авторизація, /users, /adduser, /removeuser, /history, довідка
  handlers/schedule.py  — планувальник, /schedule, /digest
  handlers/misc.py      — /status, health-check HTTP, /restart
  handlers/documents.py — AI-генерація документів
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

import database
from analysis.ai_sorter import (
    cancel_analysis_callback,
    cmd_analysis_logs,
    cmd_cleanup,
    cmd_myresults,
    confirm_analysis_callback,
    handle_delivery_callback,
    handle_gdrive_link,
    handle_zip_documents,
    run_auto_cleanup,
    sides_toggle_callback,
)
from config import ADMIN_ID, TOKEN
from documents.generator import load_all_templates
from handlers.admin import (
    cmd_adduser,
    cmd_history,
    cmd_removeuser,
    cmd_unblockuser,
    cmd_users,
    help_section_callback,
    repeat_from_history,
    repeat_uk_mode_callback,
    settings_callback,
    settings_menu,
    show_help,
    show_stats,
)
from handlers.documents import (
    build_doc_conversation,
    cmd_preview_template,
)
from handlers.misc import (
    restart_bot,
    show_bot_status,
    start_health_server,
)
from handlers.proxy import (
    auto_update_proxy,
    handle_proxy_file,
    prompt_for_zip,
    proxy_callback_handler,
    proxy_menu,
)
from handlers.schedule import (
    _load_scheduled_tasks,
    cmd_digest,
    cmd_schedule,
    handle_schedule_callback,
)

# ── Handlers ──
from handlers.scraping import (
    count_choice,
    group_choice,
    handle_navigation,
    india_state_choice,
    repeat_search_callback,
    run_task,
    save_count,
    save_kw,
    save_year,
    select_uk_mode,
    site_choice,
    start,
    stop_scraping,
    validate_choice,
    year_choice,
)
from observability import init_sentry
from state import (
    ASK_VALIDATE,
    SELECT_FORMAT,
    SELECT_INDIA_STATE,
    SELECT_SITE,
    SELECT_UK_MODE,
    TYPING_COUNT,
    TYPING_KEYWORD,
    TYPING_YEAR,
    _scheduler,
)

# ── Логування: stdout + RotatingFileHandler ───────────────────────────────
_LOG_DIR = Path(__file__).parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_log_formatter = logging.Formatter(_LOG_FORMAT)

_root = logging.getLogger()
_root.setLevel(logging.INFO)
# stdout handler
_stream_h = logging.StreamHandler()
_stream_h.setFormatter(_log_formatter)
_root.addHandler(_stream_h)
# rotating file handler: 10 MB × 5 файлів
_file_h = RotatingFileHandler(
    _LOG_DIR / "bot.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file_h.setFormatter(_log_formatter)
_root.addHandler(_file_h)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ── Глобальний обробник помилок PTB ──────────────────────────────────────
# Без нього необроблені винятки хендлерів лише пишуться в лог за замовчуванням:
# адмін не дізнається, Sentry-подія залежить від чужого формату логування.
# Тут: повний traceback у лог (→ Sentry через LoggingIntegration) + коротке
# сповіщення адміну (з тротлінгом, щоб серія помилок не заспамила чат).
_err_last_notified: dict = {}          # тип помилки → monotonic час останнього DM
_ERR_NOTIFY_COOLDOWN = 300             # сек між сповіщеннями одного типу


async def global_error_handler(update, context) -> None:
    import time as _time

    import telegram.error

    error = context.error
    # Conflict = запущено другий екземпляр бота — лог без Sentry-шуму
    if isinstance(error, telegram.error.Conflict):
        logger.warning("Telegram Conflict: схоже, запущено другий екземпляр бота.")
        return
    # Мережеві таймаути трапляються постійно — тільки warning, без DM
    if isinstance(error, telegram.error.NetworkError):
        logger.warning("Telegram NetworkError: %s", error)
        return

    logger.error("Необроблена помилка в хендлері (update=%s)",
                 getattr(update, "update_id", update), exc_info=error)

    if not ADMIN_ID:
        return
    err_key = type(error).__name__
    now = _time.monotonic()
    if now - _err_last_notified.get(err_key, 0) < _ERR_NOTIFY_COOLDOWN:
        return
    _err_last_notified[err_key] = now
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"⚠️ Помилка в боті: `{err_key}`\n`{str(error)[:300]}`\n"
                 f"Деталі — в logs/bot.log",
            parse_mode="Markdown",
        )
    except Exception:
        logger.debug("Не вдалося надіслати сповіщення адміну про помилку.")


def main() -> None:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не знайдено в .env!")

    # Sentry (опціонально — no-op якщо SENTRY_DSN не заданий)
    init_sentry()

    app = (ApplicationBuilder()
           .token(TOKEN)             # type: ignore[arg-type]
           .read_timeout(3600)       # 1 година — для аналізу великих архівів
           .write_timeout(3600)      # 1 година — для відправки великих ZIP результатів
           .connect_timeout(60)
           .pool_timeout(3600)
           .build())

    # ── Кнопки головного меню ──
    app.add_handler(MessageHandler(filters.Regex("^🌐 Налаштування проксі$"), proxy_menu))
    app.add_handler(MessageHandler(filters.Regex("^⚙️ Налаштування$"), settings_menu))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^settings_"))
    app.add_handler(MessageHandler(filters.Regex("^📊 Статистика$"), show_stats))
    app.add_handler(MessageHandler(filters.Regex("^❓ Допомога$"), show_help))
    app.add_handler(MessageHandler(filters.Regex("^📊 Статус бота$"), show_bot_status))
    app.add_handler(MessageHandler(filters.Regex("^📋 Історія$"), cmd_history))
    app.add_handler(MessageHandler(filters.Regex("^🔄 Перезапустити бота$"), restart_bot))
    # Примітка: "🪪 Документи" обробляється всередині build_doc_conversation() (entry_point)

    # ── Проксі ──
    # Звужений regex: host:port:user:pass (host — IPv4 або domain, port — тільки цифри).
    # Раніше `.+:.+:.+:.+` перехоплював будь-яке повідомлення з 3+ двокрапками
    # і ламав ConversationHandler-стани типу TYPING_KEYWORD.
    # group=1 — щоб ConversationHandler (group=0) мав пріоритет при активній розмові.
    app.add_handler(
        MessageHandler(
            filters.Regex(r"^[\w.\-]+:\d{1,5}:[^\s:]+:[^\s:]+$"),
            auto_update_proxy
        ),
        group=1
    )
    app.add_handler(MessageHandler(filters.Document.FileExtension("txt"), handle_proxy_file), group=1)
    app.add_handler(CallbackQueryHandler(
        proxy_callback_handler,
        pattern=r"^(toggle_proxy|close_proxy|proxy_upload_info|proxy_clear|proxy_back"
                r"|addgeo_.*|cancel_proxy_add|proxy_check|checkgeo_.*|proxy_remove_broken)$"
    ))

    # ── AI сортер ──
    app.add_handler(MessageHandler(filters.Regex(r"Перевірка фіз\. доків"), prompt_for_zip))
    app.add_handler(MessageHandler(
        filters.Document.FileExtension("zip")
        | filters.Document.FileExtension("rar")
        | filters.Document.FileExtension("7z"),
        handle_zip_documents))
    app.add_handler(MessageHandler(filters.Regex(r"https://drive\.google\.com"), handle_gdrive_link))
    app.add_handler(CallbackQueryHandler(
        handle_delivery_callback,
        pattern=r"^deliver_(tg|s3|s3d|ch|done)_\d{12}(_\d+)?$"
    ))
    app.add_handler(CallbackQueryHandler(
        cancel_analysis_callback,
        pattern=r"^cancel_analysis_\d+$"
    ))
    # Preview-confirm для ZIP/GDrive аналізу — юзер бачить вартість і структуру
    # перед запуском OCR+GPT-4o (реальні $). Без цього гроші горіли без згоди.
    app.add_handler(CallbackQueryHandler(
        confirm_analysis_callback,
        pattern=r"^(confirm_analysis_[0-9a-f]{10}_tx[01]|cancelconfirm_[0-9a-f]{10})$"
    ))
    # Тумблер «підписувати front/back» на екрані підтвердження аналізу
    app.add_handler(CallbackQueryHandler(
        sides_toggle_callback,
        pattern=r"^sidestoggle_[0-9a-f]{10}$"
    ))

    # ── Скрапінг / зупинка ──
    app.add_handler(CallbackQueryHandler(stop_scraping, pattern="^stop_scraping$"))
    app.add_handler(CallbackQueryHandler(repeat_from_history, pattern="^repeat_\\d+$"))
    app.add_handler(CallbackQueryHandler(
        repeat_uk_mode_callback, pattern=r"^rpt_uk_(pdf|lnk)_[0-9a-f]{10}$"
    ))

    # ── Допомога (навігація між секціями) ──
    app.add_handler(CallbackQueryHandler(help_section_callback, pattern="^(help_|noop)"))

    # ── Планувальник ──
    app.add_handler(CallbackQueryHandler(handle_schedule_callback, pattern="^(sched_|del_sched_)"))

    # ── Команди ──
    app.add_handler(CommandHandler("restart",      restart_bot))
    app.add_handler(CommandHandler("status",       show_bot_status))
    app.add_handler(CommandHandler("history",      cmd_history))
    app.add_handler(CommandHandler("schedule",     cmd_schedule))
    app.add_handler(CommandHandler("digest",       cmd_digest))
    app.add_handler(CommandHandler("users",        cmd_users))
    app.add_handler(CommandHandler("adduser",      cmd_adduser))
    app.add_handler(CommandHandler("removeuser",   cmd_removeuser))
    app.add_handler(CommandHandler("unblockuser",  cmd_unblockuser))
    app.add_handler(CommandHandler("myresults",    cmd_myresults))
    app.add_handler(CommandHandler("analysislogs", cmd_analysis_logs))
    app.add_handler(CommandHandler("cleanup",      cmd_cleanup))
    app.add_handler(CommandHandler("previewdoc",  cmd_preview_template))

    # ── ConversationHandler: генерація документів ──
    app.add_handler(build_doc_conversation())

    # ── ConversationHandler: пошук компаній ──
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex("^🔍 Пошук юр. доків$"), start),
            CallbackQueryHandler(repeat_search_callback, pattern="^repeat_search$")
        ],
        states={
            ASK_VALIDATE: [
                # validate_yes / validate_no / validate_skip — відповіді на choice
                CallbackQueryHandler(validate_choice,
                                     pattern="^validate_(yes|no|skip)$"),
                # back_validate — повернення з warning-екрану до choice
                CallbackQueryHandler(handle_navigation,
                                     pattern="^(back_validate|cancel_search)$"),
            ],
            SELECT_SITE: [
                CallbackQueryHandler(site_choice, pattern="^site_"),
                CallbackQueryHandler(group_choice, pattern="^group_"),
                CallbackQueryHandler(handle_navigation, pattern="^(back_sites|cancel_search)$")
            ],
            SELECT_INDIA_STATE: [
                CallbackQueryHandler(india_state_choice, pattern="^istate_"),
                CallbackQueryHandler(handle_navigation, pattern="^(back_|cancel_search)")
            ],
            TYPING_KEYWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_kw),
                CallbackQueryHandler(handle_navigation, pattern="^(back_|cancel_search)")
            ],
            TYPING_COUNT: [
                # Швидкі кнопки [10][50][100][...][custom] — оброблюються перед back/cancel
                CallbackQueryHandler(count_choice, pattern="^count_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_count),
                CallbackQueryHandler(handle_navigation, pattern="^(back_|cancel_search)")
            ],
            SELECT_UK_MODE: [
                CallbackQueryHandler(select_uk_mode, pattern="^(ukmode_|back_|cancel_search)")
            ],
            SELECT_FORMAT: [
                CallbackQueryHandler(run_task, pattern="^(fmt_|back_|cancel_search)")
            ],
            TYPING_YEAR: [
                # Швидкі кнопки року + custom prompt
                CallbackQueryHandler(year_choice, pattern="^year_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_year),
                CallbackQueryHandler(handle_navigation, pattern="^(back_|cancel_search)")
            ]
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True
    )
    app.add_handler(conv_handler)

    # ── Ініціалізація при старті ──
    async def post_init(application) -> None:  # type: ignore[type-arg]
        # Глобальний exception handler для fire-and-forget asyncio.create_task(...) —
        # без нього помилки в не-awaited тасках лише варнингуються при GC і губляться.
        # З ним — логи + Sentry бачать кожен unhandled exception.
        import asyncio

        def _loop_exception_handler(loop, context):
            exc = context.get("exception")
            msg = context.get("message", "unknown asyncio error")
            if exc:
                logger.error("asyncio task exception: %s", msg, exc_info=exc)
                # Sentry підхопить автоматично через logging integration
            else:
                logger.error("asyncio loop error: %s | context=%s", msg, context)

        try:
            asyncio.get_running_loop().set_exception_handler(_loop_exception_handler)
        except Exception:
            logger.exception("Не вдалось встановити asyncio exception handler.")

        # Кожен крок обгорнуто окремо: критичні кроки (init_db) переривають старт,
        # допоміжні (templates, warmup) — лише логуються, бот все одно піднімається.
        try:
            await start_health_server(port=8080)
        except Exception:
            logger.exception("post_init: health-server не стартував (non-fatal).")

        try:
            database.init_db()
        except Exception:
            logger.exception("post_init: init_db ЗАФЕЙЛИВ — критично, зупиняю старт.")
            raise

        try:
            _load_scheduled_tasks()
            _scheduler.start()
            logger.info("Scheduler запущено.")
        except Exception:
            logger.exception("post_init: scheduler не стартував (non-fatal, бот працюватиме без cron).")

        try:
            await run_auto_cleanup()
            logger.info("Auto-cleanup завершено.")
        except Exception:
            logger.exception("post_init: auto-cleanup впав (non-fatal).")

        try:
            load_all_templates()
        except Exception:
            logger.exception("post_init: load_all_templates впав (non-fatal, документи можуть не працювати).")

        # Прогрів PaddleOCR — завантажує моделі заздалегідь (~10с)
        try:
            from analysis.doc_analyzer import warmup_paddle_ocr
            await asyncio.to_thread(warmup_paddle_ocr)
        except Exception:
            logger.exception("post_init: PaddleOCR warmup впав (non-fatal, моделі завантажаться при першому використанні).")

    app.add_error_handler(global_error_handler)

    app.post_init = post_init  # type: ignore[method-assign]
    logger.info("🤖 Бот запущений!")
    app.run_polling()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот зупинений користувачем.")
    except Exception as e:
        logger.critical("Помилка при запуску: %s", e, exc_info=True)
        sys.exit(1)  # systemd/docker розуміє код != 0 як помилку і зробить restart
