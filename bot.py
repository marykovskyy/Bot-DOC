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
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ConversationHandler, CallbackQueryHandler
)

from config import TOKEN
from observability import init_sentry
import database
from state import _scheduler
from analysis.ai_sorter import (
    handle_zip_documents, handle_gdrive_link, cmd_myresults,
    handle_delivery_callback, cancel_analysis_callback, cmd_analysis_logs,
    cmd_cleanup, run_auto_cleanup,
)

# ── Handlers ──
from handlers.scraping import (
    start, site_choice, save_kw, save_count, save_year,
    select_uk_mode, run_task, stop_scraping, status_updater,
    repeat_search_callback, handle_navigation,
)
from handlers.proxy import (
    proxy_menu, proxy_callback_handler, auto_update_proxy,
    handle_proxy_file, prompt_for_zip,
)
from handlers.admin import (
    is_admin, require_auth,
    show_stats, show_help, help_section_callback,
    cmd_users, cmd_adduser, cmd_removeuser, cmd_unblockuser,
    cmd_history, repeat_from_history,
)
from handlers.schedule import (
    cmd_schedule, handle_schedule_callback,
    _load_scheduled_tasks, cmd_digest,
)
from handlers.misc import (
    start_health_server, show_bot_status, restart_bot,
)
from handlers.documents import (
    build_doc_conversation, cmd_preview_template,
)
from documents.generator import load_all_templates
from state import (
    SELECT_SITE, TYPING_KEYWORD, TYPING_COUNT,
    TYPING_YEAR, SELECT_FORMAT, SELECT_UK_MODE,
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
    app.add_handler(MessageHandler(filters.Document.FileExtension("zip"), handle_zip_documents))
    app.add_handler(MessageHandler(filters.Regex(r"https://drive\.google\.com"), handle_gdrive_link))
    app.add_handler(CallbackQueryHandler(
        handle_delivery_callback,
        pattern=r"^deliver_(tg|s3|s3d|ch|done)_\d{12}(_\d+)?$"
    ))
    app.add_handler(CallbackQueryHandler(
        cancel_analysis_callback,
        pattern=r"^cancel_analysis_\d+$"
    ))

    # ── Скрапінг / зупинка ──
    app.add_handler(CallbackQueryHandler(stop_scraping, pattern="^stop_scraping$"))
    app.add_handler(CallbackQueryHandler(repeat_from_history, pattern="^repeat_\\d+$"))

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
            SELECT_SITE: [
                CallbackQueryHandler(site_choice, pattern="^site_"),
                CallbackQueryHandler(handle_navigation, pattern="^cancel_search$")
            ],
            TYPING_KEYWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_kw),
                CallbackQueryHandler(handle_navigation, pattern="^(back_|cancel_search)")
            ],
            TYPING_COUNT: [
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
        await start_health_server(port=8080)
        database.init_db()
        _load_scheduled_tasks()
        _scheduler.start()
        logger.info("Scheduler запущено.")
        await run_auto_cleanup()
        logger.info("Auto-cleanup завершено.")
        load_all_templates()  # Завантажуємо шаблони документів з templates/

        # Прогрів PaddleOCR — завантажує моделі заздалегідь (~10с)
        import asyncio
        from analysis.doc_analyzer import warmup_paddle_ocr
        await asyncio.to_thread(warmup_paddle_ocr)

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
