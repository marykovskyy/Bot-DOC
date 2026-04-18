"""
observability.py — Ініціалізація Sentry (опціонально) та структурного контексту.

Активується якщо в .env є SENTRY_DSN. Інакше — no-op (бот працює без нього).

Використання в bot.py:
    from observability import init_sentry
    init_sentry()

Captured:
  - Всі необроблені винятки (logger.error/critical/exception)
  - Traceback з контекстом
  - Release version (з env var RELEASE_VERSION, default "dev")
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def init_sentry() -> bool:
    """Ініціалізує Sentry SDK якщо SENTRY_DSN встановлено.

    Повертає True якщо ініціалізовано, False якщо пропущено.
    Graceful: відсутність sentry-sdk або DSN не ламає бота.
    """
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        logger.debug("Sentry не налаштовано (SENTRY_DSN порожній) — пропускаю.")
        return False

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        logger.warning("SENTRY_DSN заданий, але sentry-sdk не встановлено. "
                       "pip install sentry-sdk")
        return False

    environment = os.getenv("SENTRY_ENV", "production")
    release = os.getenv("RELEASE_VERSION", "dev")
    sample_rate = float(os.getenv("SENTRY_SAMPLE_RATE", "1.0"))
    traces_sample_rate = float(os.getenv("SENTRY_TRACES_RATE", "0.0"))

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=release,
        sample_rate=sample_rate,
        traces_sample_rate=traces_sample_rate,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,        # захоплює INFO як breadcrumbs
                event_level=logging.ERROR, # ERROR і вище — надсилає як events
            ),
        ],
        # PII — НЕ надсилаємо chat_id/username користувачів за замовчуванням.
        # Якщо треба — перевизначити тегами вручну через set_tag / set_user.
        send_default_pii=False,
        # Ігноруємо часті нецікаві помилки
        ignore_errors=[
            KeyboardInterrupt,
            SystemExit,
        ],
    )
    logger.info("Sentry ініціалізовано: env=%s release=%s", environment, release)
    return True


def set_user_context(chat_id: int, username: str | None = None) -> None:
    """Прив'язує поточного користувача до контексту Sentry (опціонально)."""
    try:
        import sentry_sdk
        sentry_sdk.set_user({"id": chat_id, "username": username or ""})
    except ImportError:
        pass


def tag(key: str, value: str) -> None:
    """Додає тег до поточного scope (наприклад, country=France)."""
    try:
        import sentry_sdk
        sentry_sdk.set_tag(key, value)
    except ImportError:
        pass
