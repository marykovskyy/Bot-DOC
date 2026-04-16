"""
utils.py — Спільні утиліти: retry з exponential backoff, хелпери.
"""
from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Tuple, Type

logger = logging.getLogger(__name__)


def with_retry(
    max_retries: int = 3,
    delay: float = 1.5,
    backoff: float = 2.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
):
    """
    Декоратор: повторює функцію до max_retries разів при помилці.

    Затримка між спробами: delay → delay*backoff → delay*backoff² ...

    Використання:
        @with_retry(max_retries=3, delay=2.0, exceptions=(requests.RequestException,))
        def fetch(url):
            return requests.get(url, timeout=30)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: BaseException | None = None
            wait = delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        logger.warning(
                            "[retry %d/%d] %s → %s. Очікуємо %.1f сек...",
                            attempt, max_retries, func.__name__, exc, wait
                        )
                        time.sleep(wait)
                        wait *= backoff
                    else:
                        logger.error(
                            "[retry] %s: всі %d спроби вичерпано. Остання помилка: %s",
                            func.__name__, max_retries, exc
                        )
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


def retry_request(func, *args,
                  max_retries: int = 3,
                  delay: float = 1.5,
                  backoff: float = 2.0,
                  **kwargs):
    """
    Виклик функції з retry без декоратора (для вбудованих викликів).
    Використовує ту саму логіку що й with_retry — без дублювання.

    Приклад:
        data = retry_request(requests.get, url, timeout=30, max_retries=3)
    """
    @with_retry(max_retries=max_retries, delay=delay, backoff=backoff)
    def _inner():
        return func(*args, **kwargs)

    return _inner()
