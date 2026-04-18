"""
gsheets.py — клієнт Google Sheets з backoff+jitter, per-country локом і Retry-After.
"""
import logging
import os
import random
import threading
import time
from datetime import datetime

import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from google.oauth2.service_account import Credentials

from constants import SHEETS_MAX_RETRIES, SHEETS_RETRY_WAIT_BASE

logger = logging.getLogger(__name__)

SHEET_URL = os.getenv("GOOGLE_SHEET_URL")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

_MAX_RETRIES     = SHEETS_MAX_RETRIES
_RETRY_WAIT_BASE = SHEETS_RETRY_WAIT_BASE
_MAX_RETRY_WAIT  = 60.0  # стеля: навіть при 429 не чекаємо більше хвилини

# ── Thread-safe кеш з'єднання ─────────────────────────────────────────────
_cache_lock        = threading.Lock()
_gc_cache          = None
_spreadsheet_cache = None
_worksheets_cache: dict = {}
# Per-country lock: уникаємо race при паралельному add_worksheet(country) з різних потоків
_country_locks: dict[str, threading.Lock] = {}
_country_locks_guard = threading.Lock()


def _get_country_lock(country: str) -> threading.Lock:
    with _country_locks_guard:
        lock = _country_locks.get(country)
        if lock is None:
            lock = threading.Lock()
            _country_locks[country] = lock
        return lock


def _reset_cache() -> None:
    global _gc_cache, _spreadsheet_cache
    with _cache_lock:
        _gc_cache          = None
        _spreadsheet_cache = None
        _worksheets_cache.clear()


def _get_spreadsheet():
    global _gc_cache, _spreadsheet_cache

    if not SHEET_URL:
        raise RuntimeError("GOOGLE_SHEET_URL не знайдено в .env!")

    with _cache_lock:
        if _gc_cache is not None and _spreadsheet_cache is not None:
            return _spreadsheet_cache

        credentials        = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        _gc_cache          = gspread.authorize(credentials)
        # HTTP-таймаут для session — уникаємо вічного висіння на неактивних з'єднаннях
        try:
            _gc_cache.session.request = _wrap_session_timeout(_gc_cache.session.request)  # type: ignore[attr-defined]
        except Exception:
            pass
        _spreadsheet_cache = _gc_cache.open_by_url(SHEET_URL)
        return _spreadsheet_cache


def _wrap_session_timeout(orig_request):
    """Обгортка яка форсує timeout=30 якщо не передано."""
    def _req(method, url, **kw):
        kw.setdefault("timeout", 30)
        return orig_request(method, url, **kw)
    return _req


def _get_or_create_worksheet(ss, country: str):
    with _cache_lock:
        if country in _worksheets_cache:
            return _worksheets_cache[country]

    # Per-country lock — якщо два потоки одночасно створюють аркуш "France",
    # другий зачекає і використає готовий замість дублю.
    with _get_country_lock(country):
        # Повторна перевірка після взяття локу
        with _cache_lock:
            if country in _worksheets_cache:
                return _worksheets_cache[country]

        existing = [ws.title for ws in ss.worksheets()]
        if country in existing:
            ws = ss.worksheet(country)
        else:
            logger.info("Створюю новий аркуш: %s", country)
            ws = ss.add_worksheet(title=country, rows=1000, cols=4)
            ws.append_row(["Назва", "Посилання", "Країна", "Час додавання"])

        with _cache_lock:
            _worksheets_cache[country] = ws
        return ws


def _compute_wait(attempt: int, api_error: APIError | None = None) -> float:
    """Exponential backoff + jitter + Retry-After (якщо доступний)."""
    # Retry-After header — якщо сервер явно сказав скільки чекати
    if api_error is not None:
        response = getattr(api_error, "response", None)
        headers = getattr(response, "headers", None) if response is not None else None
        if headers:
            retry_after = headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), _MAX_RETRY_WAIT)
                except (TypeError, ValueError):
                    pass
    # Exponential з jitter ±25%
    base = _RETRY_WAIT_BASE * (2 ** attempt)
    jittered = base * (1 + random.uniform(-0.25, 0.25))
    return min(max(0.5, jittered), _MAX_RETRY_WAIT)


def _with_sheets_retry(op_name: str, country: str, action):
    """Виконує action() з retry-логікою (429 / 5xx / мережеві помилки).

    Args:
        op_name: для логування (append / batch)
        country: для очищення кешу аркуша при помилці
        action:  callable що виконує фактичний запит
    """
    for attempt in range(_MAX_RETRIES):
        try:
            return action()

        except SpreadsheetNotFound:
            logger.error(
                "Google Sheets: таблиця не знайдена (404).\n"
                "  Перевір GOOGLE_SHEET_URL у .env та доступ для email з credentials.json."
            )
            return None  # повторювати безглуздо

        except APIError as e:
            response = getattr(e, "response", None)
            status = getattr(response, "status_code", 0) if response is not None else 0
            if status == 429 or 500 <= status < 600:
                wait = _compute_wait(attempt, e)
                logger.warning(
                    "Google API %d (%s). Чекаємо %.1f сек (спроба %d/%d)...",
                    status, op_name, wait, attempt + 1, _MAX_RETRIES
                )
                time.sleep(wait)
                with _cache_lock:
                    _worksheets_cache.pop(country, None)
            else:
                logger.warning("Google API помилка %s (%s, спроба %d/%d): %s",
                               status, op_name, attempt + 1, _MAX_RETRIES, e)
                _reset_cache()

        except Exception as e:
            logger.warning("Помилка Sheets %s (спроба %d/%d): %s",
                           op_name, attempt + 1, _MAX_RETRIES, e)
            _reset_cache()

    logger.error("Не вдалось виконати %s у Sheets після %d спроб: %s",
                 op_name, _MAX_RETRIES, country)
    return None


# ── Один рядок ───────────────────────────────────────────────────────────────

def append_to_sheet(name: str, link: str, country: str) -> None:
    """Записує один рядок у Google Sheets (з retry при 429)."""
    def _do():
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, country)
        ws.append_row([name, link, country, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        return True

    _with_sheets_retry("append", country, _do)


# ── Batch: кілька рядків за один запит ───────────────────────────────────────

def append_rows_batch(rows: list[tuple[str, str]], country: str) -> None:
    """
    Записує кілька рядків в один HTTP-запит (batch append).

    Args:
        rows:    [(name, link), ...]
        country: назва аркуша (країна)
    """
    if not rows:
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = [[name, link, country, ts] for name, link in rows]

    def _do():
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, country)
        ws.append_rows(data, value_input_option="USER_ENTERED")  # type: ignore[arg-type]
        logger.debug("Sheets batch: %d рядків → '%s'", len(rows), country)
        return True

    _with_sheets_retry("batch", country, _do)
