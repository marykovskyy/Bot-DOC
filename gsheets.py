import logging
import os
import threading
import time

import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime

from constants import SHEETS_MAX_RETRIES, SHEETS_RETRY_WAIT_BASE

logger = logging.getLogger(__name__)

SHEET_URL = os.getenv("GOOGLE_SHEET_URL")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

_MAX_RETRIES     = SHEETS_MAX_RETRIES
_RETRY_WAIT_BASE = SHEETS_RETRY_WAIT_BASE

# ── Thread-safe кеш з'єднання ─────────────────────────────────────────────
_cache_lock        = threading.Lock()
_gc_cache          = None
_spreadsheet_cache = None
_worksheets_cache: dict = {}


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
        _spreadsheet_cache = _gc_cache.open_by_url(SHEET_URL)
        return _spreadsheet_cache


def _get_or_create_worksheet(ss, country: str):
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


# ── Один рядок ───────────────────────────────────────────────────────────────

def append_to_sheet(name: str, link: str, country: str) -> None:
    """Записує один рядок у Google Sheets (з retry при 429)."""
    for attempt in range(_MAX_RETRIES):
        try:
            ss = _get_spreadsheet()
            ws = _get_or_create_worksheet(ss, country)
            ws.append_row([name, link, country, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            return

        except SpreadsheetNotFound:
            logger.error(
                "Google Sheets: таблиця не знайдена (404).\n"
                "  Перевір GOOGLE_SHEET_URL у .env та доступ для email з credentials.json."
            )
            return  # повторювати безглуздо

        except APIError as e:
            status = e.response.status_code if hasattr(e, "response") else 0
            if status == 429:
                wait = _RETRY_WAIT_BASE * (attempt + 1)
                logger.warning("Google API rate limit (429). Чекаємо %d сек (спроба %d/%d)...",
                               wait, attempt + 1, _MAX_RETRIES)
                time.sleep(wait)
                with _cache_lock:
                    _worksheets_cache.pop(country, None)
            else:
                logger.warning("Google API помилка %s (спроба %d/%d): %s",
                               status, attempt + 1, _MAX_RETRIES, e)
                _reset_cache()

        except Exception as e:
            logger.warning("Помилка Sheets (спроба %d/%d), скидаємо з'єднання: %s",
                           attempt + 1, _MAX_RETRIES, e)
            _reset_cache()

    logger.error("Не вдалось записати в Sheets після %d спроб: %s / %s",
                 _MAX_RETRIES, country, name)


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

    for attempt in range(_MAX_RETRIES):
        try:
            ss = _get_spreadsheet()
            ws = _get_or_create_worksheet(ss, country)
            ws.append_rows(data, value_input_option="USER_ENTERED")  # type: ignore[arg-type]
            logger.debug("Sheets batch: %d рядків → '%s'", len(rows), country)
            return

        except SpreadsheetNotFound:
            logger.error("Google Sheets: таблиця не знайдена (404). Перевір GOOGLE_SHEET_URL.")
            return

        except APIError as e:
            status = e.response.status_code if hasattr(e, "response") else 0
            if status == 429:
                wait = _RETRY_WAIT_BASE * (attempt + 1)
                logger.warning("Google API 429 (batch). Чекаємо %d сек...", wait)
                time.sleep(wait)
                with _cache_lock:
                    _worksheets_cache.pop(country, None)
            else:
                logger.warning("Google API помилка %s (batch, спроба %d/%d): %s",
                               status, attempt + 1, _MAX_RETRIES, e)
                _reset_cache()

        except Exception as e:
            logger.warning("Помилка Sheets batch (спроба %d/%d): %s",
                           attempt + 1, _MAX_RETRIES, e)
            _reset_cache()

    logger.error("Не вдалось записати batch у Sheets після %d спроб: %s (%d рядків)",
                 _MAX_RETRIES, country, len(rows))
