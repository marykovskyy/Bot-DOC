import os
import logging
import time
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

logger = logging.getLogger(__name__)

SHEET_URL = os.getenv("GOOGLE_SHEET_URL")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

_MAX_RETRIES = 3
_RETRY_WAIT_BASE = 10

_gc_cache = None
_spreadsheet_cache = None
_worksheets_cache: dict = {}


def _reset_cache() -> None:
    global _gc_cache, _spreadsheet_cache
    _gc_cache = None
    _spreadsheet_cache = None
    _worksheets_cache.clear()


def _get_spreadsheet():
    global _gc_cache, _spreadsheet_cache

    if not SHEET_URL:
        raise RuntimeError("GOOGLE_SHEET_URL не знайдено в .env!")

    if _gc_cache is not None and _spreadsheet_cache is not None:
        return _spreadsheet_cache

    credentials = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    _gc_cache = gspread.authorize(credentials)
    _spreadsheet_cache = _gc_cache.open_by_url(SHEET_URL)
    return _spreadsheet_cache


def _get_or_create_worksheet(ss, country: str):
    if country not in _worksheets_cache:
        existing = [ws.title for ws in ss.worksheets()]
        if country in existing:
            _worksheets_cache[country] = ss.worksheet(country)
        else:
            logger.info("Створюю новий аркуш: %s", country)
            ws = ss.add_worksheet(title=country, rows=1000, cols=4)
            ws.append_row(["Назва", "Посилання", "Країна", "Час додавання"])
            _worksheets_cache[country] = ws
    return _worksheets_cache[country]


def append_to_sheet(name: str, link: str, country: str) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            ss = _get_spreadsheet()
            ws = _get_or_create_worksheet(ss, country)
            ws.append_row([name, link, country, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            return
        except Exception as e:
            err = str(e)
            if "404" in err:
                logger.error(
                    "Google Sheets 404. Перевір:\n"
                    "  1. GOOGLE_SHEET_URL у .env / token.env\n"
                    "  2. Чи наданий доступ до таблиці для email з credentials.json\n"
                    "  Помилка: %s", e
                )
                return
            elif "429" in err:
                wait = _RETRY_WAIT_BASE * (attempt + 1)
                logger.warning("Ліміт Google API (429). Чекаємо %d сек (спроба %d/%d)...",
                               wait, attempt + 1, _MAX_RETRIES)
                time.sleep(wait)
                _worksheets_cache.pop(country, None)
            else:
                logger.warning("Помилка Sheets (спроба %d/%d), скидаємо з'єднання: %s",
                               attempt + 1, _MAX_RETRIES, e)
                _reset_cache()

    logger.error("Не вдалось записати в Sheets після %d спроб: %s / %s", _MAX_RETRIES, country, name)