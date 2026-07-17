"""
india.py — Скрапер Індії через офіційний Open Data API (data.gov.in).

Джерело: датасет "Registrars of Companies (RoC)-wise Company Master Data"
         Міністерства корпоративних справ (MCA), ~3.67 млн компаній.
         resource_id: 4dbe5667-7b6b-41d7-82af-211562424d9a
API:     https://api.data.gov.in/resource/{resource_id}
Ключ:    За замовчуванням — публічний ключ, вбудований у фронтенд самого
         data.gov.in (той самий, яким портал показує превью/download будь-якому
         відвідувачу БЕЗ реєстрації). Тому скрапер працює «з коробки».
         За бажання можна задати власний INDIA_DATA_GOV_API_KEY у token.env
         (data.gov.in → My Account → API key) — тоді використається він
         (вищі ліміти запитів).

Особливості цього API (визначають логіку скрапера):
  • filters[Поле]=значення — ТІЛЬКИ точний збіг. Немає пошуку за назвою,
    діапазону дат чи сортування на стороні сервера.
  • Тому «свіжі компанії від року N» реалізовано ітерацією по датах
    реєстрації день-за-днем назад від сьогодні до 1 січня року N.
  • keyword — опційний КЛІЄНТСЬКИЙ фільтр за підрядком у назві. Спец-значення
    ('*', '0', 'all', 'усі', 'все', '-') = без фільтра (усі компанії).

Дані MCA лагують на ~1-2 місяці позаду календарної дати, тож перші дні від
сьогодні зазвичай порожні — це нормально (порожній день = 1 швидкий запит).

Повертає: Назва, CIN, Дата реєстрації, Адреса, Статус, Клас, Штат, RoC + лінк.
Документів (PDF) немає — MCA продає завірені копії окремо на mca.gov.in.
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

import database

load_dotenv("token.env")

logger = logging.getLogger(__name__)

# Публічний ключ, вбудований у фронтенд data.gov.in (використовується порталом
# для превью/download будь-кому без реєстрації). Дає доступ до того самого
# API, тому дозволяє збирати дані «з коробки». Якщо користувач має власний
# ключ — INDIA_DATA_GOV_API_KEY з token.env має пріоритет (вищі ліміти).
_PORTAL_API_KEY = "579b464db66ec23bdd0000015ccfae5e282347146ed579583a2c4559"
_API_KEY = os.getenv("INDIA_DATA_GOV_API_KEY", "").strip() or _PORTAL_API_KEY
_RESOURCE_ID = "4dbe5667-7b6b-41d7-82af-211562424d9a"
_BASE_URL = f"https://api.data.gov.in/resource/{_RESOURCE_ID}"

_PAGE_SIZE = 100          # ліміт записів на запит (документований безпечний максимум)
_DELAY_SEC = 0.3          # пауза між запитами (ввічливий scraping)
_TIMEOUT = 30
_MAX_FETCH_RETRIES = 4    # спроб на один offset до відмови (проти нескінченного циклу)

# Заголовки «як у браузера». Без них WAF data.gov.in тихо підвішує запит
# (read timeout) — саме тому голий python-requests таймаутить, а фронтенд
# порталу (той самий api.data.gov.in) відповідає миттєво. Referer/Origin
# додаємо бо ключ прив'язаний до фронтенду data.gov.in.
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.data.gov.in/",
    "Origin": "https://www.data.gov.in",
}
# Порожніх днів поспіль до зупинки. Data.gov.in лагує на ~1-2 міс, тож треба
# перетнути цей проміжок від сьогодні до перших наявних даних. 400 днів
# страхує навіть від ~13 міс лагу; у заповненому діапазоні великих прогалин нема.
_MAX_EMPTY_STREAK = 400
_MAX_TOTAL_DAYS = 366 * 12   # запобіжник від нескінченного циклу (12 років назад)

# keyword-значення, що означають «без фільтра за назвою»
_WILDCARDS = {"*", "0", "-", "all", "усі", "уси", "все", "всі", "any", ""}

# Джерело-довідка (PDF-документів API не надає)
_MCA_REF = "https://www.mca.gov.in/  (verify by CIN)"

# ── Company State Code ───────────────────────────────────────────────────
# Точні значення поля CompanyStateCode у датасеті (= пункти випадаючого
# списку "Company State Code" на сторінці data.gov.in). Зберігаються в нижньому
# регістрі — саме так фільтрує сам сайт (filters[CompanyStateCode]=maharashtra).
# Це єдине джерело правди: keyboards.py і handlers імпортують цей кортеж.
INDIA_STATE_CODES: tuple[str, ...] = (
    "andaman and nicobar islands", "andhra pradesh", "arunachal pradesh",
    "assam", "bihar", "chandigarh", "chattisgarh", "dadra & nagar haveli",
    "daman and diu", "delhi", "goa", "gujarat", "haryana", "himachal pradesh",
    "jammu & kashmir", "jharkhand", "karnataka", "kerala", "ladakh",
    "lakshadweep", "madhya pradesh", "maharashtra", "manipur", "meghalaya",
    "mizoram", "nagaland", "orissa", "pondicherry", "punjab", "rajasthan",
    "sikkim", "tamil nadu", "telangana", "tripura", "uttar pradesh",
    "uttarakhand", "west bengal",
)
_STATE_SET = set(INDIA_STATE_CODES)


def _canon_state(raw: str) -> str:
    """Нормалізує назву штату → канонічне значення CompanyStateCode або ''.

    '' означає «штат не задано» → скрапер працює у режимі ітерації за датою
    (зворотна сумісність зі старою поведінкою).
    """
    s = re.sub(r"\s+", " ", (raw or "").strip().lower())
    return s if s in _STATE_SET else ""


def _build_record(r: dict) -> dict:
    """Будує рядок результату з одного запису API (спільно для обох режимів)."""
    return {
        "Назва":            str(r.get("CompanyName") or "").strip(),
        "CIN":              str(r.get("CIN") or ""),
        "Статус":           "ACTIVE",
        "Дата реєстрації":  str(r.get("CompanyRegistrationdate_date") or ""),
        "Клас":             str(r.get("CompanyClass") or ""),
        "Категорія":        str(r.get("CompanyCategory") or ""),
        "Штат":             str(r.get("CompanyStateCode") or ""),
        "RoC":              str(r.get("CompanyROCcode") or ""),
        "Адреса":           str(r.get("Registered_Office_Address") or "").strip(),
        "Посилання на PDF": _MCA_REF,
    }


def _is_active(status: str) -> bool:
    """MCA-статус активної компанії. Значення в датасеті — 'Active'."""
    return str(status or "").strip().lower() in ("active", "actv")


def _fetch(filters: dict[str, str], offset: int) -> tuple[list[dict], int]:
    """Один запит до API з довільними filters[...]. Повертає (records, total).

    total < 0 сигналізує про помилку/rate-limit (щоб викликач зробив бекоф).
    """
    params = {
        "api-key": _API_KEY,
        "format": "json",
        "limit": _PAGE_SIZE,
        "offset": offset,
    }
    params.update(filters)
    try:
        resp = requests.get(_BASE_URL, params=params, headers=_HEADERS, timeout=_TIMEOUT)
        # Rate-limit / помилка ключа приходять як JSON {"error": ...} з HTTP 200
        try:
            data = resp.json()
        except ValueError:
            logger.warning("India: не-JSON відповідь (HTTP %s), filters=%s", resp.status_code, filters)
            return [], -1
        if isinstance(data, dict) and data.get("error"):
            logger.warning("India API: %s", data.get("error"))
            return [], -1
        records = data.get("records", []) if isinstance(data, dict) else []
        total = int(data.get("total", 0) or 0)
        return records, total
    except Exception as e:
        logger.warning("India: помилка запиту filters=%s: %s", filters, e)
        return [], -1


def _scrape_by_state(state_code: str, keyword: str, max_count: int,
                     status_dict: dict) -> list[dict]:
    """Збирає компанії конкретного штату через filters[CompanyStateCode].

    Повторює логіку сторінки data.gov.in (вибір штату → превью → download):
    пагінація за offset по всьому набору штату. keyword — опційний фільтр за
    підрядком назви; рік реєстрації (target_year) — опційний клієнтський фільтр.
    """
    results: list[dict] = []

    kw = (keyword or "").strip().lower()
    name_filter = None if kw in _WILDCARDS else kw

    target_year = str(status_dict.get("target_year", "0"))
    try:
        min_year = int(target_year) if target_year != "0" else 0
    except ValueError:
        min_year = 0

    filter_label = f"назва містить '{name_filter}'" if name_filter else "усі назви"
    year_label = f"від {min_year}" if min_year else "усі роки"
    status_dict["last_name"] = f"🇮🇳 {state_code.title()} — завантаження..."
    logger.info("India: збір за штатом '%s' (ліміт %d, %s, %s)",
                state_code, max_count, filter_label, year_label)

    seen_names: set[str] = set()
    filters = {"filters[CompanyStateCode]": state_code}
    offset = 0
    fails = 0

    while len(results) < max_count:
        if not status_dict.get("is_running", True):
            break

        records, total = _fetch(filters, offset)
        if total < 0:
            fails += 1
            if fails >= _MAX_FETCH_RETRIES:
                logger.error("India: штат '%s' — API не відповідає після %d спроб, "
                             "зупиняю (зібрано %d)", state_code, fails, len(results))
                status_dict["last_name"] = ("⚠️ data.gov.in не відповідає (таймаут). "
                                            "Спробуйте пізніше або інший штат.")
                break
            time.sleep(3.0)          # rate-limit / помилка — бекоф і повтор offset
            continue
        fails = 0
        if not records:
            break

        for r in records:
            if len(results) >= max_count:
                break
            if not _is_active(r.get("CompanyStatus")):
                status_dict["filtered_inactive"] = status_dict.get("filtered_inactive", 0) + 1
                continue

            name = str(r.get("CompanyName") or "").strip()
            if not name:
                continue
            if name_filter and name_filter not in name.lower():
                continue

            reg = str(r.get("CompanyRegistrationdate_date") or "")
            if min_year and not (reg[:4].isdigit() and int(reg[:4]) >= min_year):
                continue

            name_lower = name.lower()
            if name_lower in seen_names:
                continue
            if database.is_company_name_scraped(name):
                status_dict["filtered_duplicate"] = status_dict.get("filtered_duplicate", 0) + 1
                continue
            seen_names.add(name_lower)

            results.append(_build_record(r))
            status_dict["current"] = len(results)
            status_dict["last_name"] = f"🇮🇳 {name}"
            logger.info("[IN/%s] %d. %s | %s | %s", state_code, len(results),
                        name, r.get("CIN"), reg)

        offset += _PAGE_SIZE
        if offset >= total:
            break
        time.sleep(_DELAY_SEC)

    logger.info("India: штат '%s' завершено, зібрано %d компаній", state_code, len(results))
    return results


def scrape_india_api(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    """Збирає активні індійські компанії через data.gov.in Open Data API.

    Два режими залежно від status_dict["state_code"]:
      • штат заданий → _scrape_by_state: фільтр filters[CompanyStateCode],
        пагінація за offset по всьому штату (як превью/download на сайті).
      • штат порожній → ітерація дат реєстрації назад від сьогодні до
        1 січня target_year (стара поведінка).
    keyword — опційний фільтр за назвою в обох режимах.
    """
    results: list[dict] = []

    if not _API_KEY:
        logger.error("India: INDIA_DATA_GOV_API_KEY не заданий у token.env — скрапер вимкнено")
        status_dict["last_name"] = ("⚠️ Немає INDIA_DATA_GOV_API_KEY у token.env. "
                                    "Отримати: data.gov.in → My Account → API key.")
        return results

    # ── Режим за штатом (як на сторінці data.gov.in) ──
    # Якщо користувач обрав Company State Code — фільтруємо за штатом і
    # гортаємо весь набір за offset. Інакше — стара ітерація за датою.
    state_code = _canon_state(str(status_dict.get("state_code", "")))
    if state_code:
        return _scrape_by_state(state_code, keyword, max_count, status_dict)

    target_year = str(status_dict.get("target_year", "0"))
    try:
        floor_date = date(int(target_year), 1, 1) if target_year != "0" else date(1900, 1, 1)
    except ValueError:
        floor_date = date(1900, 1, 1)

    # keyword → опційний фільтр за підрядком назви
    kw = (keyword or "").strip().lower()
    name_filter = None if kw in _WILDCARDS else kw

    filter_label = f"назва містить '{name_filter}'" if name_filter else "усі назви"
    year_label = f"від {target_year}" if target_year != "0" else "усі роки"
    status_dict["last_name"] = f"🇮🇳 Пошук ({year_label}, {filter_label})..."
    logger.info("India: старт (%s, %s, ліміт %d)", year_label, filter_label, max_count)

    seen_names: set[str] = set()
    cur = date.today()
    empty_streak = 0
    days_scanned = 0

    while len(results) < max_count and cur >= floor_date and days_scanned < _MAX_TOTAL_DAYS:
        if not status_dict.get("is_running", True):
            break

        days_scanned += 1
        reg_date = cur.isoformat()
        offset = 0
        day_had_records = False

        # ── Пагінація в межах одного дня ──
        while len(results) < max_count:
            if not status_dict.get("is_running", True):
                break

            records, total = _fetch({"filters[CompanyRegistrationdate_date]": reg_date}, offset)
            if total < 0:
                # rate-limit / помилка — бекоф і повтор цього ж offset
                time.sleep(3.0)
                continue
            if not records:
                break
            day_had_records = True

            for r in records:
                if len(results) >= max_count:
                    break
                if not _is_active(r.get("CompanyStatus")):
                    continue

                name = str(r.get("CompanyName") or "").strip()
                if not name:
                    continue

                if name_filter and name_filter not in name.lower():
                    continue

                name_lower = name.lower()
                if name_lower in seen_names:
                    continue
                if database.is_company_name_scraped(name):
                    continue
                seen_names.add(name_lower)

                results.append(_build_record(r))
                status_dict["current"] = len(results)
                status_dict["last_name"] = f"🇮🇳 {name}"
                logger.info("[IN] %d. %s | %s | %s", len(results), name,
                            r.get("CIN"), r.get("CompanyRegistrationdate_date"))

            offset += _PAGE_SIZE
            if offset >= total:
                break
            time.sleep(_DELAY_SEC)

        # ── Керування «межею свіжості» ──
        if day_had_records:
            empty_streak = 0
        else:
            empty_streak += 1
            if empty_streak >= _MAX_EMPTY_STREAK:
                logger.info("India: %d порожніх днів поспіль — завершую (зібрано %d)",
                            empty_streak, len(results))
                break

        cur -= timedelta(days=1)
        time.sleep(_DELAY_SEC)

    logger.info("India: завершено, зібрано %d компаній", len(results))
    return results
