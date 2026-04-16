"""
finland_scraper.py — Скрапер Фінляндії через відкритий API PRH YTJ v3
(Finnish Patent and Registration Office — Yritys- ja yhteisötietojärjestelmä)

API:          https://avoindata.prh.fi/opendata-ytj-api/v3/companies
Swagger:      https://avoindata.prh.fi/fi/ytj/swagger-ui
Безкоштовно, без токена, без CAPTCHA, без браузера.

Параметри запиту:
  name    — пошук за назвою (часткове співпадіння)
  page    — сторінка пагінації (починаючи з 0), ~100 результатів/стор.

Структура відповіді:
  totalResults          — загальна кількість збігів
  companies[]           — масив компаній, де кожна має:
    businessId.value    — Y-tunnus (напр. "0196816-0")
    names[]             — масив назв (беремо перший запис)
    registrationDate    — дата реєстрації (YYYY-MM-DD)
    endDate             — дата ліквідації (null = активна)
    companySituations[] — ліквідація/банкрутство (порожній = активна)
    companyForms[]      — форма власності
    addresses[]         — адреси (postOffices[], postCode, street)
    mainBusinessLine    — вид діяльності
"""
from __future__ import annotations

import logging
import time

import requests

from utils import retry_request

logger = logging.getLogger(__name__)

_BASE_URL  = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
_PER_PAGE  = 100    # API повертає ~100 результатів на сторінку
_MAX_PAGES = 50     # жорсткий ліміт: не більше 50 сторінок (5000 записів) за запит
_DELAY_SEC = 0.5
_TIMEOUT   = 30


def _get_name(company: dict) -> str:
    """Витягує основну назву компанії з масиву names[]."""
    names = company.get("names") or []
    if not names:
        return ""
    # Беремо перший активний запис (без endDate)
    for n in names:
        if not n.get("endDate"):
            return str(n.get("name", "")).strip()
    return str(names[0].get("name", "")).strip()


def _get_city(company: dict) -> str:
    """Витягує місто з першої адреси."""
    addresses = company.get("addresses") or []
    if not addresses:
        return ""
    addr = addresses[0]
    offices = addr.get("postOffices") or []
    if offices:
        return str(offices[0]).strip()
    return ""


def _get_form(company: dict) -> str:
    """Витягує форму власності (перша доступна)."""
    forms = company.get("companyForms") or []
    if not forms:
        return ""
    descriptions = forms[0].get("descriptions") or []
    # Шукаємо фінський або перший доступний опис
    for d in descriptions:
        if d.get("language") in ("FI", "fi"):
            return str(d.get("description", "")).strip()
    if descriptions:
        return str(descriptions[0].get("description", "")).strip()
    return ""


def _get_activity(company: dict) -> str:
    """Витягує вид діяльності з mainBusinessLine."""
    mbl = company.get("mainBusinessLine") or {}
    descriptions = mbl.get("descriptions") or []
    for d in descriptions:
        if d.get("language") in ("FI", "fi"):
            return str(d.get("description", "")).strip()
    if descriptions:
        return str(descriptions[0].get("description", "")).strip()
    return ""


def _is_active(company: dict) -> bool:
    """Повертає True якщо компанія активна (не ліквідована/розпущена)."""
    # 1. endDate встановлено — компанія завершила діяльність
    if company.get("endDate"):
        return False
    # 2. companySituations[] не порожній — ліквідація/банкрутство
    situations = company.get("companySituations") or []
    if situations:
        return False
    return True


def scrape_finland_api(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    """
    Шукає фінські компанії через відкритий API PRH YTJ v3.

    Повертає список словників для збереження у Excel/CSV.
    Поля: Назва, Y-tunnus, Форма, Дата реєстрації, Вид діяльності,
          Місто, Поштовий індекс, Адреса, Посилання

    Фільтри:
      - тільки активні компанії (endDate = null, companySituations = [])
      - дата реєстрації ВІД вказаного року (client-side фільтр)
      - дублікати за назвою пропускаються
    """
    target_year = str(status_dict.get("target_year", "0"))
    results: list[dict] = []
    seen_names: set[str] = set()
    page_num = 0

    year_label = f"від {target_year}" if target_year != "0" else "всі роки"
    status_dict["last_name"] = f"🇫🇮 Пошук: '{keyword}' ({year_label})..."

    while len(results) < max_count and page_num < _MAX_PAGES:
        if not status_dict.get("is_running", True):
            break

        params: dict = {
            "name": keyword,
            "page": page_num,
        }

        try:
            resp = retry_request(
                requests.get,
                _BASE_URL,
                params=params,
                timeout=_TIMEOUT,
                max_retries=3,
                delay=2.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Фінляндія API помилка (page=%d): %s", page_num, e)
            break

        companies: list = data.get("companies") or []
        total = data.get("totalResults", 0)

        if not companies:
            logger.info("Фінляндія: результатів немає (page=%d), завершуємо.", page_num)
            break

        fetched_up_to = (page_num + 1) * _PER_PAGE
        logger.info("Фінляндія: page=%d, отримано=%d, всього у API=%d",
                    page_num, len(companies), total)

        for item in companies:
            if len(results) >= max_count:
                break
            if not status_dict.get("is_running", True):
                break

            # ── Фільтр активності ──
            if not _is_active(item):
                status_dict["filtered_inactive"] = status_dict.get("filtered_inactive", 0) + 1
                continue

            # ── Фільтр по даті реєстрації (client-side) ──
            reg_date = item.get("registrationDate", "") or ""
            if target_year and target_year != "0":
                if reg_date and reg_date < f"{target_year}-01-01":
                    continue

            name = _get_name(item)
            if not name:
                continue

            # ── Дублікати по назві ──
            name_lower = name.lower()
            if name_lower in seen_names:
                status_dict["filtered_duplicate"] = status_dict.get("filtered_duplicate", 0) + 1
                continue
            seen_names.add(name_lower)

            business_id_obj = item.get("businessId") or {}
            business_id     = str(business_id_obj.get("value", "")).strip()

            # Адреса
            addresses = item.get("addresses") or []
            addr      = addresses[0] if addresses else {}
            city      = _get_city(item)
            post_code = str(addr.get("postCode", "")).strip()
            street    = str(addr.get("street", "")).strip()

            company: dict = {
                "Назва":            name,
                "Y-tunnus":         business_id,
                "Форма":            _get_form(item),
                "Дата реєстрації":  reg_date,
                "Вид діяльності":   _get_activity(item),
                "Місто":            city,
                "Поштовий індекс":  post_code,
                "Адреса":           street,
                "Посилання":        f"https://tietopalvelu.ytj.fi/yritys/{business_id}",
            }

            results.append(company)
            status_dict["last_name"] = name
            logger.info(
                "%d. %s | Y-tunnus: %s | %s",
                len(results), name, business_id, reg_date,
            )

        # Зупиняємось якщо API вичерпаний:
        #   - companies порожній (вже перевірено вище)
        #   - ми вже пройшли всі записи за totalResults
        # НЕ зупиняємось через len(companies) < _PER_PAGE —
        # API може повертати неповні сторінки на будь-якому кроці.
        if total > 0 and fetched_up_to >= total:
            logger.info("Фінляндія: досягнуто totalResults=%d (page=%d), всього: %d",
                        total, page_num, len(results))
            break

        page_num += 1
        time.sleep(_DELAY_SEC)

    return results
