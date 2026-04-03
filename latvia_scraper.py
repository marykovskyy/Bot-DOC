import time
import logging
import requests
import database

logger = logging.getLogger(__name__)

_API_URL = "https://info.ur.gov.lv/api/legalentity/search"
_ACTIVE_STATUS = "REGISTERED"
_REQUEST_DELAY_SEC = 0.1
_FETCH_MULTIPLIER = 3  # беремо з запасом, бо частина буде неактивна


def scrape_latvia(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    status_dict['last_name'] = "🇱🇻 Підключення до API Латвії..."

    params = {
        "q": keyword,
        "page": 0,
        "pageSize": max_count * _FETCH_MULTIPLIER
    }
    headers = {
        "Accept": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://info.ur.gov.lv/"
    }

    try:
        response = requests.get(_API_URL, params=params, headers=headers)
        if response.status_code != 200:
            logger.error("API Латвії відповів %d", response.status_code)
            return results

        docs = response.json().get("response", {}).get("docs", [])
        if not docs:
            logger.warning("Нічого не знайдено для '%s' (Латвія).", keyword)
            return results

        for doc in docs:
            if len(results) >= max_count:
                break
            if not status_dict.get('is_running', True):
                break

            if doc.get("status") != _ACTIVE_STATUS:
                continue

            name = doc.get("name", "N/A")
            reg_number = doc.get("regnumber", "N/A")
            address = doc.get("address", "Адреса відсутня")

            if database.is_company_name_scraped(name):
                logger.debug("⏩ Вже в базі LV: %s", name)
                continue

            profile_url = f"https://info.ur.gov.lv/#/legal-entity/{reg_number}"
            status_dict['last_name'] = f"📄 LV: {name}"

            results.append({
                "Назва": name,
                "Статус": "ACTIVE",
                "IČO (Номер)": reg_number,
                "Адреса": address,
                "Посилання на PDF": profile_url
            })

            status_dict['current'] = len(results)
            logger.info("[LV] %s (%s)", name, reg_number)
            time.sleep(_REQUEST_DELAY_SEC)

    except Exception as e:
        logger.error("Помилка Latvia: %s", e)

    return results
