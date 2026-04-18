import time
import logging
import requests
import database

logger = logging.getLogger(__name__)

_API_URL = "https://info.ur.gov.lv/api/legalentity/search"
_ACTIVE_STATUS = "REGISTERED"
_REQUEST_DELAY_SEC = 0.1
_MAX_PAGE_SIZE = 100  # API обмеження: pageSize <= 100


def scrape_latvia(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    status_dict['last_name'] = "🇱🇻 Підключення до API Латвії..."

    headers = {
        "Accept": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://info.ur.gov.lv/"
    }

    page = 0
    while len(results) < max_count:
        if not status_dict.get('is_running', True):
            break

        params = {
            "q": keyword,
            "page": page,
            "pageSize": _MAX_PAGE_SIZE
        }

        try:
            response = requests.get(_API_URL, params=params, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.error("API Латвії відповів %d", response.status_code)
                break

            data = response.json().get("response", {})
            docs = data.get("docs", [])
            if not docs:
                if page == 0:
                    logger.warning("Нічого не знайдено для '%s' (Латвія).", keyword)
                break

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

            # Якщо отримали менше ніж pageSize — більше сторінок немає
            if len(docs) < _MAX_PAGE_SIZE:
                break

            page += 1

        except Exception as e:
            logger.error("Помилка Latvia: %s", e)
            break

    return results
