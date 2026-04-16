import time
import logging
import database

logger = logging.getLogger(__name__)

_HOME_URL = "https://datawarehouse.dbd.go.th/index"
_SEARCH_URL = "https://datawarehouse.dbd.go.th/juristic/searchInfo?keyword={keyword}"
_TABLE_SELECTOR = 'css:#table-filter-data tbody tr.cursor-pointer'
_HOME_LOAD_WAIT_SEC = 3
_PROFILE_BASE_URL = "https://datawarehouse.dbd.go.th/company/profile/{juristic_id}"

# Індекси колонок у таблиці результатів
_COL_JURISTIC_ID = 2
_COL_COMPANY_NAME = 3
_MIN_COLS = 6


def scrape_thailand(page, keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    status_dict['last_name'] = "🇹🇭 Пошук у Таїланді..."

    try:
        # Сайт потребує попереднього відвідування домашньої сторінки (сесійні куки)
        page.get(_HOME_URL)
        time.sleep(_HOME_LOAD_WAIT_SEC)

        page.get(_SEARCH_URL.format(keyword=keyword))

        if not page.wait.ele_displayed(_TABLE_SELECTOR, timeout=20):
            logger.warning("Таблиця Таїланду не з'явилась для '%s'.", keyword)
            return results

        # Зберігаємо рядки один раз — уникаємо повторних запитів до DOM
        rows = page.eles(_TABLE_SELECTOR)
        logger.info("Thailand: знайдено %d рядків", len(rows))

        for row in rows[:max_count]:
            if not status_dict.get('is_running', True):
                break

            cells = row.eles('tag:td')
            if len(cells) < _MIN_COLS:
                continue

            juristic_id = cells[_COL_JURISTIC_ID].text.strip()
            company_name = cells[_COL_COMPANY_NAME].text.strip()

            if not company_name or not juristic_id:
                continue

            if database.is_company_name_scraped(company_name):
                continue

            profile_url = _PROFILE_BASE_URL.format(juristic_id=juristic_id)
            results.append({
                "Назва": company_name,
                "Статус": "ACTIVE",
                "IČO (Номер)": juristic_id,
                "Адреса": "Thailand",
                "Посилання на PDF": profile_url
            })

            status_dict['current'] = len(results)
            status_dict['last_name'] = company_name
            logger.info("[%d] %s (%s)", len(results), company_name, juristic_id)

    except Exception as e:
        logger.error("Помилка Thailand: %s", e)

    return results
