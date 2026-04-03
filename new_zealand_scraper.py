import time
import re
import logging
import database

logger = logging.getLogger(__name__)

_SEARCH_URL = (
    "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/search"
    "?q={keyword}&entityTypes=LTD&entityStatusGroups=REGISTERED&mode=advanced"
)
_TABLE_WAIT_TIMEOUT_SEC = 15
_PAGE_TRANSITION_WAIT_SEC = 4
_COMPANY_ID_RE = re.compile(r"'(\d+)'")


def _parse_row(row) -> dict | None:
    """Витягує дані з одного рядка таблиці. Повертає None якщо рядок не підходить."""
    row_class = row.attr('class') or ""
    if 'removed' in row_class.lower() or 'removed' in row.text.lower():
        return None

    name_ele = row.ele('.entityName')
    if not name_ele:
        return None
    name = name_ele.text.strip()

    link_ele = row.ele('.link')
    if not link_ele:
        return None

    id_match = _COMPANY_ID_RE.search(link_ele.attr('href') or "")
    if not id_match:
        return None
    company_id = id_match.group(1)

    # Адреса (необов'язково)
    address = "New Zealand"
    try:
        divs = row.ele('tag:td').eles('xpath:./div')
        if len(divs) >= 2:
            address = divs[1].text.strip()
    except Exception:
        pass

    return {
        "Назва": name,
        "Статус": "ACTIVE",
        "IČO (Номер)": company_id,
        "Адреса": address,
        "Посилання на PDF": f"https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/{company_id}"
    }


def scrape_new_zealand(page, keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    status_dict['last_name'] = "🇳🇿 Початок пошуку NZ..."

    page.get(_SEARCH_URL.format(keyword=keyword))

    try:
        while len(results) < max_count:
            if not status_dict.get('is_running', True):
                break

            if not page.wait.ele_displayed('.dataList', timeout=_TABLE_WAIT_TIMEOUT_SEC):
                logger.warning("Таблиця NZ не знайдена. Завершуємо.")
                break

            rows = page.eles('css:.dataList tbody tr')
            logger.info("NZ: обробка сторінки (%d рядків)", len(rows))

            for row in rows:
                if len(results) >= max_count:
                    break

                item = _parse_row(row)
                if not item:
                    continue

                if database.is_company_name_scraped(item["Назва"]):
                    continue

                results.append(item)
                status_dict['current'] = len(results)
                status_dict['last_name'] = item["Назва"]
                logger.info("[%d/%d] %s", len(results), max_count, item["Назва"])

            if len(results) >= max_count:
                break

            # Пагінація
            next_btn = (
                page.ele('text:Next »')
                or page.ele('.pagingNext a')
                or page.ele('xpath://a[contains(text(), "Next")]')
            )
            if next_btn:
                page.run_js('arguments[0].click();', next_btn)
                time.sleep(_PAGE_TRANSITION_WAIT_SEC)
            else:
                logger.info("NZ: більше сторінок немає.")
                break

    except Exception as e:
        logger.error("Помилка NZ: %s", e)

    return results
