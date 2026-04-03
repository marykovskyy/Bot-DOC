import time
import logging
import database

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://or.justice.cz/ias/ui/rejstrik-$firma?jenPlatne=PLATNE&nazev={keyword}&polozek=50&typHledani=STARTS_WITH"
_PAGE_LOAD_WAIT_SEC = 2
_BASE_URL = "https://or.justice.cz/ias/ui/"


def _build_link(raw_link: str) -> str:
    """Нормалізує відносне або повне посилання до абсолютного."""
    if raw_link.startswith('http'):
        return raw_link
    raw_link = raw_link.lstrip('./')
    return f"{_BASE_URL}{raw_link}"


def scrape_czech(page, keyword: str, count: int, status: dict) -> list[dict]:
    results: list[dict] = []
    status['last_name'] = "🇨🇿 Пошук активних компаній (Чехія)..."

    page.get(_SEARCH_URL.format(keyword=keyword))

    try:
        time.sleep(_PAGE_LOAD_WAIT_SEC)

        if not page.ele('xpath://li[contains(@class, "result")]', timeout=10):
            logger.warning("Компаній за запитом '%s' не знайдено.", keyword)
            return results

        rows = page.eles('xpath://li[contains(@class, "result")]')

        for row in rows:
            if len(results) >= count:
                break
            if not status.get('is_running', True):
                break

            # Назва
            name_ele = row.ele('xpath:.//strong[contains(@class, "left")]')
            if not name_ele:
                continue
            name = name_ele.text.strip()

            # IČO
            ico = ""
            ico_th = row.ele('xpath:.//th[contains(text(), "IČO")]')
            if ico_th:
                ico = ico_th.next().text.replace(' ', '').replace('\xa0', '').strip()

            # Адреса
            address = ""
            addr_th = row.ele('xpath:.//th[contains(text(), "Sídlo")]')
            if addr_th:
                address = addr_th.next().text.replace('\n', ', ').replace('  ', ' ').strip()

            # Посилання
            link = ""
            link_ele = row.ele('xpath:.//a[contains(text(), "Výpis platných")]')
            if link_ele:
                link = _build_link(link_ele.attr('href') or "")

            if not ico or not name:
                continue

            if database.is_company_name_scraped(name):
                logger.debug("⏩ Вже є в базі: %s", name)
                continue

            status['last_name'] = f"📄 {name}"
            results.append({
                "Назва": name,
                "Статус": "ACTIVE",
                "IČO (Номер)": ico,
                "Адреса": address,
                "Посилання": link
            })
            logger.info("[%d] %s (IČO: %s)", len(results), name, ico)

    except Exception as e:
        logger.error("Помилка Czech: %s", e)

    return results
