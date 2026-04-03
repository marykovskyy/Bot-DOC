import json
import logging
import database

logger = logging.getLogger(__name__)

_ACTIVE_STATUSES = frozenset({'NORMAL', 'AKTIV', 'ACTIVE'})
_LISTEN_TIMEOUT_SEC = 15
_SKIP_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.css', '.js', '.woff2', '.svg', '.ico')


def _extract_companies(data) -> list:
    """Рекурсивно шукає список компаній у довільній JSON-структурі."""
    if isinstance(data, list):
        if data and isinstance(data[0], dict) and 'cvr' in data[0] and 'senesteNavn' in data[0]:
            return data
        for item in data:
            res = _extract_companies(item)
            if res:
                return res
    elif isinstance(data, dict):
        for value in data.values():
            res = _extract_companies(value)
            if res:
                return res
    return []


def scrape_denmark(page, keyword: str, count: int, status: dict) -> list[dict]:
    results: list[dict] = []
    status['last_name'] = "🇩🇰 Пошук активних компаній (Данія)..."

    page.listen.start()
    page.get(f"https://datacvr.virk.dk/soegeresultater?fritekst={keyword}&sideIndex=0&size=100")

    try:
        items: list = []

        for packet in page.listen.steps(timeout=_LISTEN_TIMEOUT_SEC):
            if not status.get('is_running', True):
                break

            url = packet.request.url.lower()
            if any(url.endswith(ext) for ext in _SKIP_EXTENSIONS):
                continue

            raw_body = packet.response.body
            if not raw_body:
                continue

            str_body = str(raw_body).lower()
            if 'cvr' not in str_body or 'senestenavn' not in str_body:
                continue

            try:
                data = raw_body if not isinstance(raw_body, str) else json.loads(raw_body)
                items = _extract_companies(data)
                if items:
                    logger.info("Знайдено API з компаніями Данії.")
                    break
            except Exception:
                continue

        if not items:
            logger.warning("Компаній за запитом '%s' не знайдено.", keyword)
            return results

        for item in items:
            if len(results) >= count:
                break
            if not status.get('is_running', True):
                break

            cvr = str(item.get('cvr', '')).strip()
            name = str(item.get('senesteNavn', '')).strip()
            comp_status = str(item.get('status', '')).strip().upper()

            if not cvr or not name:
                continue

            if comp_status not in _ACTIVE_STATUSES:
                logger.debug("⏩ Пропуск (статус %s): %s", comp_status, name)
                status["filtered_inactive"] = status.get("filtered_inactive", 0) + 1
                continue

            # Перевірка наявності адреси
            has_address = any([
                str(item.get('vejnavn', '')).strip(),
                str(item.get('postnummer', '')).strip(),
                str(item.get('postdistrikt', '')).strip(),
            ])
            if not has_address:
                logger.debug("⏩ Немає адреси: %s", name)
                continue

            if database.is_company_name_scraped(name):
                logger.debug("⏩ Вже є в базі: %s", name)
                status["filtered_duplicate"] = status.get("filtered_duplicate", 0) + 1
                continue

            link = f"https://datacvr.virk.dk/enhed/virksomhed/{cvr}"
            status['last_name'] = f"📄 {name}"

            results.append({
                "Назва": name,
                "Статус": comp_status,
                "CVR (Номер)": cvr,
                "Посилання": link
            })
            logger.info("[%d] %s (CVR: %s)", len(results), name, cvr)

    except Exception as e:
        logger.error("Помилка Denmark: %s", e)
    finally:
        page.listen.stop()

    return results
