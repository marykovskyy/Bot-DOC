import logging
import time

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

# ── Статус компанії (DBD показує колонку สถานะ / Status) ─────────────────
# Активні: тільки "ยังดำเนินกิจการอยู่" (still operating). Усе інше —
# ліквідація / банкрутство / викреслення — відкидаємо (вимога проєкту:
# не віддавати закриті компанії, інакше Google Ads бан).
_ACTIVE_STATUS_KEYWORDS = (
    "ยังดำเนินกิจการ",   # TH: still operating (активна)
    "still operating",   # EN-версія DBD
    "active",
)
_INACTIVE_STATUS_KEYWORDS = (
    "เสร็จการชำระบัญชี",  # ліквідація завершена
    "ร้าง",               # викреслена (defunct)
    "เลิก",               # розпущена / dissolved
    "พิทักษ์ทรัพย์",       # під опікою майна (receivership)
    "ล้มละลาย",           # банкрутство
    "liquidat", "dissolv", "defunct", "cancelled", "canceled", "bankrupt",
)


def _detect_status(cells) -> tuple[str, bool]:
    """Сканує всі клітинки рядка на ключові слова статусу.

    Повертає (raw_status_text, is_active).
    Жорстко: is_active=True ТІЛЬКИ при явному активному маркері.
    Якщо знайдено маркер неактивності — is_active=False.
    Якщо статус узагалі не визначено — is_active=False (консервативно,
    щоб закрита компанія випадково не пройшла як активна).
    """
    joined = " ".join((c.text or "") for c in cells).lower()
    if any(k in joined for k in _INACTIVE_STATUS_KEYWORDS):
        return joined, False
    if any(k.lower() in joined for k in _ACTIVE_STATUS_KEYWORDS):
        return joined, True
    return joined, False


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

        # ── Діагностика: дамп клітинок першого рядка ──
        # На першому реальному запуску одразу видно структуру таблиці й те,
        # якими словами DBD позначає статус → можна точно підкрутити keywords.
        if rows:
            try:
                first_cells = [c.text.strip() for c in rows[0].eles('tag:td')]
                logger.info("Thailand: колонки 1-го рядка (%d): %s",
                            len(first_cells), first_cells)
            except Exception:
                pass

        for row in rows:
            if len(results) >= max_count:
                break
            if not status_dict.get('is_running', True):
                break

            cells = row.eles('tag:td')
            if len(cells) < _MIN_COLS:
                continue

            juristic_id = cells[_COL_JURISTIC_ID].text.strip()
            company_name = cells[_COL_COMPANY_NAME].text.strip()

            if not company_name or not juristic_id:
                continue

            # ── Фільтр статусу: тільки реально активні ──
            raw_status, is_active = _detect_status(cells)
            if not is_active:
                status_dict["filtered_inactive"] = status_dict.get("filtered_inactive", 0) + 1
                logger.debug("⏩ TH: skip неактивну '%s' (status=%r)",
                             company_name, raw_status[:80])
                continue

            if database.is_company_name_scraped(company_name):
                continue

            profile_url = _PROFILE_BASE_URL.format(juristic_id=juristic_id)
            results.append({
                "Назва": company_name,
                "Статус": "ยังดำเนินกิจการอยู่ (Active)",
                "เลขทะเบียน (Juristic ID)": juristic_id,
                "Адреса": "Thailand",
                "Посилання на PDF": profile_url
            })

            status_dict['current'] = len(results)
            status_dict['last_name'] = company_name
            logger.info("[%d] %s (%s)", len(results), company_name, juristic_id)

    except Exception as e:
        logger.error("Помилка Thailand: %s", e)

    return results
