import time
import os
import logging
import database

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://bilgibankasi.ito.org.tr/tr/bilgi-bankasi/firma-bilgileri"
_DOWNLOAD_DIR = os.path.join(os.getcwd(), "turkey_reports")
_ACTIVE_STATUS_KEYWORD = "faal"
_PAGE_LOAD_WAIT_SEC = 3
_RESULTS_WAIT_SEC = 4
_PDF_DOWNLOAD_WAIT_SEC = 5
_MODAL_CLOSE_WAIT_SEC = 2

# Індекси колонок
_COL_SICIL_NO = 0
_COL_NAME = 1
_MIN_COLS = 3


def _click_search(page, keyword: str) -> bool:
    """Вводить ключове слово і натискає пошук. Повертає False якщо поле не знайдено."""
    search_input = (
        page.ele('@name=FirmaUnvan')
        or page.ele('@placeholder:Ünvan')
        or page.ele('xpath://input[@type="text"]')
    )
    if not search_input:
        logger.error("Поле пошуку Туреччини не знайдено.")
        return False

    search_input.clear()
    search_input.input(keyword)

    search_btn = (
        page.ele('text:Sorgula')
        or page.ele('text:Ara')
        or page.ele('@type=submit')
    )
    if search_btn:
        search_btn.click()
    else:
        search_input.input('\n')

    return True


def _download_pdf_for_row(page, row, company_name: str, status_dict: dict) -> str:
    """Клікає Detay, завантажує PDF, закриває модальне вікно. Повертає статус."""
    detail_btn = row.ele('text:Detay') or row.ele('tag:a')
    if not detail_btn:
        return "No PDF"

    detail_btn.click()
    time.sleep(_PDF_DOWNLOAD_WAIT_SEC)

    pdf_btn = (
        page.ele('text:PDF')
        or page.ele('text:Yazdır')
        or page.ele('.fa-file-pdf')
    )

    file_status = "No PDF"
    if pdf_btn:
        try:
            status_dict['last_name'] = f"⏳ Скачування PDF: {company_name}"
            pdf_btn.click()
            time.sleep(_PDF_DOWNLOAD_WAIT_SEC)
            file_status = "✅ Збережено в turkey_reports"
            logger.info("PDF збережено: %s", company_name)
        except Exception as e:
            logger.warning("Помилка скачування PDF для '%s': %s", company_name, e)

    # Закрити модальне вікно
    close_btn = (
        page.ele('text:Kapat')
        or page.ele('text:Close')
        or page.ele('.close')
    )
    if close_btn:
        close_btn.click()
    else:
        page.back()

    time.sleep(_MODAL_CLOSE_WAIT_SEC)
    return file_status


def scrape_turkey(page, keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    os.makedirs(_DOWNLOAD_DIR, exist_ok=True)

    try:
        status_dict['last_name'] = "🇹🇷 Відкриття реєстру Туреччини (ITO)..."
        page.set.download_path(_DOWNLOAD_DIR)
        page.get(_SEARCH_URL)
        time.sleep(_PAGE_LOAD_WAIT_SEC)

        if not _click_search(page, keyword):
            page.get_screenshot(path='turkey_error_search.png')
            return results

        time.sleep(_RESULTS_WAIT_SEC)

        if not page.wait.ele_displayed('tag:table', timeout=15):
            logger.warning("Результати Туреччини не з'явились для '%s'.", keyword)
            page.get_screenshot(path='turkey_no_results.png')
            return results

        rows = page.eles('xpath://table//tbody//tr')
        logger.info("Turkey: знайдено %d рядків", len(rows))

        for i in range(min(len(rows), max_count)):
            if not status_dict.get('is_running', True):
                break

            # Повторно знаходимо рядки після кожного кліку (DOM може оновитись)
            current_rows = page.eles('xpath://table//tbody//tr')
            if i >= len(current_rows):
                break

            row = current_rows[i]
            cells = row.eles('tag:td')
            if len(cells) < _MIN_COLS:
                continue

            if _ACTIVE_STATUS_KEYWORD not in row.text.lower():
                continue

            sicil_no = cells[_COL_SICIL_NO].text.strip()
            company_name = cells[_COL_NAME].text.strip()

            if not company_name:
                continue

            if database.is_company_name_scraped(company_name):
                continue

            file_status = _download_pdf_for_row(page, row, company_name, status_dict)

            results.append({
                "Назва": company_name,
                "Статус": "FAAL (ACTIVE)",
                "IČO (Номер)": sicil_no,
                "Адреса": "Istanbul, Turkey",
                "Посилання на PDF": "Локальний файл",
                "Файл": file_status
            })

            status_dict['current'] = len(results)
            status_dict['last_name'] = company_name
            logger.info("[%d] %s", len(results), company_name)

    except Exception as e:
        logger.error("Помилка Turkey: %s", e)
        page.get_screenshot(path='turkey_crash.png')

    return results
