import logging
import os
import time
from datetime import datetime

import requests

import database

logger = logging.getLogger(__name__)

UK_API_KEY: str = os.getenv("UK_COMPANIES_API_KEY") or ""

if not UK_API_KEY:
    import warnings
    warnings.warn("UK_COMPANIES_API_KEY не знайдено в .env — UK scraper не працюватиме")

_DOWNLOAD_TIMEOUT_SEC = 15
_API_RATE_LIMIT_SLEEP = 1


def download_pdf(pdf_url: str, save_path: str) -> bool:
    if not pdf_url:
        return False
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(pdf_url, headers=headers, allow_redirects=True, timeout=_DOWNLOAD_TIMEOUT_SEC)
        if r.status_code == 200 and r.content.startswith(b"%PDF"):
            with open(save_path, 'wb') as f:
                f.write(r.content)
            return True
        return False
    except Exception as e:
        logger.warning("Помилка скачування PDF: %s", e)
        return False


def get_all_document_links(company_number: str) -> dict[str, str]:
    docs: dict[str, str] = {}
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/filing-history"
    try:
        res = requests.get(url, params={"items_per_page": 100}, auth=(str(UK_API_KEY), ''), timeout=30)
        if res.status_code != 200:
            return docs
        for item in res.json().get('items', []):
            transaction_id = item.get('transaction_id')
            has_pdf = 'document_metadata' in item.get('links', {})
            if not transaction_id or not has_pdf:
                continue

            doc_type = item.get('type', 'DOC').upper()
            date = item.get('date', 'UNKNOWN')
            base_filename = f"{doc_type}_{date}"
            filename = f"{base_filename}.pdf"

            counter = 1
            while filename in docs:
                filename = f"{base_filename}_{counter}.pdf"
                counter += 1

            link = (
                f"https://find-and-update.company-information.service.gov.uk"
                f"/company/{company_number}/filing-history/{transaction_id}"
                f"/document?format=pdf&download=1"
            )
            docs[filename] = link
    except Exception as e:
        logger.warning("Помилка отримання документів для %s: %s", company_number, e)
    return docs


def scrape_uk_api(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    results: list[dict] = []
    target_year = str(status_dict.get('target_year', '2025'))

    # Режим: True = скачувати PDF на диск, False = тільки посилання в таблицю
    do_download: bool = status_dict.get('uk_download_pdf', True)

    if do_download:
        status_dict['last_name'] = f"🇬🇧 Шукаю компанії від {target_year} (з завантаженням PDF)..."
        base_dir = os.path.join(os.getcwd(), "uk_reports")
        today_str = datetime.now().strftime("%Y-%m-%d")
        date_dir = os.path.join(base_dir, today_str)
        os.makedirs(date_dir, exist_ok=True)
    else:
        status_dict['last_name'] = f"🇬🇧 Шукаю компанії від {target_year} (тільки посилання)..."
        date_dir = None

    # ── Пагінація: Companies House search підтримує start_index ──
    # Раніше бралися лише топ-100 збігів на слово: повторний прогін з тим
    # самим словом давав 0 (усе вже в базі — "слово вичерпане"). Тепер
    # гортаємо сторінки далі, доки не набрано max_count або збіги скінчились.
    _PAGE_SIZE = 100
    _MAX_PAGES = 10          # захисна межа: до 1000 збігів на ключове слово

    try:
        for page_num in range(_MAX_PAGES):
            if len(results) >= max_count:
                break
            if not status_dict.get('is_running', True):
                break

            start_index = page_num * _PAGE_SIZE
            response = requests.get(
                "https://api.company-information.service.gov.uk/search/companies",
                params={"q": keyword, "items_per_page": _PAGE_SIZE,
                        "start_index": start_index},
                auth=(str(UK_API_KEY), ''),
                timeout=30,
            )
            if response.status_code != 200:
                logger.error("UK API відповів %d (start_index=%d)",
                             response.status_code, start_index)
                break

            items = response.json().get('items', [])
            if not items:
                logger.info("UK '%s': збіги скінчились на сторінці %d",
                            keyword, page_num + 1)
                break
            if page_num > 0:
                logger.info("UK '%s': сторінка %d (start_index=%d, зібрано %d/%d)",
                            keyword, page_num + 1, start_index, len(results), max_count)

            _process_page(items, results, max_count, target_year, status_dict,
                          do_download, date_dir)

            time.sleep(0.5)   # пауза між сторінками пошуку (rate limit CH: 600 зап./5 хв)

    except Exception as e:
        logger.error("Помилка UK API: %s", e)

    return results


def _process_page(items: list, results: list[dict], max_count: int,
                  target_year: str, status_dict: dict,
                  do_download: bool, date_dir: str | None) -> None:
    """Обробляє одну сторінку пошукових збігів (фільтри + збір документів)."""
    try:
        for item in items:
            if len(results) >= max_count:
                break
            if not status_dict.get('is_running', True):
                break
            if item.get('company_status') != 'active':
                continue

            creation_date = item.get('date_of_creation', '')
            if target_year != '0' and (not creation_date or creation_date < f"{target_year}-01-01"):
                continue

            name = item.get('title', 'N/A')
            safe_name = " ".join("".join(c for c in name if c.isalnum() or c == ' ').split())
            company_number = item.get('company_number', 'N/A')
            address = item.get('address', {}).get('snippet', 'Адреса відсутня')

            if database.is_company_name_scraped(name):
                continue

            logger.info("Обробка: %s (%s)", name, company_number)
            status_dict['last_name'] = f"📄 {name}"

            # Отримуємо документи та застосовуємо фільтри для ОБОХ режимів
            docs = get_all_document_links(company_number)
            if not docs:
                continue

            # Фільтр DS01 (ліквідація) — відсіюємо в обох режимах
            if any("DS01" in fname for fname in docs):
                logger.info("Відбраковано (DS01): %s", name)
                continue

            # Посилання NEWINC — ОБОВ'ЯЗКОВО, без нього компанія не потрапляє в результат
            newinc_link = next(
                (lnk.replace("download=1", "download=0") for fname, lnk in docs.items() if "NEWINC" in fname),
                None
            )
            if not newinc_link:
                logger.info("Пропущено (немає NEWINC): %s", name)
                status_dict.setdefault('filtered_no_newinc', 0)
                status_dict['filtered_no_newinc'] += 1
                continue
            main_link = newinc_link

            # ── РЕЖИМ 1: тільки посилання ──────────────────────────────────
            if not do_download:
                results.append({
                    "Назва": name,
                    "Статус": "ACTIVE",
                    "Дата створення": creation_date,
                    "Номер компанії": company_number,
                    "Адреса": address,
                    "Посилання на PDF": main_link
                })
                status_dict['current'] = len(results)
                time.sleep(_API_RATE_LIMIT_SLEEP)
                continue

            # ── РЕЖИМ 2: скачувати PDF на диск ─────────────────────────────
            company_folder = f"{safe_name}_{company_number}"
            assert date_dir is not None  # завжди True коли do_download=True
            company_dir = os.path.join(date_dir, company_folder)
            os.makedirs(company_dir, exist_ok=True)

            downloaded_types: list[str] = []
            for filename, link in docs.items():
                if download_pdf(link, os.path.join(company_dir, filename)):
                    downloaded_types.append(filename.split('_')[0])

            if not downloaded_types:
                try:
                    os.rmdir(company_dir)
                except OSError:
                    pass
                continue

            director_forms = {'AP01', 'AP02', 'CH01', 'CH02'}
            has_director = bool(director_forms & set(downloaded_types))

            results.append({
                "Назва": name,
                "Статус": "ACTIVE",
                "Дата створення": creation_date,
                "Номер компанії": company_number,
                "Адреса": address,
                "Документи Директора": "✅ Є форма (AP01/CH01)" if has_director else "❌ Відсутня",
                "Завантажено файлів": f"✅ {len(downloaded_types)} шт. ({', '.join(downloaded_types)})",
                "Папка": company_folder,
                "Посилання на PDF": main_link
            })
            status_dict['current'] = len(results)
            time.sleep(_API_RATE_LIMIT_SLEEP)

    except Exception as e:
        logger.error("Помилка обробки сторінки UK: %s", e)
