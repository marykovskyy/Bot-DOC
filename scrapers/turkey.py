"""
scrapers/turkey.py — Turkey (ITO — Istanbul Chamber of Commerce).

Підхід: чистий aiohttp до приватного API ITO. Без браузера, без DrissionPage.

Сайт: https://bilgibankasi.ito.org.tr/tr/bilgi-bankasi/firma-bilgileri
API:  https://bilgibankasi.ito.org.tr/tr/api/{commerce-title-search, company-detail}

Шифрування payload:
    Сайт використовує axios-інтерсептор який шифрує всі POST body через
    CryptoJS.AES.encrypt(JSON.stringify(form_str), AES_KEY).toString(),
    далі ще раз base64 (CryptoJS.enc.Base64.stringify(Utf8.parse(...))).

    AES_KEY: "k6e6KM7gXFyk6e6KM7gXFyk6e6KM7gXFy" (зашитий у commons.js, fallback
             якщо `window.ek` не задано — реальні юзери завжди потрапляють у fallback).
    Формат:  OpenSSL "Salted__" + 8-byte salt, KDF=MD5 EVP_BytesToKey, AES-256-CBC.
    Plaintext: "UNVAN=<keyword>&PageIndex=1&PageSize=15&apiKey=-JaNdRgUkXp2s"
               для пошуку. apiKey додається автоматично інтерсептором.

    Response — звичайний plaintext JSON (нічого розшифровувати не треба).

Структура payload:
    POST /tr/api/commerce-title-search:
        UNVAN={keyword}&PageIndex={1..N}&PageSize=15&apiKey=-JaNdRgUkXp2s
        → {"Count": N, "Index": 1, "Size": 15, "Data": [{Title, SicNumber,
                                                          CompanyStatus, Address,
                                                          District}], ...}
    POST /tr/api/company-detail:
        SICNO={base_sicil}[&MUKER={branch_id}]&apiKey=-JaNdRgUkXp2s
        → {"Data": {OfficeAddress, MersisNo, Capital, NaceCodes,
                    DateOfEstablishmentReg, ChamberOfCommerceRegHistory,
                    PhoneNumber, WebPageLink, ProfessionalGroup, ...}}

Фільтр статусу:
    Беремо тільки CompanyStatus == "Faal" (active). Решта (Terk, Askı, ...)
    відкидаємо одразу — як і в попередній версії.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import zipfile
from datetime import datetime
from hashlib import md5
from pathlib import Path
from secrets import token_bytes
from typing import Any

import aiohttp
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

import database

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────
_BASE_URL    = "https://bilgibankasi.ito.org.tr"
_SEARCH_PAGE_URL = f"{_BASE_URL}/tr/bilgi-bankasi/firma-bilgileri"
_AES_KEY     = b"k6e6KM7gXFyk6e6KM7gXFyk6e6KM7gXFy"
_API_KEY     = "-JaNdRgUkXp2s"
_PAGE_SIZE   = 15           # фіксована, як на самому сайті (захардкоджено в JS)
_REQUEST_TIMEOUT = 30
_MAX_RETRIES = 3
_DETAIL_CONCURRENCY = 4     # одночасних company-detail (щадимо ITO)
_PDF_CONCURRENCY    = 2     # exportpdf важчий — нижча паралельність
_ACTIVE_STATUS = "Faal"

# Куди складати PDF — абсолютний шлях відносно проекту (стійкий до зміни CWD)
_PROJECT_ROOT     = Path(__file__).resolve().parent.parent
_PDF_REPORTS_ROOT = _PROJECT_ROOT / "turkey_reports"

_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Content-Type":    "application/x-www-form-urlencoded;charset=UTF-8",
    "Origin":          _BASE_URL,
    "Referer":         f"{_BASE_URL}/tr/bilgi-bankasi/firma-bilgileri",
    "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/148.0.0.0 Safari/537.36"),
    "lang":            "tr-TR",
}


# ── CryptoJS-compatible AES шифрування ────────────────────────────────────

def _evp_kdf(password: bytes, salt: bytes,
             key_len: int = 32, iv_len: int = 16) -> tuple[bytes, bytes]:
    """OpenSSL EVP_BytesToKey з MD5 — дефолтний KDF у CryptoJS.

    Реалізація з RFC: ітеративно d_i = MD5(d_{i-1} || password || salt),
    конкатенуємо доки не наберемо key+iv довжину.
    """
    derived = b""
    last = b""
    while len(derived) < key_len + iv_len:
        last = md5(last + password + salt).digest()
        derived += last
    return derived[:key_len], derived[key_len:key_len + iv_len]


def _aes_wrap(plaintext_str: str) -> str:
    """Спільне ядро шифрування: AES-OpenSSL + подвійний base64.

    Це фінальні 3 кроки CryptoJS pipeline:
      1. AES-256-CBC (Salted__ + random salt + MD5 KDF) → openssl blob
      2. base64 → CryptoJS .toString() (перший шар)
      3. base64 utf8 bytes першого шару → CryptoJS.enc.Base64.stringify(Utf8.parse(...))
    """
    plaintext = json.dumps(plaintext_str, ensure_ascii=False).encode("utf-8")
    salt = token_bytes(8)
    aes_key, iv = _evp_kdf(_AES_KEY, salt)
    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
    ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))

    openssl_blob = b"Salted__" + salt + ciphertext
    first_layer  = base64.b64encode(openssl_blob).decode("ascii")
    return base64.b64encode(first_layer.encode("utf-8")).decode("ascii")


def _encrypt_params(payload: dict) -> str:
    """Шифрує form-encoded payload (для search/company-detail).

    Гілка `t.data instanceof URLSearchParams` в axios інтерсепторі:
      n = "key1=val1&key2=val2&...&apiKey=-JaNdRgUkXp2s"
    """
    form_parts = [f"{k}={v}" for k, v in payload.items() if v not in (None, "")]
    form_parts.append(f"apiKey={_API_KEY}")
    return _aes_wrap("&".join(form_parts))


def _encrypt_json_params(payload: dict) -> str:
    """Шифрує JSON-payload (для exportpdf — там nested objects/arrays).

    Гілка `else` в axios інтерсепторі:
      t.data = {...t.data, apiKey: "-JaNdRgUkXp2s"}
      n = JSON.stringify(t.data)
    """
    full = {**payload, "apiKey": _API_KEY}
    inner = json.dumps(full, ensure_ascii=False, separators=(",", ":"))
    return _aes_wrap(inner)


# ── HTTP layer ────────────────────────────────────────────────────────────

async def _post_encrypted(session: aiohttp.ClientSession, endpoint: str,
                          payload: dict, *, json_mode: bool = False) -> dict | None:
    """POST {endpoint} з зашифрованими params. Retry на 429/5xx, без retry на 4xx.

    json_mode=False (default) — payload шифрується як form-encoded (search/detail).
    json_mode=True             — payload шифрується як JSON (exportpdf).
    """
    encrypted = _encrypt_json_params(payload) if json_mode else _encrypt_params(payload)
    body = f"params={encrypted}"
    url = f"{_BASE_URL}/tr/api/{endpoint}"
    last_err = "unknown"

    for attempt in range(_MAX_RETRIES):
        try:
            async with session.post(url, data=body) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                last_err = f"http_{r.status}"
                # Permanent client errors — не retry'ємо
                if r.status in (400, 401, 403, 404):
                    text = (await r.text())[:200]
                    logger.warning("ITO %s %s: %s", endpoint, r.status, text)
                    return None
                # 429 / 5xx — exponential backoff
                await asyncio.sleep(0.5 * (2 ** attempt))
        except TimeoutError:
            last_err = "timeout"
            await asyncio.sleep(0.5 * (2 ** attempt))
        except aiohttp.ClientError as e:
            last_err = f"client:{type(e).__name__}"
            await asyncio.sleep(0.5 * (2 ** attempt))

    logger.warning("ITO %s exhausted retries: %s", endpoint, last_err)
    return None


async def _search_page(session: aiohttp.ClientSession, keyword: str,
                       page_index: int) -> tuple[list[dict], int]:
    """Один POST commerce-title-search → (rows, total_count)."""
    data = await _post_encrypted(session, "commerce-title-search", {
        "UNVAN":     keyword,
        "PageIndex": page_index,
        "PageSize":  _PAGE_SIZE,
    })
    if not isinstance(data, dict) or not data.get("Result"):
        return [], 0
    rows = data.get("Data") or []
    total = int(data.get("Count") or 0)
    return rows, total


async def _fetch_detail(session: aiohttp.ClientSession, sicil_no: str) -> dict:
    """POST company-detail для одного SicNumber.

    ITO SicNumber може мати форму "1001967" (основна компанія) або
    "100283-5" (філія, де "5" — номер філії = MUKER).
    """
    parts = sicil_no.split("-", 1)
    payload: dict[str, Any] = {"SICNO": parts[0]}
    if len(parts) > 1 and parts[1]:
        payload["MUKER"] = parts[1]
    data = await _post_encrypted(session, "company-detail", payload)
    if not isinstance(data, dict) or not data.get("Result"):
        return {}
    return data.get("Data") or {}


# Поля з company-detail що передаються в exportpdf (з HAR-аналізу).
# Сервер відмалює PDF тільки з цих полів — допоміжні масиви (former titles,
# partners, директори тощо) залишаємо порожніми, бо ми їх не запитуємо.
_PDF_DETAIL_FIELDS = (
    "CompanyStatus", "SicNumber", "ChamberOfCommerce", "CompanyTitle",
    "OfficeAddress", "PhoneNumber", "WebPageLink",
    "ChamberOfCommerceRegHistory", "ProfessionalGroup", "NaceCodes",
)


async def _fetch_pdf(session: aiohttp.ClientSession, detail: dict) -> bytes | None:
    """POST exportpdf → bytes PDF (декодовані з base64), None при помилці.

    Payload — повний обʼєкт companyDetails + порожні допоміжні масиви.
    Server-side рендер PDF робиться через Skia (Chrome headless), повертається
    base64 у полі Data.
    """
    if not detail:
        return None

    # ChamberOfCommerce → ChamberOfCommerceDetails (рендерер очікує саме це імʼя)
    cd = {k: detail.get(k) for k in _PDF_DETAIL_FIELDS}
    cd["ChamberOfCommerceDetails"] = cd.pop("ChamberOfCommerce", "") or ""

    payload = {
        "formerTitles":          [],
        "gazeteBilgileri":       [],
        "partners":              [],
        "oldPartners":           [],
        "companyOfficials":      [],
        "oldCompanyOfficials":   [],
        "branchAddress":         [],
        "boardOfDirectors":      [],
        "oldBoardOfDirectors":   [],
        "companyDetails":        cd,
    }
    data = await _post_encrypted(session, "exportpdf", payload, json_mode=True)
    if not isinstance(data, dict) or not data.get("Result"):
        return None
    b64 = data.get("Data") or ""
    if not isinstance(b64, str):
        return None
    try:
        return base64.b64decode(b64)
    except Exception as e:
        logger.warning("TR exportpdf: невалідний base64: %s", e)
        return None


# ── Pipeline ──────────────────────────────────────────────────────────────

_FILENAME_BAD = re.compile(r'[<>:"/\\|?*\x00-\x1F]')


def _safe_filename(name: str, max_len: int = 80) -> str:
    """Прибирає символи неприпустимі для Windows-шляхів + обрізає по довжині."""
    cleaned = _FILENAME_BAD.sub("_", name).strip(" .")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:max_len] or "company"

def _build_result_row(search_row: dict, detail: dict,
                      pdf_filename: str = "") -> dict:
    """Зливає поля пошуку + деталі у фінальний словник для Excel.

    pdf_filename — імʼя файлу в ZIP-архіві (без шляху). Юзер відкриє ZIP і
    знайде PDF за цим імʼям. Якщо PDF не скачано — порожньо.
    """
    nace_codes = detail.get("NaceCodes") or []
    return {
        "Назва":                search_row.get("Title", "").strip(),
        "Статус":               "FAAL (Active)",
        "Sicil No":             search_row.get("SicNumber", "") or "",
        "MERSIS":               detail.get("MersisNo", "") or "",
        "Дата заснування":      detail.get("DateOfEstablishmentReg", "") or "",
        "Дата реєстрації ITO":  detail.get("ChamberOfCommerceRegHistory", "") or "",
        "Статутний капітал":    detail.get("Capital", "") or "",
        "NACE":                 ", ".join(nace_codes) if nace_codes else "",
        "Address":              detail.get("OfficeAddress") or search_row.get("Address", "") or "",
        "District":             search_row.get("District", "") or "",
        "City":                 "Istanbul",
        "Country":              "Turkey",
        "Телефон":              detail.get("PhoneNumber", "") or "",
        "Сайт":                 detail.get("WebPageLink", "") or "",
        "Професійна група":     detail.get("ProfessionalGroup", "") or "",
        "Tax Number":           detail.get("TaxNumber", "") or "",
        "ITO Search":           _SEARCH_PAGE_URL,
        "PDF файл":             pdf_filename or "—",
    }


def _make_zip(pdf_dir: Path, zip_path: Path, allowed_names: set[str]) -> int:
    """Пакує у ZIP тільки PDF імена яких є в allowed_names.

    Запит може скачати +N "зайвих" PDF (якщо база відсіяла дублікати після
    download). Сюди передаємо тільки реально потрібні — щоб ZIP точно
    відповідав Excel-результату.
    """
    count = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for pdf in sorted(pdf_dir.glob("*.pdf")):
            if pdf.name not in allowed_names:
                continue
            zf.write(pdf, arcname=pdf.name)
            count += 1
    return count


async def _run_async(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    """Основний pipeline: пошук → деталі → PDF → ZIP."""
    results: list[dict] = []
    timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)

    # Окрема підпапка per session — щоб ZIP мав тільки файли цього запуску
    session_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_dir = _PDF_REPORTS_ROOT / session_tag
    pdf_dir.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession(headers=_HEADERS, timeout=timeout) as session:
        detail_sem = asyncio.Semaphore(_DETAIL_CONCURRENCY)
        pdf_sem    = asyncio.Semaphore(_PDF_CONCURRENCY)

        async def _fetch_one(row: dict) -> dict | None:
            """Повний pipeline для однієї компанії: detail + PDF. None = пропускаємо."""
            biz_name = (row.get("Title") or "").strip()
            sicil    = (row.get("SicNumber") or "").strip()
            if not biz_name or not sicil:
                return None
            if database.is_company_name_scraped(biz_name):
                logger.debug("⏩ TR: вже в базі: %s", biz_name)
                return None

            async with detail_sem:
                detail = await _fetch_detail(session, sicil)

            # PDF — у власному семафорі (важчий, менша паралельність)
            pdf_filename = ""
            async with pdf_sem:
                pdf_bytes = await _fetch_pdf(session, detail)
            if pdf_bytes:
                safe = _safe_filename(biz_name)
                pdf_filename = f"{sicil}_{safe}.pdf"
                pdf_path = pdf_dir / pdf_filename
                try:
                    pdf_path.write_bytes(pdf_bytes)
                except OSError as e:
                    logger.warning("TR PDF write fail '%s': %s", pdf_filename, e)
                    pdf_filename = ""
            else:
                logger.debug("TR PDF не отримано для %s", biz_name)

            return {"row": row, "detail": detail, "pdf_filename": pdf_filename}

        page_index = 1
        while len(results) < max_count and status_dict.get("is_running", True):
            rows, total = await _search_page(session, keyword, page_index)
            if not rows:
                if page_index == 1:
                    logger.warning("TR: ITO API повернув порожньо для '%s'", keyword)
                break

            # Фільтр ACTIVE одразу — не палити detail+PDF на закритих
            active_rows = [
                r for r in rows
                if (r.get("CompanyStatus") or "").strip() == _ACTIVE_STATUS
            ]
            # Беремо тільки скільки потрібно щоб не качати зайві PDF.
            # +2 — невеликий запас на випадок якщо хтось буде відфільтрований
            # (вже в БД, без назви) — щоб з лишком вистачило.
            needed = max_count - len(results)
            batch = active_rows[: needed + 2]
            logger.info("TR p%d: %d Faal з %d рядків (total=%d, беремо %d)",
                        page_index, len(active_rows), len(rows), total, len(batch))

            for fut in asyncio.as_completed([_fetch_one(r) for r in batch]):
                if not status_dict.get("is_running", True):
                    break
                bundle = await fut
                if not bundle:
                    continue
                if len(results) >= max_count:
                    break

                results.append(_build_result_row(
                    bundle["row"], bundle["detail"], bundle["pdf_filename"]
                ))
                status_dict["current"]   = len(results)
                status_dict["last_name"] = bundle["row"].get("Title", "")[:60]
                logger.info("[%d] TR %s | Sicil=%s | MERSIS=%s | PDF=%s",
                            len(results),
                            bundle["row"].get("Title", "")[:40],
                            bundle["row"].get("SicNumber", ""),
                            bundle["detail"].get("MersisNo", "—"),
                            "✓" if bundle["pdf_filename"] else "—")

            if page_index * _PAGE_SIZE >= total:
                break
            page_index += 1

    # ── Пакуємо тільки PDF що ввійшли у results у ZIP ───────────────────
    # extra_zip_path — окремий слот, status_updater відправить ZIP після Excel.
    # Беремо саме results-imena, бо batch=needed+2 міг скачати "зайві" PDF.
    needed_pdfs = {r["PDF файл"] for r in results if r.get("PDF файл") and r["PDF файл"] != "—"}
    if needed_pdfs:
        zip_path = _PDF_REPORTS_ROOT / f"turkey_{session_tag}.zip"
        try:
            packed = _make_zip(pdf_dir, zip_path, needed_pdfs)
            if packed > 0:
                status_dict["extra_zip_path"] = str(zip_path)
                logger.info("TR: упаковано %d PDF у %s", packed, zip_path.name)
        except Exception as e:
            logger.warning("TR ZIP fail: %s", e)

    return results


# ── Точка входу (сумісна зі старою сигнатурою диспетчера) ────────────────

def scrape_turkey(page, keyword: str, count: int, status_dict: dict) -> list[dict]:
    """Синхронна обгортка для виклику з потоку scraper-worker.

    `page` параметр ігнорується — новий ITO-скрапер ходить чистим aiohttp
    без браузера. Залишений у сигнатурі для зворотньої сумісності з
    диспетчером (scrapers/main.py).
    """
    status_dict["last_name"] = "🇹🇷 ITO API: пошук компаній..."
    try:
        return asyncio.run(_run_async(keyword, count, status_dict))
    except Exception as e:
        logger.error("TR scrape failed: %s", e, exc_info=True)
        return []
