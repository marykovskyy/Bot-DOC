"""
intl_doc_analyzer.py — розширення doc_analyzer.py під документи різних країн.

Що додає (порівняно з базовим doc_analyzer.py):
  1. **Space-separated date pattern** — Swiss/EU IDs друкують `04 02 26`
     (DD MM YY з пробілами) замість `04.02.26`.
  2. **Розширені multilingual labels** — додано CHE/FR/DE/IT варіанти що
     зустрічаються на національних ID-картках.
  3. **Country detection по MRZ** — швидке визначення країни з 3-літерного
     коду в MRZ (CHE, FRA, DEU, GBR, USA тощо).
  4. **Tolerant MRZ parser** — якщо OCR з'їв пробіли/символи, ми все одно
     знаходимо expiry за позиційними маркерами (M/F gender + 6-digit YYMMDD
     після).
  5. **Fallback "near-label"** — якщо знайшли expiry-keyword але дати поруч
     нема (OCR пропустив), беремо першу 6-значну дату НИЖЧЕ за keyword.

Використання:
    from analysis.intl_doc_analyzer import analyze_image_intl

    result = analyze_image_intl(image_bytes)
    # → {exp_date: '2027-01-15', country: 'CHE', source: 'mrz_tolerant', ...}

Цей модуль НЕ зачіпає існуючий doc_analyzer.py — це окремий entrypoint
що використовує internal helpers з doc_analyzer.
"""
from __future__ import annotations

import logging
import re
from datetime import date

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────
#  ПАТЕРНИ — на основі реальних документів які я бачив
# ─────────────────────────────────────────────────────────────────────────

# Swiss ID на back видає дату формату "04 02 26" або "04.02.26".
# Доповнюємо існуючі патерни ще одним: space-separated.
SPACE_DATE_RE = re.compile(r"\b(\d{2})\s+(\d{2})\s+(\d{2,4})\b")

# Country codes (ISO-3 alpha) які зустрічаються в MRZ
ISO3_CODES = {
    "CHE": "Switzerland", "FRA": "France", "DEU": "Germany",
    "GBR": "United Kingdom", "USA": "United States", "CAN": "Canada",
    "AUS": "Australia", "NZL": "New Zealand", "ITA": "Italy",
    "ESP": "Spain", "PRT": "Portugal", "NLD": "Netherlands",
    "BEL": "Belgium", "POL": "Poland", "CZE": "Czechia",
    "SVK": "Slovakia", "HUN": "Hungary", "AUT": "Austria",
    "DNK": "Denmark", "SWE": "Sweden", "NOR": "Norway",
    "FIN": "Finland", "IRL": "Ireland", "ROU": "Romania",
    "BGR": "Bulgaria", "HRV": "Croatia", "SVN": "Slovenia",
    "EST": "Estonia", "LVA": "Latvia", "LTU": "Lithuania",
    "LUX": "Luxembourg", "GRC": "Greece", "MEX": "Mexico",
    "BRA": "Brazil", "ARG": "Argentina", "CHL": "Chile",
    "JPN": "Japan", "KOR": "South Korea", "IND": "India",
    "MYS": "Malaysia", "SGP": "Singapore", "ISR": "Israel",
    "TUR": "Turkey", "UKR": "Ukraine", "RUS": "Russia",
}

# Регекс для пошуку ISO-3 коду в MRZ або тексті
ISO3_RE = re.compile(r"\b(" + "|".join(ISO3_CODES.keys()) + r")\b")

# Розширені expiry-метки (доповнення до базових з doc_analyzer)
# Зокрема Swiss IDs мають: "Gültig bis", "Date d'expiration", "Data di scadenza"
# на одній картці — багатомовний друк.
EXTRA_EXPIRY_LABELS = [
    # Французькі варіації що бачив у CH/FR ID:
    r"date\s*d['']?\s*expiration",
    r"d['']?\s*expiration",
    r"valable\s+jusqu['']?[au\s]*",
    r"expire\s+le",
    # Німецькі:
    r"g[uü]ltig\s+bis",
    r"ablaufdatum",
    # Італійські:
    r"data\s+di\s+scadenza",
    r"data\s+scadenza",
    # Іспанські:
    r"fecha\s+de\s+caducidad",
    r"v[áa]lida\s+hasta",
    # Португальські:
    r"validade",
    r"data\s+de\s+validade",
    # Чеські/Словацькі:
    r"platnost\s+do",
    r"platnost",
    r"datum\s+expirace",
    # Польські:
    r"termin\s+ważności",
    r"data\s+ważności",
    # Голландські:
    r"geldig\s+tot",
    # Угорські:
    r"érvényes",
    r"lejárat",
    # Скандинавські:
    r"gyldig\s+til",       # DK/NO/SE
    r"giltig\s+till",      # SE
    r"voimassa",           # FI
    # Російська/українська (рідко):
    r"д[еі]йств[іи]т[еі]льно\s+до",
    r"д[ау]та\s+истечения",
    # Турецька/інші:
    r"geçerlilik\s+sonu",
    r"son\s+kullanma",
]


# ─────────────────────────────────────────────────────────────────────────
#  TOLERANT MRZ PARSER
# ─────────────────────────────────────────────────────────────────────────

def _mrz_extract_tolerant(text: str) -> dict:
    """Знаходить MRZ-fields навіть якщо OCR з'їв пробіли/символи.

    Стратегія: шукаємо позиційні маркери у тексті:
      - 6-digit DOB + check + sex (M/F/<) + 6-digit expiry + check + 3-letter country
      Структура: \\d{6}\\d[MF<]\\d{6}\\d[A-Z]{3}

    Повертає dict: {dob, sex, exp_iso, country_iso3}
    """
    result = {"dob": None, "sex": None, "exp_iso": None, "country_iso3": None}

    # Очищаємо текст від OCR-сміття
    cleaned = text.upper()
    for old, new in (("О", "O"), ("С", "C"), ("В", "B"), ("Н", "H"),
                     ("{", "<"), ("[", "<"), ("|", "<"), ("(", "<")):
        cleaned = cleaned.replace(old, new)

    # Шукаємо паттерн DOB(6) + check(1) + sex(M/F/<) + EXP(6) + check(1) + country(3)
    pattern = re.compile(r"(\d{6})(\d)([MF<])(\d{6})(\d)([A-Z]{3})")
    m = pattern.search(cleaned)
    if not m:
        return result

    dob_raw, _, sex, exp_raw, _, country = m.groups()

    # DOB → ISO
    try:
        yy, mm, dd = int(dob_raw[:2]), int(dob_raw[2:4]), int(dob_raw[4:6])
        # MRZ DOB: 00-30 → 2000-2030, 31-99 → 1931-1999
        year = 2000 + yy if yy <= 30 else 1900 + yy
        result["dob"] = date(year, mm, dd).strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        pass

    # EXP → ISO
    try:
        yy, mm, dd = int(exp_raw[:2]), int(exp_raw[2:4]), int(exp_raw[4:6])
        year = 2000 + yy if yy <= 50 else 1900 + yy
        result["exp_iso"] = date(year, mm, dd).strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        pass

    if sex in ("M", "F"):
        result["sex"] = sex
    if country in ISO3_CODES:
        result["country_iso3"] = country

    return result


# ─────────────────────────────────────────────────────────────────────────
#  SPACE-SEPARATED DATE → ISO
# ─────────────────────────────────────────────────────────────────────────

def _parse_space_date(d_str: str) -> str | None:
    """`04 02 26` → `2026-02-04`. Підтримує і 4-значний рік.

    Heuristic: assume DD MM YY (European). Якщо перше число >12 → точно DD.
    Якщо третє число 2-значне → 20XX.
    """
    parts = d_str.strip().split()
    if len(parts) != 3:
        return None
    try:
        a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None

    # Визначаємо рік
    if c < 100:
        year = 2000 + c if c <= 50 else 1900 + c
    else:
        year = c

    # Визначаємо порядок DD/MM vs MM/DD
    if a > 12 and 1 <= b <= 12:
        dd, mm = a, b
    elif b > 12 and 1 <= a <= 12:
        mm, dd = a, b
    else:
        # За замовчуванням DD MM YY (європейський стиль на CHE/EU IDs)
        dd, mm = a, b

    try:
        return date(year, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────
#  EXP BY LABEL FALLBACK
# ─────────────────────────────────────────────────────────────────────────

def _extract_exp_by_label(text: str) -> str | None:
    """Шукає дату поряд з multi-language expiry label.

    Логіка: для кожної мітки знаходимо її позицію, потім беремо першу дату
    у наступних 60 символах. Це обробляє випадки коли OCR розірвав рядок
    і дата опинилась нижче або поряд через табуляцію.
    """
    text_lower = text.lower()

    # Збираємо всі мітки в один регекс
    pattern = re.compile("(" + "|".join(EXTRA_EXPIRY_LABELS) + ")", re.IGNORECASE)

    for m in pattern.finditer(text_lower):
        end = m.end()
        window = text[end:end + 80]  # 80 символів після мітки

        # Пробуємо різні формати дати у вікні
        # DD/MM/YYYY або MM/DD/YYYY
        for date_re in (
            r"(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})",
            r"(\d{1,2})\s+(\d{1,2})\s+(\d{2,4})",
            r"(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})",
        ):
            dm = re.search(date_re, window)
            if dm:
                g = dm.groups()
                try:
                    if len(g[0]) == 4:    # YYYY-MM-DD
                        year, mm, dd = int(g[0]), int(g[1]), int(g[2])
                    else:                  # DD MM YY/YYYY
                        a, b, c = int(g[0]), int(g[1]), int(g[2])
                        year = c if c >= 100 else (2000 + c if c <= 50 else 1900 + c)
                        # Heuristic для DD/MM vs MM/DD
                        if a > 12 and 1 <= b <= 12:
                            dd, mm = a, b
                        elif b > 12 and 1 <= a <= 12:
                            mm, dd = a, b
                        else:
                            dd, mm = a, b   # default EU style
                    return date(year, mm, dd).strftime("%Y-%m-%d")
                except (ValueError, IndexError):
                    continue
    return None


# ─────────────────────────────────────────────────────────────────────────
#  COUNTRY / DOC TYPE DETECTION
# ─────────────────────────────────────────────────────────────────────────

def _detect_country(text: str) -> str | None:
    """Визначає 3-літерний ISO-код країни з тексту або MRZ."""
    text_upper = text.upper()
    m = ISO3_RE.search(text_upper)
    if m:
        return m.group(1)

    # Назви країн у відкритому тексті
    name_to_iso = {
        "SWITZERLAND": "CHE", "SCHWEIZ": "CHE", "SUISSE": "CHE",
        "SVIZZERA": "CHE",
        "FRANCE": "FRA", "RÉPUBLIQUE FRANÇAISE": "FRA",
        "GERMANY": "DEU", "DEUTSCHLAND": "DEU",
        "UNITED KINGDOM": "GBR", "BRITAIN": "GBR",
        "UNITED STATES": "USA",
        "ITALIA": "ITA", "ITALY": "ITA",
        "ESPAÑA": "ESP", "SPAIN": "ESP",
        "ČESKÁ REPUBLIKA": "CZE", "CZECH REPUBLIC": "CZE",
        "POLSKA": "POL", "POLAND": "POL",
    }
    for name, code in name_to_iso.items():
        if name in text_upper:
            return code
    return None


def _detect_doc_type(text: str) -> str:
    """Класифікує тип документа: driver_license / id_card / passport / other."""
    low = text.lower()

    if any(k in low for k in ("passport", "passeport", "reisepass", "passaporto")):
        return "passport"
    if any(k in low for k in (
        "driver license", "driver's license", "driving licence",
        "permis de conduire", "führerschein", "patente di guida",
        "operator license", "operator's license", "driving licence",
    )):
        return "driver_license"
    if any(k in low for k in (
        "identification card", "identity card", "id card",
        "carte d'identité", "carta d'identità", "personalausweis",
        "identitätskarte",
    )):
        return "id_card"
    return "other"


# ─────────────────────────────────────────────────────────────────────────
#  ГОЛОВНА ФУНКЦІЯ: analyze_image_intl
# ─────────────────────────────────────────────────────────────────────────

def analyze_image_intl(image_bytes: bytes, client_id: str = "") -> dict:
    """Аналізує одне зображення документа міжнародного формату.

    Pipeline:
      1. OCR через існуючий doc_analyzer (PaddleOCR + Tesseract)
      2. Tolerant MRZ parser (для EU ID/passport)
      3. Standard MRZ parser (TD1/TD3) — fallback
      4. Multi-language label-based extraction
      5. Country detection

    Returns:
      {
        "exp_date": "2027-01-15" або None,
        "country": "CHE" або None,
        "doc_type": "id_card" / "driver_license" / "passport" / "other",
        "source": "mrz_tolerant" / "mrz_td1" / "mrz_td3" / "label" / None,
        "ocr_text": str (повний текст OCR — для дебагу),
      }
    """
    # Імпортуємо internals з doc_analyzer пізно, щоб модуль працював
    # і без важких залежностей (paddleocr) — для тестів patterns.
    try:
        from io import BytesIO

        from PIL import Image

        from analysis.doc_analyzer import (
            _normalize_text,
            _paddle_ocr_text,
        )
        img = Image.open(BytesIO(image_bytes)).convert("RGB")
        text = _paddle_ocr_text(img)
        text = _normalize_text(text)
    except Exception as e:
        logger.warning("OCR failed for %s: %s", client_id, e)
        text = ""

    result = {
        "exp_date": None,
        "country": _detect_country(text),
        "doc_type": _detect_doc_type(text),
        "source": None,
        "ocr_text": text,
    }

    # 1. Tolerant MRZ
    mrz_data = _mrz_extract_tolerant(text)
    if mrz_data["exp_iso"]:
        result["exp_date"] = mrz_data["exp_iso"]
        result["source"] = "mrz_tolerant"
        if mrz_data["country_iso3"] and not result["country"]:
            result["country"] = mrz_data["country_iso3"]
        return result

    # 2. Existing TD1/TD3 MRZ parser
    try:
        from analysis.doc_analyzer import _extract_expiry_from_mrz
        iso = _extract_expiry_from_mrz(text)
        if iso:
            result["exp_date"] = iso
            result["source"] = "mrz_strict"
            return result
    except Exception:
        pass

    # 3. Multi-language label-based extraction
    iso = _extract_exp_by_label(text)
    if iso:
        result["exp_date"] = iso
        result["source"] = "label_intl"
        return result

    # 4. Спочатку пробуємо ШВИДКИЙ regex-парсер на готовому OCR-тексті
    #    (рятує час для безпечних випадків).
    try:
        from analysis.doc_analyzer import _find_expiry_in_text
        out = _find_expiry_in_text(text)
        if out:
            iso, has_kw = out
            if iso and has_kw:    # тільки якщо знайдено поруч з keyword (надійно)
                result["exp_date"] = iso
                result["source"] = "us_label_fast"
                return result
    except Exception as e:
        logger.debug("_find_expiry_in_text failed: %s", e)

    # 5. ТОЧНИЙ повний pipeline — з усіма rotations/preprocessing-варіаціями.
    #    Це повільно (~10s/photo) але потрібно для блюрних/перевернутих US DL.
    try:
        from analysis.doc_analyzer import local_analyze
        local_result = local_analyze(image_bytes, client_id=client_id)
        if local_result.get("exp_date"):
            result["exp_date"] = local_result["exp_date"]
            result["source"] = "doc_analyzer_full"
            if not result["country"] and local_result.get("country"):
                result["country"] = local_result["country"]
            if not result["doc_type"] or result["doc_type"] == "other":
                if local_result.get("doc_type"):
                    result["doc_type"] = local_result["doc_type"]
            return result
    except Exception as e:
        logger.debug("doc_analyzer.local_analyze failed: %s", e)

    return result
