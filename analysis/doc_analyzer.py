"""
doc_analyzer.py — Швидкий локальний аналіз документів на строк придатності.

Використовує Tesseract OCR (~1-2 сек/фото на CPU) замість EasyOCR (15-30 сек).
Оптимізовано для batch 300+ документів.

Порядок:
  1. MRZ-crop (нижні 25%) → бінаризація → Tesseract → парсинг дати
  2. Якщо MRZ не знайдено → повний текст → пошук дати за ключовими словами
  3. Якщо локально не знайдено → fallback на Textract (в ai_sorter.py)

Інтеграція: ai_sorter._analyze_single_image() викликає local_analyze().
"""
from __future__ import annotations

import io
import os
import re
import logging
import platform
import threading
import numpy as np
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Діагностичний логер ────────────────────────────────────────────────
# Пише детальний лог кожного кроку аналізу у файл analysis_debug.log
# Кожен рядок має префікс [client_id] для фільтрації при паралельній обробці.

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEBUG_LOG_PATH = os.path.join(_PROJECT_ROOT, "analysis_debug.log")

_diag_logger: Optional[logging.Logger] = None
_diag_ctx = threading.local()  # thread-local: зберігає client_id поточного потоку


def _get_diag_logger() -> logging.Logger:
    """Lazy init діагностичного логера у файл."""
    global _diag_logger
    if _diag_logger is not None:
        return _diag_logger

    _diag_logger = logging.getLogger("doc_analyzer.diag")
    _diag_logger.setLevel(logging.DEBUG)
    _diag_logger.propagate = False  # не дублювати в консоль

    handler = logging.FileHandler(_DEBUG_LOG_PATH, encoding="utf-8", mode="a")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _diag_logger.addHandler(handler)

    return _diag_logger


def _diag(msg: str) -> None:
    """Записує рядок в діагностичний лог з [client_id] префіксом (з flush)."""
    cid = getattr(_diag_ctx, 'client_id', '')
    prefix = f"[{cid}] " if cid else ""
    lgr = _get_diag_logger()
    lgr.debug(f"{prefix}{msg}")
    for h in lgr.handlers:
        h.flush()


def _diag_separator(client_id: str = "") -> None:
    """Початок нового документа в лозі + встановлює client_id для потоку."""
    _diag_ctx.client_id = client_id
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _diag(f"{'=' * 60}")
    _diag(f"START {ts}")
    _diag(f"{'=' * 60}")

# ── Шлях до Tesseract ───────────────────────────────────────────────────

_TESSERACT_CMD: str | None = None


def _find_tesseract() -> str | None:
    """Знаходить tesseract в системі."""
    global _TESSERACT_CMD
    if _TESSERACT_CMD is not None:
        return _TESSERACT_CMD

    import shutil

    # Шукаємо в PATH
    path = shutil.which("tesseract")
    if path:
        _TESSERACT_CMD = path
        return path

    # Стандартні шляхи на Windows
    if platform.system() == "Windows":
        import os
        for candidate in [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Programs\Tesseract-OCR\tesseract.exe"),
            os.path.expandvars(r"%LOCALAPPDATA%\Tesseract-OCR\tesseract.exe"),
        ]:
            if os.path.isfile(candidate):
                _TESSERACT_CMD = candidate
                return candidate

    _TESSERACT_CMD = ""  # порожній рядок = не знайдено (але шукали)
    return None


def _tesseract_available() -> bool:
    """Перевіряє доступність Tesseract."""
    cmd = _find_tesseract()
    return bool(cmd)


# ── Tesseract OCR ───────────────────────────────────────────────────────

def _ocr_image(img, config: str = "") -> str:
    """Запускає Tesseract OCR на PIL Image. Повертає текст.

    Додає --dpi 300 якщо не вказано: Tesseract оптимізований під 300 DPI,
    без підказки він намагається вгадати DPI з метаданих (часто 72/96)
    і масштабує неправильно.
    """
    try:
        import pytesseract
        cmd = _find_tesseract()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd
        # DPI hint: Tesseract очікує ~300 DPI для оптимального розпізнавання
        if '--dpi' not in config:
            config = f"--dpi 300 {config}".strip()
        return pytesseract.image_to_string(img, config=config)
    except Exception as e:
        logger.debug("Tesseract OCR помилка: %s", e)
        return ""


# ── Обробка зображення ──────────────────────────────────────────────────

def _prepare_image(image_bytes: bytes, max_px: int = 1200):
    """Відкриває, масштабує до max_px, конвертує в RGB.

    max_px=1200: компроміс між якістю OCR і швидкістю.
    - 1000px: швидко, але дрібний текст (EXP, ISS) іноді не читається
    - 2000px: точніше, але CLAHE + Tesseract занадто повільні (~30с timeout)
    - 1200px: достатня якість без timeout'ів
    """
    from PIL import Image
    img = Image.open(io.BytesIO(image_bytes)).convert('RGB')
    w, h = img.size
    if max(w, h) > max_px:
        ratio = max_px / max(w, h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
    elif max(w, h) < 800:
        # Занадто маленьке фото — збільшуємо для кращого OCR
        ratio = 800 / max(w, h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
    return img


def _binarize(img):
    """Конвертує в ч/б з порогом для кращого OCR MRZ."""
    from PIL import Image
    gray = img.convert('L')
    return gray.point(lambda x: 255 if x > 140 else 0, '1')


def _pil_to_cv2(img) -> np.ndarray:
    """PIL Image → OpenCV numpy array (BGR)."""
    return np.array(img)[:, :, ::-1].copy() if img.mode == 'RGB' else np.array(img)


def _cv2_to_pil(arr: np.ndarray):
    """OpenCV numpy array → PIL Image."""
    from PIL import Image
    if len(arr.shape) == 2:
        return Image.fromarray(arr, 'L')
    return Image.fromarray(arr[:, :, ::-1], 'RGB')


def _apply_clahe(img) -> 'PIL.Image':
    """CLAHE — Contrast Limited Adaptive Histogram Equalization.

    Вирівнює контраст ЛОКАЛЬНО: якщо частина документа в тіні або
    з відблиском — CLAHE підтягне контраст саме в тій зоні.
    Результат: текст стає чіткішим навіть при нерівномірному освітленні.
    """
    try:
        import cv2
        gray = np.array(img.convert('L'))
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)
        from PIL import Image
        return Image.fromarray(enhanced, 'L').convert('RGB')
    except ImportError:
        # Fallback без OpenCV: простий contrast stretch через PIL
        from PIL import ImageEnhance
        return ImageEnhance.Contrast(img).enhance(1.5)


def _sharpen(img) -> 'PIL.Image':
    """Підвищує різкість — допомагає з розмитими фото документів."""
    from PIL import ImageFilter
    return img.filter(ImageFilter.SHARPEN)


def _adaptive_threshold(img) -> 'PIL.Image':
    """Адаптивна бінаризація — краще за глобальний поріг 140.

    Глобальний поріг ламається коли:
    - Частина фото темна (тінь) → текст зникає
    - Частина фото світла (відблиск) → фон стає текстом

    Адаптивний поріг рахує поріг для кожного блоку 15×15 пікселів окремо.
    """
    try:
        import cv2
        gray = np.array(img.convert('L'))
        # Gaussian adaptive: плавніший, менше шуму ніж Mean
        binary = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 15, 8
        )
        from PIL import Image
        return Image.fromarray(binary, 'L').convert('RGB')
    except ImportError:
        # Fallback: глобальний Otsu-подібний через PIL
        gray = img.convert('L')
        # Автоматичний поріг: середнє значення пікселів
        hist = gray.histogram()
        total = sum(hist)
        running = 0
        threshold = 128
        for i, count in enumerate(hist):
            running += count
            if running > total * 0.5:
                threshold = i
                break
        return gray.point(lambda x: 255 if x > threshold else 0, '1').convert('RGB')


def _denoise(img) -> 'PIL.Image':
    """Видаляє шум — дрібні артефакти від стиснення JPEG, текстура фону."""
    try:
        import cv2
        arr = np.array(img.convert('L'))
        # Bilateral filter: зберігає краї (текст) але прибирає шум
        denoised = cv2.bilateralFilter(arr, 9, 75, 75)
        from PIL import Image
        return Image.fromarray(denoised, 'L').convert('RGB')
    except ImportError:
        # Без OpenCV: легкий blur через PIL (менш ефективний)
        from PIL import ImageFilter
        return img.filter(ImageFilter.MedianFilter(size=3))


def _apply_sauvola(img) -> 'PIL.Image':
    """Sauvola бінаризація — адаптивний поріг враховує локальну дисперсію.

    Краще за CLAHE + adaptive threshold для документів з:
    - тінями від пальців/згинів
    - відблисками від ламінації
    - нерівномірним освітленням (частина світла, частина темна)

    Sauvola: T(x,y) = mean(x,y) * [1 + k * (std(x,y)/R - 1)]
    де R=128, k=0.2 — стандартні параметри для друкованого тексту.
    """
    try:
        from skimage.filters import threshold_sauvola
        from PIL import Image
        gray = np.array(img.convert('L'))
        thresh = threshold_sauvola(gray, window_size=25, k=0.2)
        binary = ((gray > thresh) * 255).astype(np.uint8)
        return Image.fromarray(binary, 'L').convert('RGB')
    except ImportError:
        # Fallback на adaptive threshold якщо skimage не встановлено
        return _adaptive_threshold(img)


def _deskew(img) -> 'PIL.Image':
    """Виправляє нахил зображення (deskew).

    Якщо документ сфотографовано під кутом — текст нахилений,
    OCR плутає символи. Виправлення нахилу на 1-15° різко підвищує точність.
    """
    try:
        import cv2
        from PIL import Image
        gray = np.array(img.convert('L'))

        # Визначаємо кут нахилу через Hough Line Transform
        edges = cv2.Canny(gray, 50, 150, apertureSize=3)
        lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=100,
                                minLineLength=gray.shape[1] // 4, maxLineGap=10)
        if lines is None or len(lines) == 0:
            return img

        # Медіана кутів ліній
        angles = []
        for line in lines:
            x1, y1, x2, y2 = line[0]
            angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
            if abs(angle) < 15:  # ігноруємо вертикальні лінії
                angles.append(angle)

        if not angles:
            return img

        median_angle = float(np.median(angles))

        # Не виправляємо якщо нахил < 0.5° (шум)
        if abs(median_angle) < 0.5:
            return img

        _diag(f"      [Deskew] angle={median_angle:.1f}°")

        # Повертаємо зображення
        h, w = gray.shape
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, median_angle, 1.0)
        rotated = cv2.warpAffine(np.array(img), M, (w, h),
                                 flags=cv2.INTER_CUBIC,
                                 borderMode=cv2.BORDER_REPLICATE)
        return Image.fromarray(rotated)
    except Exception:
        return img


# ── PaddleOCR singleton ────────────────────────────────────────────────
_paddle_ocr_instance = None
_paddle_lock = threading.Lock()


def _get_paddle_ocr():
    """Lazy init RapidOCR (PaddleOCR моделі через ONNX Runtime) — один раз на весь процес."""
    global _paddle_ocr_instance
    if _paddle_ocr_instance is not None:
        return _paddle_ocr_instance
    with _paddle_lock:
        if _paddle_ocr_instance is not None:
            return _paddle_ocr_instance
        try:
            from rapidocr_onnxruntime import RapidOCR
            _paddle_ocr_instance = RapidOCR()
            _diag("  [PaddleOCR] initialized OK (RapidOCR/ONNX)")
            return _paddle_ocr_instance
        except Exception as e:
            _diag(f"  [PaddleOCR] init FAILED: {e}")
            return None


def _paddle_ocr_text(img) -> str:
    """Запускає RapidOCR на PIL Image, повертає повний текст."""
    engine = _get_paddle_ocr()
    if engine is None:
        return ""
    try:
        arr = np.array(img)
        if len(arr.shape) == 2:
            arr = np.stack([arr] * 3, axis=-1)
        elif arr.shape[2] == 4:
            arr = arr[:, :, :3]

        result, _ = engine(arr)
        if not result:
            return ""
        # RapidOCR повертає [(box, text, conf), ...]
        lines = [text for _, text, _ in result]
        return '\n'.join(lines)
    except Exception as e:
        _diag(f"    [PaddleOCR] error: {e}")
        return ""


def _paddle_ocr_data(img) -> list[dict]:
    """Запускає RapidOCR, повертає список слів з координатами (для spatial)."""
    engine = _get_paddle_ocr()
    if engine is None:
        return []
    try:
        arr = np.array(img)
        if len(arr.shape) == 2:
            arr = np.stack([arr] * 3, axis=-1)
        elif arr.shape[2] == 4:
            arr = arr[:, :, :3]

        result, _ = engine(arr)
        if not result:
            return []

        words = []
        for box, text, conf in result:
            if not text.strip() or conf < 0.1:
                continue
            # box: [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]
            xs = [p[0] for p in box]
            ys = [p[1] for p in box]
            left = int(min(xs))
            top = int(min(ys))
            w = int(max(xs) - left)
            h = int(max(ys) - top)
            words.append({
                'text': text.strip(),
                'left': left, 'top': top, 'w': w, 'h': h,
                'conf': int(conf * 100),
            })
        return words
    except Exception as e:
        _diag(f"    [PaddleOCR] data error: {e}")
        return []


def _preprocess_variants(img) -> list[tuple['PIL.Image', str]]:
    """Генерує кілька варіантів обробки зображення.

    Кожен варіант оптимізований під різні умови фото:
    - Original: чисті скани, вже хороша якість
    - CLAHE+Sharpen: фото з тінями/відблисками, розмиті
    - Adaptive threshold: дуже низький контраст, кольоровий фон
    - Denoise+CLAHE: JPEG артефакти, зернисті фото

    Для кожного варіанту запускається OCR, і обирається найкращий результат.
    """
    variants = [
        (img, "original"),
    ]

    # Варіант 2: CLAHE + Sharpen (найефективніший для більшості фото)
    try:
        v2 = _sharpen(_apply_clahe(img))
        variants.append((v2, "clahe+sharp"))
    except Exception:
        pass

    # Варіант 3: Adaptive threshold (для дуже поганого контрасту)
    try:
        v3 = _adaptive_threshold(img)
        variants.append((v3, "adaptive_thresh"))
    except Exception:
        pass

    # Варіант 4: Denoise + CLAHE (для зернистих/стиснених фото)
    try:
        v4 = _apply_clahe(_denoise(img))
        variants.append((v4, "denoise+clahe"))
    except Exception:
        pass

    return variants


# ── MRZ парсинг ─────────────────────────────────────────────────────────

def _mrz_date_to_iso(yymmdd: str) -> Optional[str]:
    """YYMMDD → YYYY-MM-DD."""
    if len(yymmdd) != 6 or not yymmdd.isdigit():
        return None
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    year = 2000 + yy if yy <= 30 else 1900 + yy
    try:
        return date(year, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract_expiry_from_mrz(text: str) -> Optional[str]:
    """Витягує expiry date з MRZ-тексту.
    TD3 line2[21:27], TD1 line2[8:14]."""
    text = text.upper()
    # Часті OCR-помилки в MRZ
    for old, new in {
        'О': 'O', 'С': 'C', 'В': 'B', 'Н': 'H',
        '{': '<', '[': '<', '(': '<', '|': '<',
        ' ': '',
    }.items():
        text = text.replace(old, new)

    mrz_44: list[str] = []
    mrz_30: list[str] = []

    for line in text.split('\n'):
        cleaned = ''.join(c for c in line if c.isalnum() or c == '<')
        if not cleaned:
            continue
        if 42 <= len(cleaned) <= 46:
            mrz_44.append((cleaned + '<' * 44)[:44])
        elif 28 <= len(cleaned) <= 32:
            mrz_30.append((cleaned + '<' * 30)[:30])

    # TD3 (паспорт): expiry at line2[21:27]
    if len(mrz_44) >= 2:
        iso = _mrz_date_to_iso(mrz_44[1][21:27])
        if iso:
            return iso

    # TD1 (ID карта): expiry at line2[8:14]
    if len(mrz_30) >= 3:
        iso = _mrz_date_to_iso(mrz_30[1][8:14])
        if iso:
            return iso

    return None


# ── Визначення країни/штату для формату дати ─────────────────────────────

# US штати → формат MM/DD/YYYY
_US_STATES = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
    'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming',
    'district of columbia', 'puerto rico', 'guam',
}

# Ключові слова US-документів
_US_KEYWORDS = {'driver license', 'driver\'s license', 'identification card',
                'usa', 'united states'}

# US штати — абревіатури (OCR часто не розпізнає повну назву)
_US_STATE_ABBREVS = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
}

# Regex для типових US-адрес: "CITY, ST 12345"
_US_ADDRESS_RE = re.compile(
    r'[A-Z]{2,},?\s+(' + '|'.join(_US_STATE_ABBREVS) + r')\s+\d{5}',
    re.I
)


def _normalize_text(text: str) -> str:
    """Нормалізує текст: curly apostrophes → ASCII, зайві пробіли тощо."""
    # OCR часто повертає curly quotes замість ASCII
    text = text.replace('\u2018', "'").replace('\u2019', "'")   # ' ' → '
    text = text.replace('\u201C', '"').replace('\u201D', '"')   # " " → "
    text = text.replace('\u00B4', "'").replace('\u0060', "'")   # ´ ` → '
    return text


def _detect_date_format(text: str) -> str:
    """Визначає формат дати за текстом документа.

    Returns:
        'us' (MM/DD/YYYY) або 'eu' (DD/MM/YYYY)
    """
    low = _normalize_text(text).lower()

    # Шукаємо назви US-штатів (повні)
    for state in _US_STATES:
        if state in low:
            return 'us'

    # US-ключові слова
    for kw in _US_KEYWORDS:
        if kw in low:
            return 'us'

    # US-адреса з абревіатурою штату: "PARIS, KY 40361"
    if _US_ADDRESS_RE.search(text):
        return 'us'

    # Додаткові ознаки US: "driver" + "license/licence" будь-де в тексті
    if 'driver' in low and ('license' in low or 'licence' in low):
        return 'us'

    # US DL class types: "CLASS C", "CLASS D" (OCR часто: "ctass", "c1ass")
    if re.search(r'\b[c-d][l1!|]ass\s+[a-d]\b', low):
        return 'us'
    if re.search(r'\bclass\s+[a-d]\b', low):
        return 'us'

    # Слова-ознаки US-документів (достатньо 1 збігу)
    us_strong_hints = ['endorsement', 'not for federal', 'real id',
                       'not for f', 'federal id']
    if any(h in low for h in us_strong_hints):
        return 'us'

    # Слабші ознаки — потрібно 2+ збіги
    us_hints = ['restrictions', 'restr', 'end none', 'res none',
                'veteran', 'donor', 'hazmat', 'wgt', 'hgt']
    if sum(1 for h in us_hints if h in low) >= 2:
        return 'us'

    # За замовчуванням US формат: більшість документів у системі — US DL.
    # Якщо документ EU — зазвичай є MRZ або специфічні EU-keywords.
    # Краще помилитися в бік US (MM/DD) ніж EU (DD/MM): US DL дат набагато більше.
    return 'us'


# ── Пошук дати за ключовими словами ─────────────────────────────────────

_DATE_PATTERNS = [
    re.compile(r'\b(\d{2})[./\-](\d{2})[./\-](\d{4})\b'),       # DD.MM.YYYY (4-digit year)
    re.compile(r'\b(\d{4})[.\-/](\d{2})[.\-/](\d{2})\b'),       # YYYY-MM-DD
    re.compile(
        r'\b(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{4})\b',
        re.I
    ),
    re.compile(r'\b(\d{2})[./\-](\d{2})[./\-](\d{2})\b'),       # DD.MM.YY (2-digit year!)
]

_EXPIRY_KEYWORDS = [
    'exp', 'expiry', 'expires', 'expiration', 'valid until', 'date of expiry',
    'exe',  # OCR часто плутає p→e: "exp" → "exe"
    'gültig bis', 'gueltig bis', 'ablaufdatum',
    "date d'expiration", 'expire le',
    'geldig tot', 'data di scadenza',
    'fecha de caducidad', 'vencimiento',
    'validade', 'data de validade',
    'platnost', 'datum expirace',
    '4b', 'érvényes', 'lejárat',
    'effective',  # Australian DL: "Effective ... Expiry"
]

# Ключові слова DOB/ISSUE — дати поруч з ними НЕ є expiry
_DOB_KEYWORDS = [
    'dob', 'date of birth', 'born', 'birthday', 'birth date',
    'geburtsdatum', 'date de naissance', 'fecha de nacimiento',
    'data di nascita', 'geboortedatum', 'datum narození',
    'születési', 'data de nascimento',
    'dos',  # Australian DL: "DOB" часто OCR-ується як "Dos"
    'age 21',  # Washington DL: "AGE 21 ON mm/dd/yyyy" — дата 21-річчя, НЕ expiry
    'age21',   # OCR без пробілу
    'under 21',  # Інші US: "UNDER 21 UNTIL mm/dd/yyyy"
]

_ISSUE_KEYWORDS = [
    'iss', 'issued', 'issue date', 'date of issue',
    'rev', 'revision',
    '4aiss', '4a iss', '4a1ss',  # OCR варіації "4aISS"
    'woss',  # OCR помилка: "ISS" → "woss"
    'ausstellungsdatum', "date de délivrance", "date d'émission",
    'fecha de emisión', 'data di rilascio',
    '4a',  # ICAO field 4a = issue date
    'ssue',  # OCR часто розбиває "Issue" → "C)ssue.no", "1ssue" тощо
    'end none',  # Texas back: "END: NONE" поруч з DOB
    'elss', 'eiss', 'alss', 'aiss',  # OCR garbled "ISS": "aelSS", "aeISS"
    'lss',  # OCR: "ISS" → "lSS"
]

_MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
}


def _yy_to_yyyy(yy: int) -> int:
    """2-значний рік → 4-значний. 00-50 → 2000-2050, 51-99 → 1951-1999."""
    return 2000 + yy if yy <= 50 else 1900 + yy


def _parse_date(match: re.Match, pat_idx: int, fmt: str = 'eu') -> Optional[str]:
    """regex match → YYYY-MM-DD.

    fmt='us' → MM/DD/YYYY, fmt='eu' → DD/MM/YYYY.
    Якщо одне число >12 — формат визначається автоматично незалежно від fmt.

    pat_idx: 0=DD.MM.YYYY, 1=YYYY-MM-DD, 2=DD MON YYYY, 3=DD.MM.YY (2-digit year)
    """
    try:
        g = match.groups()
        if pat_idx == 0:                     # DD.MM.YYYY (4-digit year)
            a, b, yyyy = int(g[0]), int(g[1]), int(g[2])
            if a > 12 and 1 <= b <= 12:
                dd, mm = a, b
            elif b > 12 and 1 <= a <= 12:
                mm, dd = a, b
            elif fmt == 'us':
                mm, dd = a, b
            else:
                dd, mm = a, b
        elif pat_idx == 1:                   # YYYY-MM-DD
            yyyy, mm, dd = int(g[0]), int(g[1]), int(g[2])
        elif pat_idx == 2:                   # DD MON YYYY
            dd, mm, yyyy = int(g[0]), _MONTH_MAP.get(g[1].upper(), 0), int(g[2])
        elif pat_idx == 3:                   # DD.MM.YY (2-digit year!)
            a, b, yy = int(g[0]), int(g[1]), int(g[2])
            yyyy = _yy_to_yyyy(yy)
            if a > 12 and 1 <= b <= 12:
                dd, mm = a, b
            elif b > 12 and 1 <= a <= 12:
                mm, dd = a, b
            elif fmt == 'us':
                mm, dd = a, b
            else:
                dd, mm = a, b
        else:
            return None
        if not (1 <= mm <= 12 and 1 <= dd <= 31 and 1950 <= yyyy <= 2036):
            return None
        d = date(yyyy, mm, dd)
        iso = d.strftime("%Y-%m-%d")
        # Фільтр: дати в межах ±1 дня від сьогодні — підозрілий OCR-артефакт
        # (Tesseract іноді "читає" дату з метаданих EXIF або шуму)
        from datetime import timedelta
        today = date.today()
        if abs((d - today).days) <= 1:
            return None
        return iso
    except (ValueError, IndexError):
        return None


def _find_expiry_in_text(text: str) -> Optional[tuple[str, bool]]:
    """Шукає expiry date в тексті за ключовими словами.
    Автоматично визначає US/EU формат дати за назвою штату/країни.

    Returns:
        (iso_date, has_keyword) або None.
        has_keyword=True  → дата знайдена поруч із EXP-keyword (надійна)
        has_keyword=False → дата знайдена як fallback (тільки майбутні!)

    Логіка:
      1. Збирає всі дати з тексту
      2. Визначає рядки з expiry-keywords та DOB-keywords
      3. Виключає дати поруч із DOB-keywords
      4. Повертає дату поруч із expiry-keyword (пріоритет)
      5. Fallback: найпізніша дата в майбутньому (тільки після 2020)
    """
    fmt = _detect_date_format(text)
    lines = text.split('\n')

    # Збираємо ВСІ дати з тексту
    all_dates: list[tuple[str, int]] = []
    for li, line in enumerate(lines):
        for pi, pat in enumerate(_DATE_PATTERNS):
            for m in pat.finditer(line):
                iso = _parse_date(m, pi, fmt)
                if iso:
                    all_dates.append((iso, li))

    if not all_dates:
        _diag(f"      [_find_expiry] no dates parsed from text")
        return None

    _diag(f"      [_find_expiry] format={fmt}, all_dates={all_dates}")

    # Рядки з EXPIRY-keywords
    expiry_lines: set[int] = set()
    for li, line in enumerate(lines):
        low = line.lower()
        if any(kw in low for kw in _EXPIRY_KEYWORDS):
            expiry_lines.add(li)

    # Рядки з DOB-keywords (дати поруч — це дата народження, НЕ expiry)
    dob_lines: set[int] = set()
    for li, line in enumerate(lines):
        low = line.lower()
        if any(kw in low for kw in _DOB_KEYWORDS):
            dob_lines.add(li)

    # Рядки з ISSUE-keywords (дати поруч — це дата видачі, НЕ expiry)
    issue_lines: set[int] = set()
    # Рядки де ISS і EXP разом (OCR зліпив "ISS ... EXP ..." в один рядок)
    mixed_iss_exp_lines: set[int] = set()
    for li, line in enumerate(lines):
        low = line.lower()
        has_iss = any(kw in low for kw in _ISSUE_KEYWORDS)
        has_exp = any(kw in low for kw in _EXPIRY_KEYWORDS)
        if has_iss:
            if not has_exp:
                issue_lines.add(li)
            else:
                # Рядок має і ISS і EXP — мішаний рядок
                mixed_iss_exp_lines.add(li)

    if expiry_lines or issue_lines or dob_lines or mixed_iss_exp_lines:
        _diag(f"      [_find_expiry] EXP_lines={expiry_lines} ISS_lines={issue_lines} "
              f"DOB_lines={dob_lines} mixed={mixed_iss_exp_lines}")
        # Логуємо вміст keyword-рядків
        for li in sorted(expiry_lines | issue_lines | dob_lines | mixed_iss_exp_lines):
            tags = []
            if li in expiry_lines: tags.append("EXP")
            if li in issue_lines: tags.append("ISS")
            if li in dob_lines: tags.append("DOB")
            if li in mixed_iss_exp_lines: tags.append("MIXED")
            _diag(f"        line {li} [{','.join(tags)}]: {lines[li][:100]}")

    # Фільтруємо: виключаємо дати поруч із DOB або ISSUE (±2 рядки)
    def _is_near_dob(line_idx: int) -> bool:
        return any(abs(line_idx - dl) <= 2 for dl in dob_lines)

    def _is_near_issue(line_idx: int) -> bool:
        return any(abs(line_idx - dl) <= 1 for dl in issue_lines)

    def _is_near_mixed(line_idx: int) -> bool:
        """Рядок поруч з мішаним ISS+EXP рядком (±1)."""
        return any(abs(line_idx - ml) <= 1 for ml in mixed_iss_exp_lines)

    # Пріоритет 0: дати НА ТОМУ Ж РЯДКУ що expiry-keyword
    today_iso = date.today().strftime("%Y-%m-%d")
    skipped_lines: set[int] = set()  # Рядки де дата підозріла → не довіряти

    for exp_li in expiry_lines:
        line_dates = [(d, li) for d, li in all_dates if li == exp_li]
        if not line_dates:
            continue
        _diag(f"      [P0] EXP line {exp_li}: dates={[d for d,_ in line_dates]}, "
              f"text={lines[exp_li][:80]}")
        # Якщо рядок має дату до 2000 — це DOB+EXP на одному рядку
        has_old_date = any(d < "2000-01-01" for d, _ in line_dates)
        if has_old_date:
            recent_on_line = [d for d, _ in line_dates if d >= "2010-01-01"]
            if recent_on_line:
                r = (max(recent_on_line), True)
                _diag(f"      [P0] old+recent dates on line → {r}")
                return r
            skipped_lines.add(exp_li)
            _diag(f"      [P0] all dates old (<2000) → SKIP line {exp_li}")
            continue
        # Звичайний випадок
        best_on_line = max(d for d, _ in line_dates)
        near_dob = _is_near_dob(exp_li)
        near_iss = _is_near_issue(exp_li)
        if not near_dob and not near_iss:
            if best_on_line > today_iso:
                _diag(f"      [P0] future date on EXP line → ({best_on_line}, True)")
                return (best_on_line, True)
            if len(line_dates) == 1 and best_on_line < "2024-01-01":
                skipped_lines.add(exp_li)
                _diag(f"      [P0] single old date ({best_on_line}) on EXP line → SKIP (probably ISS)")
                continue
            _diag(f"      [P0] past date on EXP line → ({best_on_line}, True)")
            return (best_on_line, True)
        else:
            _diag(f"      [P0] near_dob={near_dob}, near_iss={near_iss}")
        # Якщо поруч DOB/ISS — тільки майбутні
        future_on_line = [d for d, _ in line_dates if d > today_iso]
        if future_on_line:
            r = (max(future_on_line), True)
            _diag(f"      [P0] future near DOB/ISS → {r}")
            return r

    # Рядки де є дата до 2000 (однозначно DOB навіть без keyword)
    lines_with_old_dates = {li for _, li in all_dates
                            if any(d < "2000-01-01" for d, l in all_dates if l == li)}
    # Додаємо skipped рядки з пріоритету 0 та їх сусідів (±1)
    skip_all = set(lines_with_old_dates)
    for sl in skipped_lines:
        skip_all.update({sl - 1, sl, sl + 1})

    # Пріоритет 1a: МАЙБУТНІ дати поруч із expiry-keywords (±2 рядки), НЕ DOB/ISS
    candidates_future = [
        d for d, li in all_dates
        if d > today_iso
        and any(abs(li - el) <= 2 for el in expiry_lines)
        and not _is_near_dob(li)
        and not _is_near_issue(li)
        and li not in skip_all
    ]
    if candidates_future:
        _diag(f"      [P1a] future near EXP: {candidates_future} → {max(candidates_future)}")
        return (max(candidates_future), True)
    _diag(f"      [P1a] no future dates near EXP (skip_all={skip_all})")

    # Пріоритет 1b: свіжі (>= 2010) дати поруч із expiry, НЕ DOB/ISS
    # Виключаємо дати поруч з мішаними ISS+EXP рядками (де EXP garbled)
    candidates_recent = [
        d for d, li in all_dates
        if d >= "2010-01-01"
        and any(abs(li - el) <= 2 for el in expiry_lines)
        and not _is_near_dob(li)
        and not _is_near_issue(li)
        and not _is_near_mixed(li)
        and li not in skip_all
    ]
    if candidates_recent:
        _diag(f"      [P1b] recent near EXP: {candidates_recent} → {max(candidates_recent)}")
        return (max(candidates_recent), True)
    _diag(f"      [P1b] no recent dates near EXP")

    # Пріоритет 2: дати поруч із expiry-keywords, тільки МАЙБУТНІ
    candidates = [
        d for d, li in all_dates
        if d > today_iso
        and any(abs(li - el) <= 2 for el in expiry_lines)
    ]
    if candidates:
        _diag(f"      [P2] future near EXP (any): {candidates} → {max(candidates)}")
        return (max(candidates), True)

    # Fallback: найпізніша дата в МАЙБУТНЬОМУ, не DOB, не ISSUE
    today_iso = date.today().strftime("%Y-%m-%d")
    future = [d for d, li in all_dates
              if d > today_iso and d >= "2020-01-01"
              and not _is_near_dob(li) and not _is_near_issue(li)]
    if future:
        _diag(f"      [Fallback] future date without keyword: {future} → ({max(future)}, False)")
        return (max(future), False)

    _diag(f"      [_find_expiry] no valid date found → None")
    return None


# ── Просторовий пошук (Textract-стиль) ────────────────────────────────

# Якорі для просторового пошуку: слова що вказують на expiry
_SPATIAL_EXPIRY_ANCHORS = {
    'exp', 'expiry', 'expires', 'expiration', 'expire',
    '4b', 'ablauf', 'geldig', 'valid',
}
_SPATIAL_DOB_ANCHORS = {
    'dob', 'birth', 'born', 'dos', 'née', 'geb', 'geburt',
    'nascimento', 'nacimiento', 'naissance', 'születés',
    'age',  # Washington DL: "AGE 21 ON" — дата 21-річчя
}
_SPATIAL_ISSUE_ANCHORS = {
    'iss', 'issued', 'issue', 'rev', 'revision',
    '4a', 'délivrance', 'rilascio', 'emisión',
}

# Regex для дати серед окремих слів: "08/25/2026" або "28.08.26"
_DATE_WORD_RE = re.compile(r'(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})')


def _spatial_find_expiry(img) -> Optional[str]:
    """Знаходить expiry date через просторовий аналіз bounding boxes.

    Принцип (як у Textract):
      1. image_to_data() → координати кожного слова
      2. Знайти якір ("EXP", "EXPIRY") → його bounding box
      3. Шукати дату СПРАВА або ЗНИЗУ від якоря
      4. Ігнорувати дати поруч із DOB/ISS якорями

    Перевага над рядковим аналізом: працює коли дата на іншому
    рядку OCR або в іншій колонці таблиці.
    """
    try:
        import pytesseract
        cmd = _find_tesseract()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd

        # PSM 11 = sparse text — найкращий для ID-карток з розкиданими полями
        # --dpi 300: Tesseract оптимізований під 300 DPI
        data = pytesseract.image_to_data(img, config="--psm 11 --dpi 300", output_type=pytesseract.Output.DICT)
    except Exception as e:
        logger.debug("image_to_data помилка: %s", e)
        return None

    n = len(data['text'])
    if n == 0:
        return None

    # Визначаємо формат дати за повним текстом
    full_text = ' '.join(t for t in data['text'] if t.strip())
    fmt = _detect_date_format(full_text)

    # ── Крок 1: Знайти всі слова з координатами ──
    words: list[dict] = []
    for i in range(n):
        txt = data['text'][i].strip()
        conf = int(data['conf'][i]) if str(data['conf'][i]).lstrip('-').isdigit() else 0
        if not txt or conf < 10:
            continue
        words.append({
            'text': txt,
            'left': data['left'][i],
            'top': data['top'][i],
            'w': data['width'][i],
            'h': data['height'][i],
            'conf': conf,
        })

    if not words:
        return None

    # Середня висота слова — для визначення "тої ж лінії"
    avg_h = max(1, sum(w['h'] for w in words) // len(words))

    # ── Крок 2: Знайти якорі та дати ──
    expiry_anchors: list[dict] = []
    dob_anchors: list[dict] = []
    issue_anchors: list[dict] = []
    date_words: list[tuple[dict, str]] = []   # (word_info, iso_date)

    for w in words:
        low = w['text'].lower().rstrip(':.,;')

        # Класифікуємо слово (fuzzy: перевіряємо і підрядки)
        # OCR часто зліплює: "4a.tss:" → "4aiss", "4b.Exp" → "4bexp"
        if low in _SPATIAL_EXPIRY_ANCHORS or any(a in low for a in ('exp', 'expir')):
            expiry_anchors.append(w)
        if low in _SPATIAL_DOB_ANCHORS or any(a in low for a in ('dob', 'birth', 'born', 'age')):
            dob_anchors.append(w)
        if low in _SPATIAL_ISSUE_ANCHORS or any(a in low for a in ('iss', 'issue', 'tss')):
            # "tss" = OCR garbled "ISS"
            issue_anchors.append(w)

        # Перевіряємо чи слово — дата
        # Спочатку виправляємо типові OCR-помилки цифр
        fixed_text = w['text']
        for old_ch, new_ch in [('O', '0'), ('o', '0'), ('I', '1'), ('l', '1'),
                               ('N', '1'), ('S', '5'), ('B', '8'), ('G', '6'),
                               ('Z', '2'), ('T', '7')]:
            # Заміняємо ТІЛЬКИ якщо символ оточений цифрами або роздільниками
            pass  # Складна евристика — простіше: пробуємо обидва варіанти

        # Пробуємо оригінал
        m = _DATE_WORD_RE.search(w['text'])
        if not m:
            # Пробуємо з виправленням літер → цифри
            cleaned = w['text']
            for old_c, new_c in [('O', '0'), ('o', '0'), ('I', '1'), ('l', '1'),
                                 ('N', '1'), ('S', '5'), ('B', '8')]:
                cleaned = cleaned.replace(old_c, new_c)
            m = _DATE_WORD_RE.search(cleaned)
        if m:
            # Визначаємо pat_idx: 4 цифри в кінці = pat 0, 2 цифри = pat 3
            g3 = m.group(3)
            pat_idx = 0 if len(g3) == 4 else 3
            iso = _parse_date(m, pat_idx, fmt)
            if iso:
                date_words.append((w, iso))

    # Діагностика spatial
    if expiry_anchors or issue_anchors or dob_anchors:
        _diag(f"      [Spatial] anchors: EXP={[w['text'] for w in expiry_anchors]}, "
              f"ISS={[w['text'] for w in issue_anchors]}, DOB={[w['text'] for w in dob_anchors]}")
    if date_words:
        _diag(f"      [Spatial] dates: {[(w['text'], iso) for w, iso in date_words]}")
    else:
        _diag(f"      [Spatial] no date words found")

    if not date_words:
        return None

    # ── Крок 3: Просторове зіставлення ──
    def _distance_right_or_below(anchor: dict, target: dict) -> float:
        """Відстань від якоря до цілі, якщо ціль СПРАВА або ЗНИЗУ.
        Повертає float('inf') якщо ціль в неправильному напрямку."""
        ax_right = anchor['left'] + anchor['w']
        ay_center = anchor['top'] + anchor['h'] // 2
        tx_left = target['left']
        ty_center = target['top'] + target['h'] // 2

        # СПРАВА: ціль правіше якоря, на тій же лінії (±1.5 висоти)
        if tx_left >= ax_right - 10 and abs(ty_center - ay_center) < avg_h * 1.5:
            return tx_left - ax_right

        # ЗНИЗУ: ціль нижче якоря, в тій же колонці (±3 ширини якоря)
        a_col_center = anchor['left'] + anchor['w'] // 2
        t_col_center = target['left'] + target['w'] // 2
        if target['top'] > anchor['top'] and abs(a_col_center - t_col_center) < anchor['w'] * 3:
            return (target['top'] - anchor['top']) + abs(a_col_center - t_col_center) * 0.5

        return float('inf')

    def _is_near_anchor(target: dict, anchors: list[dict], max_dist: float = 300) -> bool:
        """Чи є ціль поруч з будь-яким якорем?"""
        for a in anchors:
            if _distance_right_or_below(a, target) < max_dist:
                return True
        return False

    # Пріоритет 1: дати поруч із expiry-якорем, НЕ поруч із DOB/ISS
    best_date = None
    best_dist = float('inf')
    for dw, iso in date_words:
        if _is_near_anchor(dw, dob_anchors, 200):
            continue
        if _is_near_anchor(dw, issue_anchors, 200):
            continue
        for anchor in expiry_anchors:
            dist = _distance_right_or_below(anchor, dw)
            if dist < best_dist:
                best_dist = dist
                best_date = iso

    if best_date and best_dist < 400:
        today_iso = date.today().strftime("%Y-%m-%d")
        if best_date <= today_iso:
            _diag(f"      [Spatial] best={best_date} dist={best_dist:.0f} but PAST → None")
            return None
        _diag(f"      [Spatial] ✅ best={best_date} dist={best_dist:.0f} → return")
        return best_date

    # Якщо не знайшли поруч з EXP — не повертаємо сумнівну дату.
    # Пріоритет 2 прибрано: він повертав ISS/DOB дати коли обидва
    # якоря (EXP і ISS) були поруч з одною датою.
    return None


def warmup_paddle_ocr() -> None:
    """Прогрів PaddleOCR — викликати при старті бота.

    PaddleOCR завантажує моделі (~10-15 сек) при першому виклику.
    Робимо це заздалегідь щоб перший документ не чекав.
    """
    try:
        _get_paddle_ocr()
        logger.info("PaddleOCR прогрітий і готовий")
    except Exception as e:
        logger.warning("PaddleOCR warmup failed: %s", e)


# ── Головна функція ─────────────────────────────────────────────────────

def _vote_dates(dates: list[tuple[str, str]]) -> Optional[str]:
    """Голосування по знайдених датах від різних OCR движків.

    dates: [(date_iso, source_label), ...]
    Якщо 2+ движки згідні — бере їх дату.
    Інакше — бере Tesseract (перший), якщо є.
    """
    if not dates:
        return None
    if len(dates) == 1:
        return dates[0][0]

    # Рахуємо голоси
    votes: dict[str, int] = {}
    for d, _ in dates:
        votes[d] = votes.get(d, 0) + 1

    # Якщо є дата з 2+ голосами — бере її
    best = max(votes.items(), key=lambda x: x[1])
    if best[1] >= 2:
        _diag(f"    [Vote] consensus: {best[0]} ({best[1]} votes)")
        return best[0]

    # Немає консенсусу — перевіряємо чи дати мають однаковий місяць-день
    # (типова OCR помилка: рік відрізняється на 1-4 цифри)
    # В цьому випадку обираємо дату ближчу до сьогодні (реалістичнішу)
    from datetime import date as _dt_date
    today = _dt_date.today()
    all_dates_str = [d for d, _ in dates]
    mds = [d[5:] for d in all_dates_str]  # MM-DD частини
    if len(set(mds)) == 1:
        # Однаковий місяць-день, різний рік → OCR помилка в році
        # Обираємо дату ближчу до сьогодні (± мінімальна відстань)
        closest = min(dates, key=lambda x: abs((_dt_date.fromisoformat(x[0]) - today).days))
        _diag(f"    [Vote] no consensus (same M-D, diff year): {dates}, closest → {closest[0]}")
        return closest[0]

    # Різні дати — бере першу (Tesseract як більш надійний)
    _diag(f"    [Vote] no consensus: {dates}, using first")
    return dates[0][0]


def _date_crop_reocr(img, approx_date: str) -> Optional[str]:
    """Кропає зону дати і перезапускає OCR з psm 7 для точнішого розпізнавання.

    Якщо Tesseract вже знайшов приблизну дату через spatial analysis,
    кропаємо саме ту зону з padding і перечитуємо з --psm 7 (single line)
    + Sauvola бінаризація для максимальної точності на цифрах.
    """
    try:
        import pytesseract
        cmd = _find_tesseract()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd

        data = pytesseract.image_to_data(img, config="--psm 11 --dpi 300",
                                          output_type=pytesseract.Output.DICT)
        n = len(data['text'])
        date_re = re.compile(r'\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}')

        for i in range(n):
            txt = data['text'][i].strip()
            if not txt or not date_re.search(txt):
                continue

            # Знайшли слово з датою — кропаємо з padding
            x, y = data['left'][i], data['top'][i]
            w_box, h_box = data['width'][i], data['height'][i]
            pad = max(h_box, 10)
            img_w, img_h = img.size
            crop = img.crop((
                max(0, x - pad),
                max(0, y - pad // 2),
                min(img_w, x + w_box + pad),
                min(img_h, y + h_box + pad // 2),
            ))

            # Sauvola бінаризація на кропі
            crop_bin = _apply_sauvola(crop)

            # Перечитуємо з psm 7 (single text line) — точніше для дат
            re_text = pytesseract.image_to_string(
                crop_bin,
                config="--psm 7 --dpi 300 -c tessedit_char_whitelist=0123456789/.-"
            ).strip()

            if re_text:
                m = _DATE_WORD_RE.search(re_text)
                if m:
                    fmt = _detect_date_format(re_text)
                    g3 = m.group(3)
                    pat_idx = 0 if len(g3) == 4 else 3
                    iso = _parse_date(m, pat_idx, fmt)
                    if iso:
                        _diag(f"    [DateCrop] re-OCR: '{re_text}' → {iso}")
                        return iso
    except Exception as e:
        _diag(f"    [DateCrop] error: {e}")
    return None


def _try_ocr_on_image(img) -> Optional[str]:
    """Пробує знайти expiry date на одному зображенні (без повороту).

    Стратегія (з Ensemble voting):
      0. Deskew — виправляємо нахил
      1. MRZ-зона (нижні 25% + верхні 25%) → якщо є MRZ → СТОП
      2. Tesseract spatial (image_to_data) → шукаємо дату поруч з EXP
      3. Tesseract text (image_to_string) → шукаємо за ключовими словами
      4. PaddleOCR text → теж шукаємо → VOTING з Tesseract якщо обидва знайшли
      5. Sauvola + Tesseract fallback
      6. Date crop re-OCR (якщо знайшли приблизну дату — перечитуємо точніше)

    Повертає exp_date_iso або None.
    """
    w, h = img.size
    today_iso = date.today().strftime("%Y-%m-%d")
    all_found: list[str] = []

    # ── Крок 0: Deskew (виправлення нахилу) ──
    img = _deskew(img)

    # ── Крок 1: MRZ-зона (нижні 25% + верхні 25%) ──
    mrz_config = "--psm 6 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<"
    for crop_idx, crop_box in enumerate([(0, int(h * 0.75), w, h), (0, 0, w, int(h * 0.25))]):
        zone = "bottom" if crop_idx == 0 else "top"
        try:
            mrz_crop = img.crop(crop_box)
            mrz_bin = _binarize(mrz_crop)
            mrz_text = _ocr_image(mrz_bin, config=mrz_config)
            if mrz_text:
                mrz_preview = mrz_text.replace('\n', ' | ')[:120]
                _diag(f"    [MRZ {zone}] text: {mrz_preview}")
                exp = _extract_expiry_from_mrz(mrz_text)
                if exp:
                    _diag(f"    [MRZ {zone}] ✅ FOUND: {exp} → RETURN")
                    return exp
                else:
                    _diag(f"    [MRZ {zone}] no valid date in MRZ")
        except Exception as e:
            _diag(f"    [MRZ {zone}] error: {e}")

    # ── Крок 2: Tesseract Spatial (image_to_data) ──
    tesseract_spatial_date = None
    try:
        tesseract_spatial_date = _spatial_find_expiry(img)
        if tesseract_spatial_date:
            _diag(f"    [Spatial] ✅ FOUND: {tesseract_spatial_date}")
            # FAST PATH: якщо Tesseract spatial знайшов майбутню дату — повертаємо
            if tesseract_spatial_date > today_iso:
                _diag(f"    [Spatial] future date → RETURN (skip PaddleOCR)")
                return tesseract_spatial_date
    except Exception as e:
        _diag(f"    [Spatial] error: {e}")

    # ── Крок 3: Tesseract Text (image_to_string) ──
    tesseract_text_date = None
    tesseract_text_kw = False
    try:
        full_text = _ocr_image(img, config="--psm 3")
        if full_text:
            full_text = _normalize_text(full_text)
            text_preview = full_text.replace('\n', ' | ')[:500]
            _diag(f"    [Text] OCR: {text_preview}")
            result = _find_expiry_in_text(full_text)
            if result:
                tesseract_text_date, tesseract_text_kw = result
                _diag(f"    [Text] date={tesseract_text_date}, kw={tesseract_text_kw}")
                # FAST PATH: Tesseract text знайшов майбутню дату з keyword
                if tesseract_text_date > today_iso and tesseract_text_kw:
                    _diag(f"    [Text] ✅ future + keyword → RETURN (skip PaddleOCR)")
                    return tesseract_text_date
        else:
            _diag(f"    [Text] OCR returned empty text")
    except Exception as e:
        _diag(f"    [Text] error: {e}")

    # ── Крок 4: PaddleOCR (тільки якщо Tesseract НЕ знайшов впевнену дату) ──
    # PaddleOCR допомагає коли: Tesseract нічого, або знайшов минулу дату,
    # або знайшов без keyword (невпевнено) → voting для підтвердження
    paddle_date = None
    paddle_kw = False
    try:
        paddle_text = _paddle_ocr_text(img)
        if paddle_text:
            paddle_text = _normalize_text(paddle_text)
            paddle_preview = paddle_text.replace('\n', ' | ')[:300]
            _diag(f"    [Paddle] OCR: {paddle_preview}")
            result = _find_expiry_in_text(paddle_text)
            if result:
                paddle_date, paddle_kw = result
                _diag(f"    [Paddle] date={paddle_date}, kw={paddle_kw}")
        else:
            _diag(f"    [Paddle] OCR returned empty text")
    except Exception as e:
        _diag(f"    [Paddle] error: {e}")

    # ── Voting: збираємо всі знайдені дати ──
    vote_candidates: list[tuple[str, str]] = []  # (date, source)
    if tesseract_spatial_date:
        vote_candidates.append((tesseract_spatial_date, "T-spatial"))
    if tesseract_text_date:
        vote_candidates.append((tesseract_text_date, "T-text"))
    if paddle_date:
        vote_candidates.append((paddle_date, "Paddle"))

    if vote_candidates:
        best = _vote_dates(vote_candidates)
        if best and best > today_iso:
            _diag(f"    [Ensemble] ✅ FUTURE: {best} → RETURN")
            return best
        # Зберігаємо минулі дати з keyword
        if best:
            has_any_kw = (tesseract_text_kw or paddle_kw or
                          tesseract_spatial_date is not None)
            if has_any_kw:
                _diag(f"    [Ensemble] past date with keyword: {best}")
                all_found.append(best)
            else:
                _diag(f"    [Ensemble] past date WITHOUT keyword → IGNORED")

    # ── Крок 5: Sauvola бінаризація + Tesseract fallback ──
    if not all_found:
        try:
            sauvola_img = _apply_sauvola(img)
            sauvola_text = _ocr_image(sauvola_img, config="--psm 3")
            if sauvola_text:
                sauvola_text = _normalize_text(sauvola_text)
                sauvola_preview = sauvola_text.replace('\n', ' | ')[:300]
                _diag(f"    [Sauvola] OCR: {sauvola_preview}")
                result = _find_expiry_in_text(sauvola_text)
                if result:
                    exp, has_kw = result
                    _diag(f"    [Sauvola] date={exp}, kw={has_kw}")
                    if exp > today_iso:
                        _diag(f"    [Sauvola] ✅ FUTURE date → RETURN")
                        return exp
                    # Cross-validate: якщо Sauvola знайшла минулу дату,
                    # але ensemble мав майбутню дату з тим самим місяцем-днем
                    # (Sauvola помилилась у році), → беремо ensemble дату
                    if has_kw and vote_candidates:
                        sauvola_md = exp[5:]  # MM-DD
                        for vc_date, vc_src in vote_candidates:
                            if vc_date > today_iso and vc_date[5:] == sauvola_md:
                                _diag(f"    [Sauvola] ⚠ past {exp} but ensemble had future {vc_date} (same M-D) → use ensemble")
                                return vc_date
                    if has_kw:
                        all_found.append(exp)
            else:
                _diag(f"    [Sauvola] OCR returned empty text")
        except Exception as e:
            _diag(f"    [Sauvola] error: {e}")

    # ── Крок 6: CLAHE + Sharpen fallback ──
    if not all_found:
        try:
            enhanced = _sharpen(_apply_clahe(img))
            clahe_text = _ocr_image(enhanced, config="--psm 3")
            if clahe_text:
                clahe_text = _normalize_text(clahe_text)
                clahe_preview = clahe_text.replace('\n', ' | ')[:300]
                _diag(f"    [CLAHE] OCR: {clahe_preview}")
                result = _find_expiry_in_text(clahe_text)
                if result:
                    exp, has_kw = result
                    _diag(f"    [CLAHE] date={exp}, kw={has_kw}")
                    if exp > today_iso:
                        _diag(f"    [CLAHE] ✅ FUTURE date → RETURN")
                        return exp
                    if has_kw:
                        all_found.append(exp)
            else:
                _diag(f"    [CLAHE] OCR returned empty text")
        except Exception as e:
            _diag(f"    [CLAHE] error: {e}")

    # ── Крок 7: Date Crop Re-OCR (перечитуємо зону дати точніше) ──
    # DateCrop уточнює дату тільки якщо результат близький до оригіналу
    # (той самий рік і місяць, або різниця ≤ 45 днів).
    # Якщо DateCrop видає дико іншу дату — це сміття, ігноруємо.
    if all_found:
        try:
            original = all_found[-1]
            re_date = _date_crop_reocr(img, original)
            if re_date and re_date != original:
                # Перевіряємо чи DateCrop дата "близька" до оригіналу
                from datetime import datetime
                try:
                    d_orig = datetime.strptime(original, "%Y-%m-%d")
                    d_new = datetime.strptime(re_date, "%Y-%m-%d")
                    diff_days = abs((d_new - d_orig).days)
                    if diff_days <= 45:
                        _diag(f"    [DateCrop] corrected: {original} → {re_date} (diff={diff_days}d ✅)")
                        all_found[-1] = re_date
                    else:
                        _diag(f"    [DateCrop] REJECTED: {original} → {re_date} (diff={diff_days}d ≫ 45d, keeping original)")
                except ValueError:
                    _diag(f"    [DateCrop] REJECTED: parse error for {re_date}")
        except Exception as e:
            _diag(f"    [DateCrop] error: {e}")

    # Повертаємо найкращий знайдений
    if all_found:
        best = max(all_found)
        _diag(f"    [Result] best from candidates: {best}")
        return best

    _diag(f"    [Result] no date found on this orientation")
    return None


def local_analyze(image_bytes: bytes, client_id: str = "") -> dict:
    """
    Швидкий локальний аналіз документа через Tesseract.

    Стратегія: пробуємо ВСІ орієнтації, збираємо всі знайдені дати,
    повертаємо найкращу (найпізнішу дійсну).

    Це вирішує проблему, коли 0° дає сміттєву дату і блокує 180°
    де реальна дата видна чітко.

    Returns:
        {"exp_date": "YYYY-MM-DD" | None, "doc_type": None, "country": None, "source": ...}
    """
    result = {"exp_date": None, "doc_type": None, "country": None, "source": "Local OCR"}

    _diag_separator(client_id)

    if not _tesseract_available():
        _diag("  Tesseract not available → skip")
        return result

    try:
        img = _prepare_image(image_bytes)
    except Exception as e:
        _diag(f"  Image open error: {e}")
        logger.debug("Помилка відкриття зображення: %s", e)
        return result

    _diag(f"  Image size: {img.size[0]}×{img.size[1]}")
    today_iso = date.today().strftime("%Y-%m-%d")

    # Збираємо результати з орієнтацій, з EARLY EXIT
    candidates: list[tuple[str, str]] = []   # (exp_date, source)

    orientations = [
        (img,                               "Local OCR"),
        (img.rotate(180, expand=False),     "Local OCR (180°)"),
        (img.rotate(270, expand=True),      "Local OCR (90°)"),
    ]

    for rotated_img, source_label in orientations:
        _diag(f"  --- Orientation: {source_label} ---")
        try:
            exp = _try_ocr_on_image(rotated_img)
            if exp:
                candidates.append((exp, source_label))
                _diag(f"  → candidate: {exp}")
                if exp > today_iso:
                    _diag(f"  ⚡ EARLY EXIT: future date found")
                    break
            else:
                _diag(f"  → no date on this orientation")
        except Exception as e:
            _diag(f"  → error: {e}")

    if not candidates:
        _diag(f"  FINAL: no dates found → None (will go to Textract)")
        return result

    # Вибираємо найкращу дату
    future = [(d, s) for d, s in candidates if d > today_iso]
    _diag(f"  All candidates: {candidates}")
    _diag(f"  Future candidates: {future}")

    if future:
        best = max(future, key=lambda x: x[0])
    else:
        best = max(candidates, key=lambda x: x[0])

    _diag(f"  FINAL: {best[0]} via {best[1]}")

    result["exp_date"] = best[0]
    result["source"] = best[1]
    return result


# ── API для handlers/analysis.py (/checkdoc команда) ──────────────────

def analyze_document(image_bytes: bytes, detailed: bool = False) -> dict:
    """Аналізує одне фото документа. Використовується хендлером /checkdoc."""
    result = local_analyze(image_bytes)
    return {
        "exp_date": result.get("exp_date"),
        "doc_type": result.get("doc_type"),
        "country": result.get("country"),
        "source": result.get("source"),
        "is_valid": result.get("exp_date") is not None
                    and result["exp_date"] > date.today().strftime("%Y-%m-%d"),
    }


def format_report(result: dict) -> str:
    """Форматує результат аналізу в Markdown-звіт."""
    exp = result.get("exp_date", "—")
    src = result.get("source", "—")
    valid = result.get("is_valid", False)
    status = "✅ Дійсний" if valid else "❌ Прострочений або не визначено"

    return (
        f"📋 **Результат аналізу документа**\n\n"
        f"📅 Дійсний до: `{exp}`\n"
        f"📌 Статус: {status}\n"
        f"🔍 Розпізнав: {src}\n"
    )


def check_dependencies() -> dict:
    """Перевіряє наявність OCR-бібліотек."""
    deps = {}
    for mod in ["pytesseract", "PIL", "numpy", "cv2"]:
        try:
            __import__(mod if mod != "PIL" else "PIL.Image")
            deps[mod.lower().replace("pil", "pillow")] = True
        except ImportError:
            deps[mod.lower().replace("pil", "pillow")] = False
    deps["easyocr"] = False  # Не використовується більше
    deps["passporteye"] = False  # Не використовується більше
    return deps
