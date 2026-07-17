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

# ── Ліміт потоків нативних OCR-бібліотек (МАЄ бути ДО import numpy/onnx/cv2) ──
# Кожен local_analyze CPU-важкий (Tesseract + RapidOCR/ONNX + OpenCV). Якщо
# кожен процес хапає всі ядра, то при N паралельних воркерах виникає N×cores
# потоків, що б'ються за ті самі ядра → кожне фото сповільнюється в рази
# (замір: 12с → 70с при 15 воркерах → таймаути → хибні NOT_FOUND). Робимо
# нативні бібліотеки 1-поточними; паралелізм дає семафор рівня воркерів
# (_local_ocr_semaphore в ai_sorter). Env читаються при import numpy/onnx,
# тому виставляємо їх ДО цих імпортів.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import logging
import platform
import re
import threading
import time
from datetime import date, datetime

import numpy as np

# OpenCV обмежуємо в runtime (не через env) — теж не має захоплювати всі ядра.
try:
    import cv2 as _cv2
    _cv2.setNumThreads(1)
except Exception:
    pass

logger = logging.getLogger(__name__)

# ── Діагностичний логер ────────────────────────────────────────────────
# Пише детальний лог кожного кроку аналізу у файл analysis_debug.log
# Кожен рядок має префікс [client_id] для фільтрації при паралельній обробці.

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEBUG_LOG_PATH = os.path.join(_PROJECT_ROOT, "analysis_debug.log")

_diag_logger: logging.Logger | None = None
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


def _apply_clahe(img) -> PIL.Image:
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


def _sharpen(img) -> PIL.Image:
    """Підвищує різкість — допомагає з розмитими фото документів."""
    from PIL import ImageFilter
    return img.filter(ImageFilter.SHARPEN)


def _adaptive_threshold(img) -> PIL.Image:
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


def _denoise(img) -> PIL.Image:
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


def _apply_sauvola(img) -> PIL.Image:
    """Sauvola бінаризація — адаптивний поріг враховує локальну дисперсію.

    Краще за CLAHE + adaptive threshold для документів з:
    - тінями від пальців/згинів
    - відблисками від ламінації
    - нерівномірним освітленням (частина світла, частина темна)

    Sauvola: T(x,y) = mean(x,y) * [1 + k * (std(x,y)/R - 1)]
    де R=128, k=0.2 — стандартні параметри для друкованого тексту.
    """
    try:
        from PIL import Image
        from skimage.filters import threshold_sauvola
        gray = np.array(img.convert('L'))
        thresh = threshold_sauvola(gray, window_size=25, k=0.2)
        binary = ((gray > thresh) * 255).astype(np.uint8)
        return Image.fromarray(binary, 'L').convert('RGB')
    except ImportError:
        # Fallback на adaptive threshold якщо skimage не встановлено
        return _adaptive_threshold(img)


def _order_corners(pts) -> np.ndarray:
    """Впорядковує 4 точки: top-left, top-right, bottom-right, bottom-left."""
    pts = np.array(pts, dtype=np.float32).reshape(4, 2)
    s = pts.sum(axis=1)
    d = (pts[:, 0] - pts[:, 1])
    tl = pts[np.argmin(s)]
    br = pts[np.argmax(s)]
    tr = pts[np.argmax(d)]
    bl = pts[np.argmin(d)]
    return np.array([tl, tr, br, bl], dtype=np.float32)


def _card_quads_from_mask(mask, img_area: float) -> list:
    """З бінарної маски дістає кандидат-квадрати картки (топ-3 за площею)."""
    import cv2
    quads = []
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in sorted(contours, key=cv2.contourArea, reverse=True)[:3]:
        area = cv2.contourArea(cnt)
        # Картка займає 18-92% кадру: менше — шум, більше — вже заповнює сама.
        if area < img_area * 0.18 or area > img_area * 0.92:
            continue
        peri = cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) == 4:
            quads.append(approx.reshape(4, 2).astype(np.float32))
        else:
            quads.append(cv2.boxPoints(cv2.minAreaRect(cnt)).astype(np.float32))
    return quads


def _warp_card(arr, quad):
    """Перспективне виправлення картки за 4 кутами. None якщо пропорції не карткові."""
    import cv2
    pts = _order_corners(quad)
    tl, tr, br, bl = pts
    maxW = int(max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl)))
    maxH = int(max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl)))
    if maxW < 200 or maxH < 120:
        return None
    # ID-1 ~1.58, паспорт ~1.42; відсіюємо «смужки».
    aspect = max(maxW, maxH) / float(max(1, min(maxW, maxH)))
    if aspect > 3.2:
        return None
    dst = np.array([[0, 0], [maxW - 1, 0], [maxW - 1, maxH - 1], [0, maxH - 1]],
                   dtype=np.float32)
    M = cv2.getPerspectiveTransform(pts, dst)
    return cv2.warpPerspective(arr, M, (maxW, maxH))


def _card_crop_candidates(img, max_candidates: int = 3) -> list:
    """Мульти-стратегійна детекція картки → список кропнутих PIL-зображень.

    Стратегії: Canny-краї, адаптивний поріг, Otsu — кожна ловить свій тип
    фону/контрасту. Беремо найбільші контури, warp-имо, дедуплікуємо за
    розміром. Порожній список → fallback нічого не робить (без регресу).
    """
    try:
        import cv2
        arr = _pil_to_cv2(img)                      # BGR
        h, w = arr.shape[:2]
        img_area = float(w * h)
        gray = cv2.cvtColor(arr, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 0)
        kernel = np.ones((3, 3), np.uint8)

        masks = []
        # 1) Canny-краї (чіткий контур картки на контрастному фоні)
        masks.append(cv2.dilate(cv2.Canny(blur, 30, 120), kernel, iterations=2))
        # 2) Адаптивний поріг (документ світліший/темніший за фон)
        at = cv2.adaptiveThreshold(blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                   cv2.THRESH_BINARY_INV, 35, 10)
        masks.append(cv2.morphologyEx(at, cv2.MORPH_CLOSE, kernel, iterations=2))
        # 3) Otsu (глобальний поріг — рівномірне освітлення)
        _, ot = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        masks.append(cv2.morphologyEx(ot, cv2.MORPH_CLOSE, kernel, iterations=2))

        quads = []
        for m in masks:
            quads.extend(_card_quads_from_mask(m, img_area))

        results, seen = [], []
        for q in quads:
            warped = _warp_card(arr, q)
            if warped is None:
                continue
            sh, sw = warped.shape[:2]
            # Дедуп: пропускаємо схожі за розміром (±8%)
            if any(abs(sh - s[0]) < s[0] * 0.08 and abs(sw - s[1]) < s[1] * 0.08
                   for s in seen):
                continue
            seen.append((sh, sw))
            results.append(_cv2_to_pil(warped))
            if len(results) >= max_candidates:
                break
        if results:
            _diag(f"    [Crop] {len(results)} card candidate(s) from {w}x{h}")
        return results
    except Exception as e:
        logger.debug("_card_crop_candidates error: %s", e)
        return []


def _deskew(img) -> PIL.Image:
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
            # 1-поточний ONNX: при паралельних воркерах не даємо одному екземпляру
            # захопити всі ядра (інакше перепідписка → сповільнення в рази).
            try:
                _paddle_ocr_instance = RapidOCR(intra_op_num_threads=1, inter_op_num_threads=1)
            except TypeError:
                _paddle_ocr_instance = RapidOCR()
            _diag("  [PaddleOCR] initialized OK (RapidOCR/ONNX, 1 thread)")
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


def _preprocess_variants(img) -> list[tuple[PIL.Image, str]]:
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

def _mrz_date_to_iso(yymmdd: str) -> str | None:
    """YYMMDD → YYYY-MM-DD.

    Це поле EXPIRY: вікно 00-50 → 20xx (паспорти видають на 10 років,
    тож expiry 2033+ — норма; стара межа <=30 робила з 2033 → 1933).
    """
    if len(yymmdd) != 6 or not yymmdd.isdigit():
        return None
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    year = 2000 + yy if yy <= 50 else 1900 + yy
    try:
        return date(year, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract_expiry_from_mrz(text: str) -> str | None:
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

    # TD3 (паспорт): expiry at data-line[21:27].
    # OCR (особливо RapidOCR) часто віддає рядки У ЗВОРОТНОМУ ПОРЯДКУ або
    # губить P<-рядок зовсім — тому пробуємо позицію [21:27] на КОЖНОМУ
    # 44-рядку, а не лише на mrz_44[1]. P<SURNAME-рядки відсіюються самі
    # (літери на місці дати не парсяться).
    for line44 in mrz_44:
        iso = _mrz_date_to_iso(line44[21:27])
        if iso:
            return iso

    # TD1 (ID карта): expiry at line2[8:14]
    if len(mrz_30) >= 3:
        iso = _mrz_date_to_iso(mrz_30[1][8:14])
        if iso:
            return iso

    # ── TOLERANT fallback (міжнародні ID/паспорти) ──
    # Якщо OCR пошкодив структуру рядків (з'їв пробіли/символи), шукаємо
    # expiry за ПОЗИЦІЙНИМ маркером MRZ незалежно від розбиття на рядки:
    #   DOB(6) check(1) sex(M/F/<) EXPIRY(6) check(1) country(3)
    iso = _mrz_tolerant_expiry(text)
    if iso:
        return iso

    return None


# Позиційні паттерни MRZ (толерантні до пошкодженого розбиття рядків):
# TD1 line2 (ID-карти): DOB(6) chk стать EXPIRY(6) chk КРАЇНА(3) — країна ПІСЛЯ
_MRZ_TOLERANT_RE = re.compile(r"(\d{6})(\d)([MF<])(\d{6})(\d)([A-Z]{3})")
# TD3 line2 (паспорти): КРАЇНА(3) DOB(6) chk стать EXPIRY(6) chk — країна ПЕРЕД
# Приклад: "1240925469GBR7510186M3305304<<<" → GBR 751018 6 M 330530 4
_MRZ_TD3_TOLERANT_RE = re.compile(r"[A-Z]{3}(\d{6})(\d)([MF<])(\d{6})(\d)")


def _mrz_tolerant_expiry(text: str) -> str | None:
    """Знаходить expiry в MRZ навіть якщо OCR пошкодив розбиття на рядки.

    Шукає позиційні блоки TD1 (DOB+стать+EXPIRY+країна) та TD3
    (країна+DOB+стать+EXPIRY). Expiry YY: 00-50 → 20xx.
    Рятує випадки, коли OCR прочитав дата-рядок, але загубив P<-рядок
    або переплутав порядок рядків.
    """
    cleaned = text.upper()
    for old, new in (('О', 'O'), ('С', 'C'), ('В', 'B'), ('Н', 'H'),
                     ('{', '<'), ('[', '<'), ('|', '<'), ('(', '<'),
                     (' ', '')):
        cleaned = cleaned.replace(old, new)

    def _exp_to_iso(exp_raw: str) -> str | None:
        try:
            yy, mm, dd = int(exp_raw[:2]), int(exp_raw[2:4]), int(exp_raw[4:6])
            year = 2000 + yy if yy <= 50 else 1900 + yy
            return date(year, mm, dd).strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            return None

    m = _MRZ_TOLERANT_RE.search(cleaned)
    if m:
        iso = _exp_to_iso(m.group(4))
        if iso:
            return iso

    m = _MRZ_TD3_TOLERANT_RE.search(cleaned)
    if m:
        iso = _exp_to_iso(m.group(4))
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


# ── Визначення країни документа (ISO-3) ──────────────────────────────────

# ISO-3 коди країн, що зустрічаються в MRZ міжнародних документів
_ISO3_CODES = {
    'CHE': 'Switzerland', 'FRA': 'France', 'DEU': 'Germany',
    'GBR': 'United Kingdom', 'USA': 'United States', 'CAN': 'Canada',
    'AUS': 'Australia', 'NZL': 'New Zealand', 'ITA': 'Italy',
    'ESP': 'Spain', 'PRT': 'Portugal', 'NLD': 'Netherlands',
    'BEL': 'Belgium', 'POL': 'Poland', 'CZE': 'Czechia',
    'SVK': 'Slovakia', 'HUN': 'Hungary', 'AUT': 'Austria',
    'DNK': 'Denmark', 'SWE': 'Sweden', 'NOR': 'Norway',
    'FIN': 'Finland', 'IRL': 'Ireland', 'ROU': 'Romania',
    'BGR': 'Bulgaria', 'HRV': 'Croatia', 'SVN': 'Slovenia',
    'EST': 'Estonia', 'LVA': 'Latvia', 'LTU': 'Lithuania',
    'LUX': 'Luxembourg', 'GRC': 'Greece', 'MEX': 'Mexico',
    'BRA': 'Brazil', 'ARG': 'Argentina', 'CHL': 'Chile',
    'JPN': 'Japan', 'KOR': 'South Korea', 'IND': 'India',
    'MYS': 'Malaysia', 'SGP': 'Singapore', 'ISR': 'Israel',
    'TUR': 'Turkey', 'UKR': 'Ukraine', 'RUS': 'Russia',
}
_ISO3_RE = re.compile(r"\b(" + "|".join(_ISO3_CODES.keys()) + r")\b")

# Назви країн у відкритому тексті → ISO-3
_COUNTRY_NAME_TO_ISO3 = {
    'switzerland': 'CHE', 'schweiz': 'CHE', 'suisse': 'CHE', 'svizzera': 'CHE',
    'france': 'FRA', 'république française': 'FRA', 'republique francaise': 'FRA',
    'germany': 'DEU', 'deutschland': 'DEU', 'bundesrepublik': 'DEU',
    'united kingdom': 'GBR', 'great britain': 'GBR',
    'united states': 'USA',
    'italia': 'ITA', 'italy': 'ITA', 'repubblica italiana': 'ITA',
    'españa': 'ESP', 'espana': 'ESP', 'spain': 'ESP',
    'česká republika': 'CZE', 'ceska republika': 'CZE', 'czech republic': 'CZE',
    'polska': 'POL', 'poland': 'POL',
    'nederland': 'NLD', 'netherlands': 'NLD',
    'österreich': 'AUT', 'osterreich': 'AUT', 'austria': 'AUT',
    'belgique': 'BEL', 'belgium': 'BEL', 'belgië': 'BEL',
    'portugal': 'PRT', 'sverige': 'SWE', 'sweden': 'SWE',
}


def _detect_country(text: str) -> str | None:
    """Визначає 3-літерний ISO-код країни з MRZ або відкритого тексту.

    Returns ISO-3 код (напр. 'CHE') або None.
    """
    if not text:
        return None
    up = text.upper()
    # 1) ISO-3 код як окреме слово в тексті/MRZ
    m = _ISO3_RE.search(up)
    if m:
        return m.group(1)
    # 2) MRZ: код країни приклеєний до типу документа/прізвища.
    #    напр. "P<CHESURNAME", "IDCHE...", "I<DEU..." → беремо 3 літери після
    #    префіксу типу документа.
    m = re.search(r"\b[A-Z]{1,2}<?([A-Z]{3})[A-Z<]", up)
    if m and m.group(1) in _ISO3_CODES:
        return m.group(1)
    # 3) MRZ позиційний блок: ...EXPIRY(6)+check+COUNTRY(3)
    m = _MRZ_TOLERANT_RE.search(up.replace('О', 'O').replace('С', 'C')
                                 .replace('В', 'B').replace('Н', 'H'))
    if m and m.group(6) in _ISO3_CODES:
        return m.group(6)
    # 4) Назва країни у відкритому тексті
    low = text.lower()
    for name, code in _COUNTRY_NAME_TO_ISO3.items():
        if name in low:
            return code
    return None


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
    # 0) Надійно визначена країна має пріоритет над евристиками нижче.
    #    MM/DD використовують по суті лише США; решта світу — DD/MM. Це виправляє
    #    IT/FR/DE-документи (напр. 11.04.2029 = 11 квітня, а не 4 листопада).
    #    US driver license зазвичай не має ISO3/MRZ → _detect_country поверне None
    #    → падаємо в евристики нижче, тож US-логіка не ламається.
    _country = _detect_country(text)
    if _country == 'USA':
        return 'us'
    if _country and _country in _ISO3_CODES:
        return 'eu'

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

# Скорочення місяців: англ. + фр./іт./ісп./нім./порт. (двомовні паспорти ЄС)
_MONTH_ABBR = (
    r'JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC'
    r'|JANV|FEV|FÉV|AVR|MAI|JUIN|JUIL|AOU|AOÛT|SEPT|DÉC'   # FR (+MAI = DE/NO)
    r'|GEN|MAG|GIU|LUG|AGO|SET|OTT|DIC'                               # IT
    r'|ENE|ABR'                                                       # ES
    r'|MÄR|MRZ|OKT|DEZ'                                          # DE
    r'|OUT'                                                           # PT
)

_DATE_PATTERNS = [
    re.compile(r'\b(\d{2})[./\-](\d{2})[./\-](\d{4})\b'),       # DD.MM.YYYY (4-digit year)
    re.compile(r'\b(\d{4})[.\-/](\d{2})[.\-/](\d{2})\b'),       # YYYY-MM-DD
    re.compile(
        r'\b(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{4})\b',
        re.I
    ),
    re.compile(r'\b(\d{2})[./\-](\d{2})[./\-](\d{2})\b'),       # DD.MM.YY (2-digit year!)
    re.compile(r'\b(\d{2})\s+(\d{2})\s+(\d{2,4})\b'),           # DD MM YY[YY] (Swiss/FR/EU spaces)
    # Двомовні паспортні дати: "30 MAY/MAI 33", "23MAR/MAR 2026", "26 FEB/FEV16"
    # OCR часто зліплює пробіли, тому \s* і рік 2 або 4 цифри.
    re.compile(
        r'(\d{1,2})\s*(' + _MONTH_ABBR + r')\s*\.?/\s*(' + _MONTH_ABBR + r')\s*\.?\s*(\d{4}|\d{2})(?!\d)',
        re.I
    ),
    # Компактна одномовна: "28Apr27", "28 Apr 27" (2- або 4-значний рік)
    re.compile(
        r'(?<![A-Za-z0-9])(\d{1,2})\s*(' + _MONTH_ABBR + r')\s*(\d{4}|\d{2})(?![0-9])',
        re.I
    ),
]

_EXPIRY_KEYWORDS = [
    'exp', 'expiry', 'expires', 'expiration', 'valid until', 'date of expiry',
    'exe',  # OCR часто плутає p→e: "exp" → "exe"
    'gültig bis', 'gueltig bis', 'ablaufdatum',
    "date d'expiration", 'expire le',
    'geldig tot', 'data di scadenza',
    'fecha de caducidad', 'vencimiento',
    'validade', 'data de validade',
    'platnost', 'platnost do', 'datum expirace',
    '4b', 'érvényes', 'lejárat',
    'effective',  # Australian DL: "Effective ... Expiry"
    # ── Розширені міжнародні мітки (EU/CH/інші) ──
    'valable', 'valable jusqu', "jusqu'au",  # FR: "valable jusqu'au"
    'data scadenza', 'scadenza',             # IT
    'válida hasta', 'valida hasta',          # ES
    'data ważności', 'ważności', 'termin ważności',  # PL
    'geldig tot',                            # NL
    'gyldig til', 'giltig till',             # DK/NO/SE
    'voimassa',                              # FI
    'identitätskarte', 'carte d',            # CH ID hints
    'geçerlilik', 'son kullanma',            # TR
    'действителен до', 'дійсний до',         # RU/UA
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

# Розширена мапа: + фр./іт./ісп./нім./порт. скорочення (двомовні паспорти)
_MONTH_MAP_EXT = {
    **_MONTH_MAP,
    'JANV': 1, 'FEV': 2, 'FÉV': 2, 'AVR': 4, 'MAI': 5, 'JUIN': 6,
    'JUIL': 7, 'AOU': 8, 'AOÛT': 8, 'SEPT': 9, 'DÉC': 12,            # FR (+MAI DE/NO)
    'GEN': 1, 'MAG': 5, 'GIU': 6, 'LUG': 7, 'AGO': 8, 'SET': 9,
    'OTT': 10, 'DIC': 12,                                            # IT
    'ENE': 1, 'ABR': 4,                                              # ES
    'MÄR': 3, 'MRZ': 3, 'OKT': 10, 'DEZ': 12,                        # DE
    'OUT': 10,                                                       # PT
}


def _yy_to_yyyy(yy: int) -> int:
    """2-значний рік → 4-значний. 00-50 → 2000-2050, 51-99 → 1951-1999."""
    return 2000 + yy if yy <= 50 else 1900 + yy


def _parse_date(match: re.Match, pat_idx: int, fmt: str = 'eu') -> str | None:
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
            elif (b > 12 and 1 <= a <= 12) or fmt == 'us':
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
            elif (b > 12 and 1 <= a <= 12) or fmt == 'us':
                mm, dd = a, b
            else:
                dd, mm = a, b
        elif pat_idx == 4:                   # DD MM YY[YY] (space-separated, Swiss/FR/EU)
            a, b, c = int(g[0]), int(g[1]), int(g[2])
            yyyy = c if len(g[2]) == 4 else _yy_to_yyyy(c)
            if a > 12 and 1 <= b <= 12:
                dd, mm = a, b
            elif b > 12 and 1 <= a <= 12:
                mm, dd = a, b
            else:
                dd, mm = a, b           # default EU style для пробільних дат
        elif pat_idx == 5:                   # DD MON1/MON2 YY[YY] (двомовний паспорт)
            dd = int(g[0])
            mm = (_MONTH_MAP_EXT.get(g[1].upper(), 0)
                  or _MONTH_MAP_EXT.get(g[2].upper(), 0))
            yyyy = int(g[3]) if len(g[3]) == 4 else _yy_to_yyyy(int(g[3]))
        elif pat_idx == 6:                   # DDMonYY компактна ("28Apr27")
            dd = int(g[0])
            mm = _MONTH_MAP_EXT.get(g[1].upper(), 0)
            yyyy = int(g[2]) if len(g[2]) == 4 else _yy_to_yyyy(int(g[2]))
        else:
            return None
        # Верхня межа року: поточний + 40. US-ліцензії (напр. Arizona — дійсна
        # до 65 років власника) і паспорти можуть мати exp за 30-40 років.
        # Стеля 2036 раніше ХИБНО відкидала валідні дати на кшталт 12/30/2041,
        # лишаючи тільки issue-дату → документ помилково ставав "не валід".
        _max_year = date.today().year + 40
        if not (1 <= mm <= 12 and 1 <= dd <= 31 and 1950 <= yyyy <= _max_year):
            return None
        d = date(yyyy, mm, dd)
        iso = d.strftime("%Y-%m-%d")
        # Фільтр: дати в межах ±1 дня від сьогодні — підозрілий OCR-артефакт
        # (Tesseract іноді "читає" дату з метаданих EXIF або шуму)
        today = date.today()
        if abs((d - today).days) <= 1:
            return None
        return iso
    except (ValueError, IndexError):
        return None


def _find_expiry_in_text(text: str) -> tuple[str, bool] | None:
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
        _diag("      [_find_expiry] no dates parsed from text")
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

    # Рядки де ВСІ дати старі (<2000) → однозначно DOB-рядок без EXP.
    # ВАЖЛИВО: якщо на рядку є і стара (DOB 1985), і свіжа (EXP 2026) дати —
    # НЕ викидаємо весь рядок, бо тоді втратимо валідну EXP. Свіжа дата
    # буде використана як кандидат, стару відсіє фільтр d > today_iso / d >= "2010".
    from collections import defaultdict as _dd
    _line_dates: dict[int, list[str]] = _dd(list)
    for d, li in all_dates:
        _line_dates[li].append(d)
    lines_with_old_dates = {
        li for li, dlist in _line_dates.items()
        if dlist and all(d < "2000-01-01" for d in dlist)
    }
    # Рядки з мішаним складом (є і <2000, і >=2010) — лог для діагностики
    mixed_old_recent = {
        li for li, dlist in _line_dates.items()
        if any(d < "2000-01-01" for d in dlist) and any(d >= "2010-01-01" for d in dlist)
    }
    if mixed_old_recent:
        _diag(f"      [skip] mixed old+recent lines (NOT skipped): {sorted(mixed_old_recent)}")
    # Додаємо skipped рядки з пріоритету 0 та їх сусідів (±1)
    skip_all = set(lines_with_old_dates)
    for sl in skipped_lines:
        skip_all.update({sl - 1, sl, sl + 1})

    # Пріоритет 1a: МАЙБУТНІ дати поруч із expiry-keywords (±2 рядки), НЕ DOB/ISS
    # Виняток: якщо дата майбутня І на її рядку є також стара дата (DOB+EXP на
    # одному рядку) — не блокуємо її через _is_near_dob, бо на такому рядку
    # «DOB» якір стосується саме старої дати, а не свіжої.
    def _date_on_mixed_line(line_idx: int) -> bool:
        return line_idx in mixed_old_recent

    candidates_future = [
        d for d, li in all_dates
        if d > today_iso
        and any(abs(li - el) <= 2 for el in expiry_lines)
        and (not _is_near_dob(li) or _date_on_mixed_line(li))
        and (not _is_near_issue(li) or _date_on_mixed_line(li))
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
    _diag("      [P1b] no recent dates near EXP")

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

    _diag("      [_find_expiry] no valid date found → None")
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


def _spatial_find_expiry(img) -> str | None:
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
        _diag("      [Spatial] no date words found")

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

def _vote_dates(dates: list[tuple[str, str]]) -> str | None:
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

    # Немає консенсусу, різні дати — перевіряємо майбутні vs минулі
    # (EXP завжди майбутня; якщо один движок витягнув минулу дату — це ймовірно
    # ISS/DOB, тоді пріоритет майбутній)
    future_dates = [(d, s) for d, s in dates
                    if _dt_date.fromisoformat(d) >= today]
    past_dates = [(d, s) for d, s in dates
                  if _dt_date.fromisoformat(d) < today]
    if future_dates and past_dates:
        # Є і майбутні, і минулі — обираємо найближчу майбутню (реалістичний EXP)
        best_future = min(future_dates,
                          key=lambda x: (_dt_date.fromisoformat(x[0]) - today).days)
        _diag(f"    [Vote] no consensus (future vs past): {dates}, "
              f"using future → {best_future[0]}")
        return best_future[0]

    # Різні дати — бере першу (Tesseract як більш надійний)
    _diag(f"    [Vote] no consensus: {dates}, using first")
    return dates[0][0]


def _date_crop_reocr(img, approx_date: str) -> str | None:
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


def _try_ocr_on_image(img, deadline: float | None = None) -> str | None:
    """Пробує знайти expiry date на одному зображенні (без повороту).

    Стратегія (з Ensemble voting):
      0. Deskew — виправляємо нахил
      1. MRZ-зона (нижні 25% + верхні 25%) → якщо є MRZ → СТОП
      2. Tesseract spatial (image_to_data) → шукаємо дату поруч з EXP
      3. Tesseract text (image_to_string) → шукаємо за ключовими словами
      4. PaddleOCR text → теж шукаємо → VOTING з Tesseract якщо обидва знайшли
      5. Sauvola + Tesseract fallback
      6. Date crop re-OCR (якщо знайшли приблизну дату — перечитуємо точніше)

    deadline (time.monotonic) — м'який дедлайн: важкі кроки пропускаються,
    коли часу мало, щоб повернути хоч щось замість таймауту.
    Повертає exp_date_iso або None.
    """
    w, h = img.size
    today_iso = date.today().strftime("%Y-%m-%d")
    all_found: list[str] = []

    def _time_left() -> float:
        return 999.0 if deadline is None else deadline - time.monotonic()

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
                _diag("    [Spatial] future date → RETURN (skip PaddleOCR)")
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
            # MRZ на ПОВНОМУ тексті (EU ID/паспорт): надійніше за дату-за-міткою.
            # Ловить MRZ навіть якщо він не потрапив у crop-зони верх/низ.
            mrz_full = _extract_expiry_from_mrz(full_text)
            if mrz_full:
                _diag(f"    [Text-MRZ] ✅ FOUND: {mrz_full} → RETURN")
                return mrz_full
            result = _find_expiry_in_text(full_text)
            if result:
                tesseract_text_date, tesseract_text_kw = result
                _diag(f"    [Text] date={tesseract_text_date}, kw={tesseract_text_kw}")
                # FAST PATH: Tesseract text знайшов майбутню дату з keyword
                if tesseract_text_date > today_iso and tesseract_text_kw:
                    _diag("    [Text] ✅ future + keyword → RETURN (skip PaddleOCR)")
                    return tesseract_text_date
        else:
            _diag("    [Text] OCR returned empty text")
    except Exception as e:
        _diag(f"    [Text] error: {e}")

    # ── Крок 4: PaddleOCR (тільки якщо Tesseract НЕ знайшов впевнену дату) ──
    # PaddleOCR допомагає коли: Tesseract нічого, або знайшов минулу дату,
    # або знайшов без keyword (невпевнено) → voting для підтвердження
    paddle_date = None
    paddle_kw = False
    if _time_left() < 8:
        _diag(f"    ⏱ {_time_left():.0f}s left → skip Paddle/Sauvola/CLAHE, "
              f"return best-so-far")
        return max(all_found) if all_found else None
    try:
        paddle_text = _paddle_ocr_text(img)
        if paddle_text:
            paddle_text = _normalize_text(paddle_text)
            paddle_preview = paddle_text.replace('\n', ' | ')[:300]
            _diag(f"    [Paddle] OCR: {paddle_preview}")
            # MRZ на повному PaddleOCR-тексті (Paddle часто краще читає дрібний MRZ)
            mrz_pad = _extract_expiry_from_mrz(paddle_text)
            if mrz_pad:
                _diag(f"    [Paddle-MRZ] ✅ FOUND: {mrz_pad} → RETURN")
                return mrz_pad
            result = _find_expiry_in_text(paddle_text)
            if result:
                paddle_date, paddle_kw = result
                _diag(f"    [Paddle] date={paddle_date}, kw={paddle_kw}")
        else:
            _diag("    [Paddle] OCR returned empty text")
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
                _diag("    [Ensemble] past date WITHOUT keyword → IGNORED")

    # ── Крок 5: Sauvola бінаризація + Tesseract fallback ──
    if not all_found and _time_left() >= 6:
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
                        _diag("    [Sauvola] ✅ FUTURE date → RETURN")
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
                _diag("    [Sauvola] OCR returned empty text")
        except Exception as e:
            _diag(f"    [Sauvola] error: {e}")

    # ── Крок 6: CLAHE + Sharpen fallback ──
    if not all_found and _time_left() >= 5:
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
                        _diag("    [CLAHE] ✅ FUTURE date → RETURN")
                        return exp
                    if has_kw:
                        all_found.append(exp)
            else:
                _diag("    [CLAHE] OCR returned empty text")
        except Exception as e:
            _diag(f"    [CLAHE] error: {e}")

    # ── Крок 7: Date Crop Re-OCR (перечитуємо зону дати точніше) ──
    # DateCrop уточнює дату тільки якщо результат близький до оригіналу
    # (той самий рік і місяць, або різниця ≤ 45 днів).
    # Якщо DateCrop видає дико іншу дату — це сміття, ігноруємо.
    if all_found and _time_left() >= 4:
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

    _diag("    [Result] no date found on this orientation")
    return None


# ── Німецькі права без терміну дії (поле 4b порожнє) ──────────────────────
# Старі DE-права (видані до 2013) НЕ мають друкованої дати закінчення — вони
# чинні до поетапних дедлайнів обміну ЄС. OCR правильно не знаходить дату, тож
# такі документи раніше летіли в «Невизначені». Детектуємо їх окремо, щоб
# винести в папку «ручна перевірка» (рішення юзера — дату з 4a не вгадуємо).
_DE_LICENSE_MARKERS = (
    "FUHRERSCHEIN", "HRERSCHEIN", "UHRERSCH",
    "BUNDESREPUB", "DEUTSCHLAN", "FAHRERLAUBNIS",
)


def _is_german_license(text: str) -> bool:
    """Нечітко визначає, чи текст — з німецького посвідчення водія.

    Толерантно до спотвореного OCR: або словниковий маркер (навіть частковий),
    або одночасна наявність полів 4a. (видача) і 4c. (орган) — вони унікальні
    для DE-прав і виживають навіть при поганому розпізнаванні.
    """
    if not text:
        return False
    up = text.upper().replace("Ü", "U")
    if any(w in up for w in _DE_LICENSE_MARKERS):
        return True
    return bool(re.search(r"4\s*A[.\s)]", up) and re.search(r"4\s*C[.\s)]", up))


def quick_german_license_check(image_bytes: bytes) -> bool:
    """Швидка (1 OCR-прохід) перевірка, чи фото — нім. посвідчення водія.

    Викликається ai_sorter'ом окремо від local_analyze, коли той не дав дати
    або впав у таймаут: повний пайплайн на «глухих» фото часто не встигає дійти
    до детекції. Тут — один дешевий Tesseract-прохід на зменшеному зображенні.
    """
    if not _tesseract_available():
        return False
    try:
        img = _prepare_image(image_bytes)
        txt = _ocr_image(img, config="--psm 3")
        return _is_german_license(_normalize_text(txt))
    except Exception:
        return False


def local_analyze(image_bytes: bytes, client_id: str = "",
                  time_budget: float = 40.0) -> dict:
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

    # ── М'який бюджет часу ──
    # Раніше: зовнішній asyncio.wait_for(45с) ВБИВАВ аналіз разом із уже
    # знайденими кандидатами (замір 08.07: 66/69 «Невизначених» — таймаути,
    # у 61 з них дата БУЛА знайдена, але результат викинуто). Тепер пайплайн
    # сам стежить за дедлайном: пропускає важкі кроки/орієнтації, коли час
    # закінчується, і ПОВЕРТАЄ краще зі знайденого замість нічого.
    deadline = time.monotonic() + time_budget

    def _run_orientations(base) -> list[tuple[str, str]]:
        """OCR по 4 орієнтаціях (0/180/90CW/270CW) з early-exit на майбутній даті."""
        oris = [
            (base,                           "Local OCR"),
            (base.rotate(180, expand=False), "Local OCR (180°)"),
            (base.rotate(270, expand=True),  "Local OCR (90° CW)"),
            (base.rotate(90,  expand=True),  "Local OCR (270° CW)"),
        ]
        found: list[tuple[str, str]] = []
        for rotated_img, source_label in oris:
            time_left = deadline - time.monotonic()
            if time_left < 5:
                _diag(f"  ⏱ time budget: {time_left:.0f}s left → skip remaining "
                      f"orientations (return {len(found)} candidate(s))")
                break
            _diag(f"  --- Orientation: {source_label} (time left {time_left:.0f}s) ---")
            try:
                exp = _try_ocr_on_image(rotated_img, deadline=deadline)
                if exp:
                    found.append((exp, source_label))
                    _diag(f"  → candidate: {exp}")
                    if exp > today_iso:
                        _diag("  ⚡ EARLY EXIT: future date found")
                        break
                else:
                    _diag("  → no date on this orientation")
            except Exception as e:
                _diag(f"  → error: {e}")
        return found

    # 1) Спершу аналізуємо ОРИГІНАЛ — перевірена поведінка, без ризику регресу.
    candidates = _run_orientations(img)

    # 2) Fallback: якщо дат не знайдено — пробуємо кропнути картку (кілька
    #    стратегій детекції), прибравши фон/решітку/відблиски. Беремо перший
    #    кандидат, що дав дату. Суто additive: оригінал уже не дав нічого,
    #    тож гірше зробити неможливо.
    if not candidates and deadline - time.monotonic() >= 10:
        for ci, cropped in enumerate(_card_crop_candidates(img, max_candidates=1), 1):
            _diag(f"  [Crop-fallback] try candidate #{ci} {cropped.size}")
            candidates = _run_orientations(cropped)
            if candidates:
                img = cropped   # для подальшого визначення країни
                break

    if not candidates:
        # Дати нема. Детекцію нім. прав без терміну НЕ робимо тут: ці «глухі» фото
        # часто впираються в 45с таймаут ще до цього місця. Замість цього ai_sorter
        # викликає quick_german_license_check() окремо (з власним коротким лімітом).
        _diag("  FINAL: no dates found → None")
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

    # ── Визначаємо країну документа (для міжнародних док.) ──
    # Один швидкий текстовий OCR-прохід на оригінальній орієнтації.
    try:
        country_text = _ocr_image(img, config="--psm 3")
        if country_text:
            ct = _detect_country(_normalize_text(country_text))
            if not ct:
                # друга спроба — PaddleOCR (краще читає латиницю на ID)
                ct = _detect_country(_normalize_text(_paddle_ocr_text(img)))
            if ct:
                result["country"] = ct
                _diag(f"  Country detected: {ct}")
    except Exception as e:
        _diag(f"  Country detection error: {e}")

    return result


# ── Класифікація сторони документа (front / back) ─────────────────────
# Безкоштовно, тільки OCR. Сигнали (емпірично підтверджено на UK/DE/CA/FR):
#   BACK  — таблиця категорій водійського (AM/A1/B1/C1/D1/BE/CE/DE), 'fkq',
#           підписи "Codes/Valid to/Issued by/Category".
#   FRONT — заголовки документа (DRIVING LICENCE/PASSPORT/PERSONALAUSWEIS...),
#           іменні поля (Surname/Given/DOB), коди полів 4a-4d,
#           паспортний MRZ (рядок з P< на дата-сторінці).
_SIDE_CAT_CODES = ('AM', 'A1', 'A2', 'B1', 'C1E', 'C1', 'D1E', 'D1', 'BE', 'CE', 'DE')
_SIDE_FRONT_HEADERS = (
    'DRIVING LICENCE', 'DRIVER LICENSE', 'DRIVERS LICENSE', 'PERMIS DE CONDUIRE',
    'FUHRERSCHEIN', 'PASSPORT', 'PASSEPORT', 'REISEPASS', 'PERSONALAUSWEIS',
    'IDENTITY CARD', 'CARTE NATIONALE', 'REPUBLIQUE', 'BUNDESREPUBLIK',
    'CANADA', 'PASAPORTE',
)
_SIDE_FRONT_FIELDS = ('SURNAME', 'GIVEN NAME', 'DATE OF BIRTH', 'NATIONALITY',
                      '4A', '4B', '4C', '4D')
_SIDE_BACK_FIELDS = ('CODES', 'VALID TO', 'ISSUED BY', 'CATEGOR', 'FKQ', 'CODE 12')


# ── Детектор обличчя (портрет лицьової сторони) ───────────────────────
_FACE_CASCADE = None


def _get_face_cascade():
    """Lazy-кеш Haar-каскаду облич (йде в комплекті з opencv, без завантажень)."""
    global _FACE_CASCADE
    if _FACE_CASCADE is None:
        import cv2
        _FACE_CASCADE = cv2.CascadeClassifier(
            os.path.join(cv2.data.haarcascades, 'haarcascade_frontalface_default.xml'))
    return _FACE_CASCADE


def _has_large_face(img) -> bool:
    """True якщо є ВЕЛИКИЙ портрет — надійна ознака лицьової сторони.

    Поріг minSize ≈18% картки відсікає дрібний «привид-фото» на звороті ID.
    Пробує 4 орієнтації (фото може бути повернуте).
    """
    try:
        import cv2
        casc = _get_face_cascade()
        if casc.empty():
            return False
        arr = _pil_to_cv2(img)
        h, w = arr.shape[:2]
        ms = int(min(h, w) * 0.18)
        for rot in (None, cv2.ROTATE_90_CLOCKWISE, cv2.ROTATE_180,
                    cv2.ROTATE_90_COUNTERCLOCKWISE):
            a = arr if rot is None else cv2.rotate(arr, rot)
            gray = cv2.cvtColor(a, cv2.COLOR_BGR2GRAY)
            if len(casc.detectMultiScale(gray, 1.1, 6, minSize=(ms, ms))) > 0:
                return True
        return False
    except Exception as e:
        logger.debug("_has_large_face error: %s", e)
        return False


def _side_score(up: str) -> tuple[int, int]:
    """(front_score, back_score) для нормалізованого ВЕРХНЬОГО тексту."""
    cats = sum(1 for c in _SIDE_CAT_CODES if re.search(r'\b' + re.escape(c) + r'\b', up))
    fkq = 1 if re.search(r'\bFKQ\b', up) else 0
    back = cats + fkq * 4 + sum(2 for m in _SIDE_BACK_FIELDS if m in up)
    front = (sum(3 for h in _SIDE_FRONT_HEADERS if h in up)
             + sum(1 for f in _SIDE_FRONT_FIELDS if f in up))
    mrz_lines = [ln for ln in up.split('\n') if ln.count('<') >= 4]
    if any(ln.replace(' ', '').startswith(('P<', 'PK', 'PM')) for ln in mrz_lines):
        front += 4   # паспортний MRZ TD3 (2 рядки) → дата-сторінка = front
    elif len(mrz_lines) >= 2:
        back += 5    # MRZ TD1 (3 рядки) — на звороті ID-картки
    return front, back


def detect_side(image_bytes: bytes) -> str:
    """Визначає сторону документа: 'front' | 'back' | 'unknown'.

    Безкоштовно (локальний OCR + детекція обличчя). Сигнали:
      FRONT — великий портрет (обличчя), заголовки документа, іменні поля,
              паспортний MRZ (TD3).
      BACK  — таблиця категорій водійського, MRZ TD1 (3 рядки), back-поля.
    Для чистоти сигналів спершу кропить картку; пробує 4 орієнтації.
    """
    if not _tesseract_available():
        return 'unknown'
    try:
        img = _prepare_image(image_bytes)
    except Exception:
        return 'unknown'

    # ── ШВИДКИЙ ШЛЯХ: велике обличчя = надійний сигнал лицьової ──
    # Працює на повному фото (без кропу). Driver-license/ID-зворот облич не
    # має; дрібний «привид» на звороті відсікає поріг minSize. Для більшості
    # лиць тут і завершуємо — БЕЗ важкого OCR (це різко прискорює масові прогони).
    if _has_large_face(img):
        return 'front'

    # ── Без обличчя → ймовірно зворот/нечітко. Легкий OCR (early-exit) ──
    # для таблиці категорій водійського / MRZ-TD1 / заголовків. Без кропу.
    best_front = best_back = 0
    best_signal = -1
    for angle in (0, 180, 270, 90):
        try:
            rimg = img if angle == 0 else img.rotate(angle, expand=True)
            up = _normalize_text(_ocr_image(rimg, '--psm 3')).upper()
        except Exception:
            continue
        fr, bk = _side_score(up)
        if fr + bk > best_signal:
            best_signal, best_front, best_back = fr + bk, fr, bk
        if best_signal >= 6:
            break

    front, back = best_front, best_back
    if back >= 4 and back > front:
        return 'back'
    if front >= 3 and front > back:
        return 'front'
    if back > front and back >= 2:
        return 'back'
    if front > back and front >= 1:
        return 'front'
    return 'unknown'


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

    country = result.get("country")
    country_line = ""
    if country:
        name = _ISO3_CODES.get(country, country)
        country_line = f"🌍 Країна: {country} ({name})\n"

    return (
        f"📋 **Результат аналізу документа**\n\n"
        f"📅 Дійсний до: `{exp}`\n"
        f"{country_line}"
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
