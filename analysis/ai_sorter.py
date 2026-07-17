import os

# ── Ліміт потоків нативних бібліотек (до транзитивного import numpy/onnx) ──
# Локальний OCR CPU-bound; кожен воркер має бути 1-поточним, паралелізм дає
# _local_ocr_semaphore нижче. Детальніше — у doc_analyzer.py.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
import asyncio
import base64
import io
import logging
import platform
import re
import shutil
import tempfile
import time
import zipfile
from datetime import date, datetime

import boto3
from dateutil import parser as dateparser
from dotenv import load_dotenv
from openai import AsyncOpenAI
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)
load_dotenv("token.env")

# Авторизація: захищаємо всі user-facing хендлери. Імпорт внизу файлу був би
# чистіший, але декоратори застосовуються над функціями — потрібен зараз.
from handlers.admin import require_auth  # noqa: E402


def _diag_ai(client_id: str, msg: str) -> None:
    """Пише в діагностичний лог doc_analyzer (спільний файл)."""
    from analysis.doc_analyzer import _diag, _diag_ctx
    _diag_ctx.client_id = client_id  # встановлюємо контекст для потоку
    _diag(f"[AI] {msg}")

# ─────────────────────────────────────────────
#  КОНСТАНТИ
# ─────────────────────────────────────────────
MAX_ZIP_SIZE_MB            = 5120  # 5 ГБ — для Telegram Local API або Google Drive
MAX_ZIP_SIZE_MB_TELEGRAM   = 50    # стандартний Telegram Bot API
MAX_ZIP_UNCOMPRESSED_MB    = 10240 # 10 ГБ розпакованих (раз стиснутий ліміт 5 ГБ)

# ── Textract circuit-breaker ─────────────────────────────────────────────
# Якщо Textract підряд падає (network/auth/throttle/AWS down), не маємо сенсу
# дзвонити в нього на кожному документі. Лічимо consecutive-failures у межах
# сесії; коли перевищуємо threshold — перемикаємось у "Textract unavailable"
# режим і пропускаємо його одразу (документ → невизначені або Vision-fallback).
# Лічильник скидається при першому ж успішному виклику.
_TEXTRACT_FAIL_THRESHOLD = 5
_textract_consecutive_fails = 0
_textract_disabled_for_session = False

# Тимчасова папка для резервних ZIP (внутрішня, для /myresults)
# __file__ тепер в analysis/ → шлях до results/ в кореневій папці проекту
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(_PROJECT_ROOT, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# ── Папка на робочому столі ──
# На Windows зчитуємо реальний шлях Desktop через реєстр (правильно для OneDrive)
def _get_desktop_path() -> str:
    if platform.system() == "Windows":
        try:
            import winreg
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders"
            )
            desktop, _ = winreg.QueryValueEx(key, "Desktop")
            winreg.CloseKey(key)
            return desktop
        except Exception:
            pass
    return os.path.join(os.path.expanduser("~"), "Desktop")

LOCAL_RESULTS_DIR = os.path.join(_get_desktop_path(), "AI_Docs_Results")
os.makedirs(LOCAL_RESULTS_DIR, exist_ok=True)
SUPPORTED_IMAGE_EXTS  = ('.png', '.jpg', '.jpeg', '.webp', '.bmp')
MAX_IMAGE_SIZE_PX     = 1536   # стиск перед API — зменшує вартість і час
MAX_CONCURRENT_API    = 15     # клієнтський semaphore: макс одночасних клієнтів
                               # природньо обмежує і Textract, і OpenAI
API_RETRY_COUNT       = 2      # кількість повторних спроб при помилці API
PROGRESS_UPDATE_SEC   = 8      # оновлення статусу в Telegram кожні N секунд
CHUNK_ZIP_MB          = 20   # максимальний розмір однієї частини ZIP для Telegram
SESSION_KEEP_DAYS     = int(os.getenv("SESSION_KEEP_DAYS", "30"))  # скільки днів зберігати сесії

# ── Telegram канал для результатів (опціонально) ──
_TG_RESULTS_CHANNEL = os.getenv("TG_RESULTS_CHANNEL_ID", "")

# ─────────────────────────────────────────────
#  КЛІЄНТИ API
# ─────────────────────────────────────────────
from botocore.config import Config as BotocoreConfig

_AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
_AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
_S3_BUCKET  = os.getenv("AWS_S3_BUCKET_NAME", "")
_S3_REGION  = os.getenv("AWS_S3_REGION", "us-east-1")

# Ключі AWS наявні й не плейсхолдер (без них Textract взагалі неможливий).
_TEXTRACT_KEYS_OK = (
    bool(_AWS_KEY) and bool(_AWS_SECRET)
    and not str(_AWS_KEY).lower().startswith(("your_", "placeholder"))
)
# Дефолт для кнопки вибору: env TEXTRACT_ENABLED (false → за замовч. «без Textract»).
_TEXTRACT_ENV = os.getenv("TEXTRACT_ENABLED", "").strip().lower()
TEXTRACT_ENABLED = _TEXTRACT_KEYS_OK and _TEXTRACT_ENV not in ("0", "false", "no", "off")
# Per-run перемикач: ставить воркер черги за вибором кнопки перед кожним аналізом.
# None = взяти дефолт TEXTRACT_ENABLED (для прямих викликів поза чергою).
_RUN_USE_TEXTRACT = None

# Per-run перемикач: чи перейменовувати фото на front/back у результаті
# (за вибором користувача на екрані підтвердження). True = як було раніше.
_RUN_RENAME_SIDES = True

textract_client = boto3.client(
    'textract',
    region_name='us-east-1',
    aws_access_key_id=_AWS_KEY,
    aws_secret_access_key=_AWS_SECRET,
    # pool ≥ MAX_CONCURRENT_API щоб уникнути "Connection pool is full"
    # Таймаути: швидкий fail замість зависань на хвилини (несплачений/недоступний AWS).
    config=BotocoreConfig(
        max_pool_connections=MAX_CONCURRENT_API + 5,
        connect_timeout=5,
        read_timeout=15,
        retries={'max_attempts': 2, 'mode': 'standard'},
    )
)
if not TEXTRACT_ENABLED:
    logger.info("AWS Textract DISABLED (TEXTRACT_ENABLED=false або ключі відсутні)")

s3_client = boto3.client(
    's3',
    region_name=_S3_REGION,
    aws_access_key_id=_AWS_KEY,
    aws_secret_access_key=_AWS_SECRET,
)

_OPENAI_KEY = os.getenv("OPENAI_API_KEY") or ""
# OpenAI ввімкнений тільки якщо є валідний ключ (не placeholder/не пустий)
OPENAI_ENABLED = bool(_OPENAI_KEY) and not _OPENAI_KEY.lower().startswith(("your_", "sk-your", "placeholder"))
openai_client = AsyncOpenAI(api_key=_OPENAI_KEY) if OPENAI_ENABLED else None
if not OPENAI_ENABLED:
    logger.info("OpenAI Vision DISABLED (ключ відсутній або placeholder)")

# Один семафор на рівні КЛІЄНТА — природньо обмежує і Textract, і OpenAI
# (OpenAI — fallback всередині клієнта, тому окремий semaphore не потрібен)
_api_semaphore = asyncio.Semaphore(MAX_CONCURRENT_API)

# ── Окремий семафор для CPU-важкого локального OCR ────────────────────────
# MAX_CONCURRENT_API (15) — для МЕРЕЖЕВИХ стадій (Textract/OpenAI), там 15 ок.
# Локальний OCR — CPU-bound: при 15 паралельних + багатопотоковий ONNX/OpenCV
# ядра перепідписані, кожне фото сповільнюється в рази → таймаути 35с → хибні
# NOT_FOUND. Обмежуємо ~третиною ядер (кожен воркер 1-поточний, див.
# doc_analyzer). Тюнінг: env LOCAL_OCR_CONCURRENCY.
_LOCAL_OCR_CONCURRENCY = max(
    2, int(os.getenv("LOCAL_OCR_CONCURRENCY", str(max(2, (os.cpu_count() or 4) // 3))))
)
_local_ocr_semaphore = asyncio.Semaphore(_LOCAL_OCR_CONCURRENCY)
logger.info("Local OCR concurrency: %d (CPU: %s)", _LOCAL_OCR_CONCURRENCY, os.cpu_count())

# ─────────────────────────────────────────────
#  ЧЕРГА ЗАДАЧ (для паралельних запитів)
# ─────────────────────────────────────────────

# maxsize=10: не більше 10 аналізів у черзі одночасно.
# При переповненні — користувач отримує відмову замість нескінченного очікування.
_QUEUE_MAX_SIZE     = 10
_analysis_queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
_cancel_events:  dict[int, asyncio.Event] = {}   # chat_id → Event скасування
_queue_task:     asyncio.Task | None = None
# Лічильник для виявлення «мертвого» воркера між перезапусками
_queue_generation: int = 0


async def _queue_worker(generation: int) -> None:
    """Стабільний воркер черги аналізів.

    Проблема старого коду: воркер завершувався через 10 хв простою (TimeoutError),
    але нові задачі могли потрапляти в чергу і залишатися необробленими до наступного
    виклику _enqueue_analysis.

    Виправлення:
      - Воркер НЕ завершується при порожній черзі — чекає нескінченно
      - Завершується ТІЛЬКИ якщо з'явився новий воркер (_queue_generation змінився)
      - При будь-якій помилці обробки — логується з контекстом і воркер продовжує роботу
    """
    global _queue_task
    logger.info("Queue worker #%d started", generation)
    while True:
        # Перевіряємо чи ми ще актуальне покоління воркера
        if generation != _queue_generation:
            logger.info("Queue worker #%d: superseded, stopping", generation)
            break
        try:
            job = await asyncio.wait_for(_analysis_queue.get(), timeout=30)
        except TimeoutError:
            # Черга порожня — продовжуємо чекати (НЕ виходимо)
            continue
        except asyncio.CancelledError:
            logger.info("Queue worker #%d: cancelled", generation)
            break

        update, context, zip_path, work_dir, status_msg, use_textract, rename_sides = job
        chat_id_log = getattr(getattr(update, 'effective_chat', None), 'id', '?')
        # Per-run вибір Textract + front/back (кнопки) — серіалізовано, воркер один.
        global _RUN_USE_TEXTRACT, _RUN_RENAME_SIDES
        _RUN_USE_TEXTRACT = bool(use_textract)
        _RUN_RENAME_SIDES = bool(rename_sides)
        try:
            logger.info("Queue worker #%d: processing job for chat %s (textract=%s, sides=%s)",
                        generation, chat_id_log, _RUN_USE_TEXTRACT, _RUN_RENAME_SIDES)
            await _process_zip_file(update, context, zip_path, work_dir, status_msg)
        except Exception as e:
            logger.error(
                "Queue worker #%d: unhandled error for chat %s: %s",
                generation, chat_id_log, e, exc_info=True
            )
            try:
                await status_msg.edit_text(
                    f"❌ Критична помилка обробки черги:\n`{e}`",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
        finally:
            _analysis_queue.task_done()

    logger.info("Queue worker #%d stopped", generation)


def _ensure_queue_worker() -> None:
    """Запускає воркер якщо він не активний. Викликається синхронно."""
    global _queue_task, _queue_generation
    if _queue_task is None or _queue_task.done():
        _queue_generation += 1
        _queue_task = asyncio.create_task(_queue_worker(_queue_generation))
        logger.debug("Queue worker started, generation=%d", _queue_generation)


def _scan_zip_contents(zip_path: str) -> tuple[int, int, int]:
    """Швидко сканує ZIP і повертає (clients, images, nested_zips) для прев'ю.

    Клієнт = top-level папка, image = файл-зображення. Якщо всередині є
    вкладені .zip (архів в архіві) — зазирає і в них (читає namelist з
    пам'яті, нічого не пише на диск).
    """
    import io as _io
    import zipfile as _zf
    _img_ext = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif", ".bmp"}

    def _scan_namelist(names, clients: set, depth_prefix: str = "") -> int:
        imgs = 0
        for name in names:
            parts = name.replace("\\", "/").split("/")
            if len(parts) >= 2 and parts[0]:
                clients.add(depth_prefix + parts[0])
            if os.path.splitext(name)[1].lower() in _img_ext:
                imgs += 1
        return imgs

    clients: set[str] = set()
    images = 0
    nested = 0
    try:
        with _zf.ZipFile(zip_path, "r") as z:
            inner_zip_names = [n for n in z.namelist() if n.lower().endswith(".zip")]
            images += _scan_namelist(z.namelist(), clients)
            # Зазираємо у вкладені архіви
            for iz in inner_zip_names:
                nested += 1
                try:
                    data = z.read(iz)
                    with _zf.ZipFile(_io.BytesIO(data)) as inner:
                        images += _scan_namelist(inner.namelist(), clients,
                                                 depth_prefix=f"{iz}/")
                except Exception as e:
                    logger.debug("scan nested zip %s: %s", iz, e)
    except Exception as e:
        logger.warning("scan_zip: %s", e)
    return len(clients), images, nested


def _scan_7z_contents(archive_path: str) -> tuple[int, int, int]:
    """Сканує RAR/7z через `7z l -slt` → (clients, images, nested) для прев'ю."""
    seven = _find_7z()
    if not seven:
        return 0, 0, 0
    import subprocess
    _img_ext = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif", ".bmp"}
    try:
        out = subprocess.run([seven, "l", "-slt", archive_path],
                             capture_output=True, text=True, timeout=300).stdout
    except Exception as e:
        logger.warning("scan_7z: %s", e)
        return 0, 0, 0
    clients: set[str] = set()
    images = 0
    for line in out.splitlines():
        if not line.startswith("Path = "):
            continue
        name = line[7:].strip().replace("\\", "/")
        # пропускаємо запис самого архіву (абсолютний шлях із диском)
        if os.path.splitext(name)[1].lower() in {".rar", ".7z", ".zip"}:
            continue
        parts = [p for p in name.split("/") if p]
        if len(parts) >= 2:
            clients.add("/".join(parts[:-1]))   # папка, що містить файл
        if os.path.splitext(name)[1].lower() in _img_ext:
            images += 1
    return len(clients), images, 0


def _scan_archive_contents(archive_path: str) -> tuple[int, int, int]:
    """Прев'ю-скан будь-якого архіву: ZIP → namelist, RAR/7z → 7z list."""
    fmt = _detect_archive_format(archive_path)
    if fmt == "zip":
        return _scan_zip_contents(archive_path)
    if fmt in ("rar", "7z"):
        return _scan_7z_contents(archive_path)
    return 0, 0, 0


def _build_confirm_kb(token: str, rename_sides: bool):
    """Будує клавіатуру екрана підтвердження аналізу.

    Верхній ряд — тумблер «підписувати front/back» (за вибором користувача).
    Далі — вибір режиму (Textract) і скасування. Винесено в helper, бо тумблер
    перемальовує ту саму клавіатуру.
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    sides_label = ("🏷 Підписувати front/back: ✅ УВІМК"
                   if rename_sides else
                   "🏷 Підписувати front/back: ⬜ ВИМК")
    rows = [[InlineKeyboardButton(sides_label, callback_data=f"sidestoggle_{token}")]]

    if _TEXTRACT_KEYS_OK:
        rows += [
            [InlineKeyboardButton("⚡ Локально (безкоштовно)",
                                  callback_data=f"confirm_analysis_{token}_tx0")],
            [InlineKeyboardButton("☁️ З Textract (точніше, платно)",
                                  callback_data=f"confirm_analysis_{token}_tx1")],
            [InlineKeyboardButton("❌ Скасувати", callback_data=f"cancelconfirm_{token}")],
        ]
    else:
        rows += [
            [InlineKeyboardButton("▶️ Почати аналіз (локально)",
                                  callback_data=f"confirm_analysis_{token}_tx0")],
            [InlineKeyboardButton("❌ Скасувати", callback_data=f"cancelconfirm_{token}")],
        ]
    return InlineKeyboardMarkup(rows)


async def sides_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тумблер «підписувати front/back» на екрані підтвердження.

    Фліпає pending[token]['rename_sides'] і перемальовує клавіатуру.
    """
    query = update.callback_query
    if not query or not query.data:
        return
    token = query.data[len("sidestoggle_"):]
    pending = context.bot_data.get("pending_analysis", {})
    data = pending.get(token)
    if not data:
        try:
            await query.answer("⌛ Сесія прострочена — надішли архів заново.", show_alert=True)
        except Exception:
            pass
        return
    data["rename_sides"] = not data.get("rename_sides", True)
    try:
        await query.answer("✅ Підпис увімкнено" if data["rename_sides"] else "⬜ Підпис вимкнено")
    except Exception:
        pass
    try:
        await query.edit_message_reply_markup(
            reply_markup=_build_confirm_kb(token, data["rename_sides"]))
    except Exception:
        pass


async def _ask_confirm_analysis(update, context, zip_path: str, work_dir: str, status_msg) -> None:
    """Показує preview ZIP (клієнти/фото/оцінка вартості) і чекає підтвердження.
    Дані складаємо в context.bot_data[token], callback витягне і запустить _enqueue_analysis.
    """
    import uuid as _uuid

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # noqa: F401

    clients_count, images_count, nested_count = await asyncio.to_thread(
        _scan_archive_contents, zip_path)

    # Орієнтовна вартість: AWS Textract ~$0.0015/стор, GPT-4o Vision ~$0.003/стор.
    # Не всі фото підуть у OpenAI (fallback). Грубо: $0.002 × images.
    est_cost_usd = images_count * 0.002

    token = _uuid.uuid4().hex[:10]
    pending = context.bot_data.setdefault("pending_analysis", {})
    # TTL 1 година на випадок якщо юзер забув — _cleanup старих токенів нижче
    pending[token] = {
        "zip_path": zip_path,
        "work_dir": work_dir,
        "status_msg_chat_id": status_msg.chat_id,
        "status_msg_id": status_msg.message_id,
        "user_id": update.effective_user.id if update.effective_user else 0,
        "created_at": time.time(),
        "rename_sides": True,   # дефолт: підписувати front/back (як було)
    }
    # Чистимо протухлі (>1 год)
    stale = [k for k, v in pending.items() if time.time() - v.get("created_at", 0) > 3600]
    for k in stale:
        old = pending.pop(k, {})
        shutil.rmtree(old.get("work_dir", ""), ignore_errors=True)

    kb = _build_confirm_kb(token, rename_sides=True)

    if images_count == 0 and nested_count == 0:
        await status_msg.edit_text(
            "⚠️ В архіві не знайдено зображень (.jpg/.png/.webp/.heic).\n"
            "Перевір структуру: <b>папка клієнта → фото всередині</b>.",
            parse_mode="HTML",
        )
        # Видаляємо token — запускати нема що
        pending.pop(token, None)
        shutil.rmtree(work_dir, ignore_errors=True)
        return

    nested_line = (
        f"🗜 Вкладених архівів: <code>{nested_count}</code> "
        f"(розпакую автоматично)\n" if nested_count else ""
    )
    await status_msg.edit_text(
        f"📦 <b>Архів готовий до аналізу</b>\n\n"
        f"{nested_line}"
        f"👥 Клієнтів (папок): <code>{clients_count}</code>\n"
        f"🖼 Зображень: <code>{images_count}</code>\n"
        f"💵 Вартість API <i>(лише з Textract)</i>: <b>~${est_cost_usd:.2f}</b>\n\n"
        f"🏷 <b>Підпис front/back</b> — перейменувати фото в результаті на "
        f"front/back (тумблер зверху).\n"
        f"⚡ <b>Локально</b> — безкоштовно, тільки OCR.\n"
        f"☁️ <b>З Textract</b> — точніше на складних/прострочених, платно.\n\n"
        f"Обери режим:",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def _enqueue_analysis(
    update, context, zip_path: str, work_dir: str, status_msg,
    use_textract: bool = False, rename_sides: bool = True,
) -> None:
    """Додає аналіз у чергу і повідомляє користувача про позицію.

    При переповненні черги (>_QUEUE_MAX_SIZE) — відхиляє запит з поясненням
    замість того щоб блокуватись або ростити чергу нескінченно.
    """
    pos = _analysis_queue.qsize()

    # ── Перевірка переповнення ──
    if pos >= _QUEUE_MAX_SIZE:
        await status_msg.edit_text(
            f"⚠️ *Черга переповнена* ({pos}/{_QUEUE_MAX_SIZE}).\n\n"
            f"Зараз обробляється багато архівів одночасно.\n"
            f"Спробуйте надіслати архів через кілька хвилин.",
            parse_mode="Markdown"
        )
        # Прибираємо тимчасові файли — аналіз не буде виконано
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)
        return

    # ── Повідомляємо про позицію в черзі ──
    if pos > 0:
        await status_msg.edit_text(
            f"⏳ *Черга аналізів:* ви {pos + 1}-й із максимум {_QUEUE_MAX_SIZE}.\n"
            f"Зачекайте завершення попередніх аналізів...",
            parse_mode="Markdown"
        )

    await _analysis_queue.put((update, context, zip_path, work_dir, status_msg,
                               use_textract, rename_sides))

    # ── Гарантуємо що воркер активний ──
    _ensure_queue_worker()


# ─────────────────────────────────────────────
#  СТИСНЕННЯ ЗОБРАЖЕННЯ
# ─────────────────────────────────────────────

def _compress_image(image_bytes: bytes, max_px: int = MAX_IMAGE_SIZE_PX) -> bytes:
    """Зменшує зображення до max_px по більшій стороні якщо воно більше.Зменшує вартість API і прискорює обробку без втрати читабельності тексту.Повертає JPEG bytes."""
    try:
        from PIL import Image  # type: ignore[import-untyped]
        img = Image.open(io.BytesIO(image_bytes))
        img = img.convert("RGB")
        w, h = img.size
        if max(w, h) > max_px:
            ratio = max_px / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=88, optimize=True)
        return buf.getvalue()
    except ImportError:
        logger.debug("Pillow не встановлено — надсилаємо оригінал")
        return image_bytes
    except Exception as e:
        logger.debug("Помилка стиску зображення: %s", e)
        return image_bytes


def _get_progress_bar(done: int, total: int, length: int = 12) -> str:
    """Повертає текстовий прогрес-бар: [████░░░░] 50%"""
    if total <= 0:
        return f"`[{'░' * length}] 0%`"
    filled  = int((done / total) * length)
    percent = int((done / total) * 100)
    bar = "█" * filled + "░" * (length - filled)
    return f"`[{bar}] {percent}%`"


# ─────────────────────────────────────────────
#  TEXTRACT
# ─────────────────────────────────────────────

async def _textract_analyze(image_bytes: bytes, client_id: str = "") -> dict:
    """Викликає AWS Textract AnalyzeID.

    Повертає dict з полями: exp_date, doc_type, country, name, _confidence, _error.

    Безпека: ВСІ можливі помилки (network, auth, throttle, AWS down, неочікувані
    exceptions у boto3) ловляться. На виході завжди валідний dict — caller не має
    шансу впасти на TypeError від None'а.

    Circuit breaker: при 5+ consecutive failures у сесії перемикається в режим
    "skip" — повертає _error='circuit_open' одразу, без виклику AWS.
    """
    global _textract_consecutive_fails, _textract_disabled_for_session

    result = {"exp_date": None, "doc_type": None, "country": None, "name": None,
              "_confidence": 0.0, "_error": None}

    # ── Textract увімкнено? (per-run вибір кнопки або дефолт + наявність ключів) ──
    _use_tx = TEXTRACT_ENABLED if _RUN_USE_TEXTRACT is None else (_TEXTRACT_KEYS_OK and _RUN_USE_TEXTRACT)
    if not _use_tx:
        result["_error"] = "disabled"
        return result

    # ── Circuit breaker: Textract вже визнаний недоступним у цій сесії ──
    if _textract_disabled_for_session:
        result["_error"] = "circuit_open"
        _diag_ai(client_id, "  Textract SKIPPED (circuit breaker open)")
        return result

    try:
        response = await asyncio.to_thread(
            textract_client.analyze_id,
            DocumentPages=[{'Bytes': image_bytes}]
        )

        # ── Діагностика: логуємо ВСІ поля від Textract ──
        _diag_ai(client_id, "--- Textract AnalyzeID response ---")
        for page_idx, id_doc in enumerate(response.get('IdentityDocuments', [])):
            fields = id_doc.get('IdentityDocumentFields', [])
            _diag_ai(client_id, f"  Page {page_idx}: {len(fields)} fields")
            for field in fields:
                ftype = field.get('Type', {}).get('Text', '')
                ftype_conf = field.get('Type', {}).get('Confidence', 0.0)
                val = field.get('ValueDetection', {})
                raw_text = val.get('Text', '')
                norm_val = val.get('NormalizedValue', {}).get('Value', '')
                norm_type = val.get('NormalizedValue', {}).get('ValueType', '')
                confidence = val.get('Confidence', 0.0)

                # Логуємо кожне поле
                norm_info = f" norm={norm_val}" if norm_val else ""
                _diag_ai(client_id,
                         f"    {ftype:30s} = {raw_text:30s}{norm_info:20s} "
                         f"conf={confidence:5.1f}%  type_conf={ftype_conf:5.1f}%")

        # ── Витягуємо потрібні поля ──
        for id_doc in response.get('IdentityDocuments', []):
            fields = id_doc.get('IdentityDocumentFields', [])

            for field in fields:
                ftype = field.get('Type', {}).get('Text', '')
                val   = field.get('ValueDetection', {})
                confidence = val.get('Confidence', 0.0)
                # NormalizedValue — стандартизований формат від Textract (пріоритет)
                text: str = val.get('NormalizedValue', {}).get('Value') or val.get('Text') or ""

                if ftype in ('EXPIRATION_DATE', 'DOCUMENT_EXPIRATION_DATE'):
                    # Беремо тільки якщо confidence > 50% (уникаємо сміття)
                    if confidence > 50 and text.strip():
                        # Якщо вже є дата — беремо ту що з вищим confidence
                        if result["exp_date"] is None or confidence > result["_confidence"]:
                            result["exp_date"] = text
                            result["_confidence"] = confidence
                elif ftype == 'ID_TYPE':
                    result["doc_type"] = text
                elif ftype == 'STATE_NAME' and text.strip():
                    result["country"] = text
                elif ftype == 'COUNTRY' and text.strip():
                    # COUNTRY тільки якщо STATE_NAME ще не заповнено
                    if not result["country"]:
                        result["country"] = text
                elif ftype == 'FIRST_NAME' and text.strip():
                    result["name"] = text + " " + (result["name"] or "")
                elif ftype == 'LAST_NAME' and text.strip():
                    result["name"] = (result["name"] or "") + " " + text

            # Якщо знайшли дату — не перевіряємо інші сторінки
            if result["exp_date"]:
                break

        _diag_ai(client_id, f"  → exp_date={result['exp_date']}, "
                 f"doc_type={result['doc_type']}, country={result['country']}, "
                 f"conf={result['_confidence']:.1f}%")

        # Виклик пройшов без exception — скидаємо лічильник consecutive-failures.
        # InvalidParameter ми НЕ рахуємо як failure тут (це ок, документ просто
        # не ID — Textract сам гарантовано працює).
        _textract_consecutive_fails = 0

    except Exception as e:
        err_name = type(e).__name__
        err_str = str(e)
        is_invalid_doc = 'InvalidParameter' in err_name or 'UnsupportedDocument' in err_name

        if 'Throttling' in err_name or 'throttling' in err_str.lower():
            logger.warning("Textract: throttling, потрібен retry")
            result["_error"] = "throttling"
            _diag_ai(client_id, "  Textract THROTTLING")
        elif is_invalid_doc:
            logger.warning("Textract: невалідний документ — %s", e)
            result["_error"] = "invalid_param"
            _diag_ai(client_id, f"  Textract INVALID: {e}")
        else:
            # Сюди потрапляє: ConnectionError, EndpointConnectionError, BotoCoreError,
            # ExpiredTokenException, AccessDeniedException, ServiceUnavailable, ...
            logger.warning("Textract error: %s", e)
            result["_error"] = err_str
            _diag_ai(client_id, f"  Textract ERROR: {e}")

        # Circuit breaker: інкрементимо лічильник тільки на "інфраструктурних"
        # помилках (НЕ на invalid_param — то проблема документа, не AWS).
        if not is_invalid_doc:
            _textract_consecutive_fails += 1
            if (_textract_consecutive_fails >= _TEXTRACT_FAIL_THRESHOLD
                    and not _textract_disabled_for_session):
                _textract_disabled_for_session = True
                logger.error(
                    "Textract вимкнено для сесії: %d помилок підряд. "
                    "Документи без локальної OCR-дати → 'невизначені'.",
                    _textract_consecutive_fails,
                )
                _diag_ai(client_id,
                         f"  *** Textract CIRCUIT BREAKER OPEN "
                         f"({_textract_consecutive_fails} fails in row) ***")

    if result.get("name"):
        result["name"] = result["name"].strip()

    return result


# ─────────────────────────────────────────────
#  OPENAI VISION (резерв)
# ─────────────────────────────────────────────

async def _openai_vision_analyze(image_bytes: bytes, client_id: str = "") -> dict:
    """GPT-4o Vision як резерв коли Textract не впорався.Повертає dict з exp_date та doc_type."""
    result = {"exp_date": None, "doc_type": None}

    # Якщо OpenAI вимкнений (немає ключа / placeholder) — одразу повертаємо порожній результат
    if not OPENAI_ENABLED or openai_client is None:
        return result

    b64 = base64.b64encode(image_bytes).decode('utf-8')
    prompt = (
        "Analyze this identity document image carefully.\n"
        "Return a JSON object with exactly these fields:\n"
        "{\n"
        '  "exp_date": "YYYY-MM-DD or null",\n'
        '  "doc_type": "passport/driver_license/id_card/other or null"\n'
        "}\n"
        "For exp_date: look for labels EXP, EXPIRES, Expiry, 4b, Valid Until, Gültig bis.\n"
        "Return ONLY the JSON, no explanation."
    )

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {
                        "url": f"data:image/jpeg;base64,{b64}",
                        "detail": "high"
                    }}
                ]
            }],
            temperature=0.0,
            max_tokens=100,
            response_format={"type": "json_object"}
        )
        raw_content = response.choices[0].message.content or "{}"
        _diag_ai(client_id, "--- OpenAI Vision response ---")
        _diag_ai(client_id, f"  raw: {raw_content}")
        import json
        data = json.loads(raw_content)
        exp = data.get("exp_date") or ""
        if exp and exp.lower() not in ("null", "none", ""):
            result["exp_date"] = exp
        result["doc_type"] = data.get("doc_type")
        _diag_ai(client_id, f"  → exp_date={result['exp_date']}, doc_type={result['doc_type']}")

    except Exception as e:
        logger.debug("OpenAI Vision error: %s", e)
        _diag_ai(client_id, f"  OpenAI Vision ERROR: {e}")

    return result


# ─────────────────────────────────────────────
#  ОСНОВНИЙ АНАЛІЗ ДОКУМЕНТА
# ─────────────────────────────────────────────

async def _analyze_single_image(image_bytes: bytes, client_id: str = "") -> dict:
    """Внутрішня обгортка `__analyze_single_image_unsafe` з top-level safety net.

    ГАРАНТУЄ: ніколи не raise'ить — будь-яка несподівана помилка ловиться,
    логується, і повертається валідний dict {exp_date: None, source: "ERROR_*"}.
    Документ за такою відповіддю → "невизначені" (через date_not_found гілку).
    """
    try:
        return await _analyze_single_image_unsafe(image_bytes, client_id=client_id)
    except Exception as e:
        logger.exception("ai_sorter: несподівана помилка аналізу для %s: %s",
                         client_id, e)
        _diag_ai(client_id, f"FATAL ERROR в _analyze_single_image: {type(e).__name__}: {e}")
        return {
            "exp_date": None,
            "doc_type": None,
            "country":  None,
            "source":   f"ERROR_{type(e).__name__}",
        }


async def _analyze_single_image_unsafe(image_bytes: bytes, client_id: str = "") -> dict:
    """Аналізує одне зображення:
    0. Перевірка кешу (по MD5 хешу)
    1. Локальний Tesseract OCR (~1-2 сек, безкоштовно)
    2. AWS Textract (хмарний fallback)
    3. OpenAI Vision (запасний — тільки якщо Textract не знайшов дату)
    Semaphore на рівні клієнта в analyze_client_documents — тут не потрібен.
    Повертає: {exp_date, doc_type, source}"""
    import hashlib as _hashlib

    from database import get_cache_entry, save_cache_entry

    # ── Крок 0: Кеш ──
    # CACHE_VERSION: збільшувати при змінах OCR-логіки (vote, parse, rotations тощо),
    # щоб старі помилкові записи не «прилипли» і перераховувались заново.
    # v2 (2026-04-18): Vote future-vs-past fix, skip-логіка по датах, 90° CW
    # v3 (2026-06-02): year cap 2036→+40y, intl dates (FR space+4dig), MRZ on
    #   full text, country detect, past-date cloud escalation
    # v4 (2026-06-12): card-crop fallback + guard «стара дата = ДН, не expiry»
    #   (фікс італійських ID, де зворот давав 19xx замість 20xx)
    # v5 (2026-07-01): 1-поточний OCR + CPU-семафор (усунення таймаутів під
    #   навантаженням) + EU date-format за країною. Bump обов'язковий: інвалідовує
    #   старі NOT_FOUND, щоб «Невизначені» перерахувались новим пайплайном.
    # v6 (2026-07-08): м'який time-budget у local_analyze (повертає best-so-far
    #   замість втрати результату на wait_for), MRZ TD3 на будь-якому рядку +
    #   tolerant TD3-line2, двомовні/компактні дати ("30 MAY/MAI33", "28Apr27").
    #   Замір 08.07: 66/69 Невизначених — таймаути, у 61 дата знайдена але викинута.
    CACHE_VERSION = b"v6"
    img_hash = _hashlib.md5(image_bytes + CACHE_VERSION).hexdigest()
    cached = await asyncio.to_thread(get_cache_entry, img_hash)
    if cached:
        logger.debug("Кеш-хіт для %s", img_hash[:8])
        cached_src = cached.get('source', '?')
        _diag_ai(client_id, f"CACHE HIT hash={img_hash[:8]} → exp={cached.get('exp_date')} "
                 f"src={cached_src}")
        res = {
            "exp_date": cached.get("exp_date"),
            "doc_type": cached.get("doc_type"),
            "country":  cached.get("country"),
            "source":   f"🗄 Кеш ({cached_src})",
        }
        # Нім. права без терміну кешуються з source=DE_NO_EXPIRY → відновлюємо hint,
        # щоб і при кеш-хіті вони йшли в «ручну перевірку», а не в «Невизначені».
        if cached_src == "DE_NO_EXPIRY":
            res["doc_hint"] = "de_license_no_expiry"
        return res

    # ── Крок 1: Локальний Tesseract OCR (безкоштовний, ~1-2 сек) ──
    # local_past_fallback: якщо локальний OCR дав ТІЛЬКИ прострочену дату,
    # ми НЕ довіряємо їй сліпо (часто це дата видачі/народження, а не EXP).
    # Замість цього ескалюємо на Textract/OpenAI; якщо хмара теж нічого не дасть —
    # повертаємось до цієї past-дати як останній варіант.
    local_past_fallback = None
    local_hint = None   # напр. "de_license_no_expiry" — прокидаємо у NOT_FOUND-гілки
    today_iso = date.today().strftime("%Y-%m-%d")
    try:
        from analysis.doc_analyzer import local_analyze
        # CPU-семафор: обмежуємо к-сть ОДНОЧАСНИХ локальних OCR (кожен 1-поточний),
        # щоб не перепідписати ядра. local_analyze тепер сам стежить за своїм
        # бюджетом (40с) і повертає best-so-far; wait_for 50с — лише страховка
        # від зависання нативного коду (не має спрацьовувати в нормі).
        async with _local_ocr_semaphore:
            local_result = await asyncio.wait_for(
                asyncio.to_thread(local_analyze, image_bytes, client_id),
                timeout=50.0
            )
        local_hint = local_result.get("doc_hint")
        if local_result.get("exp_date"):
            local_exp = local_result["exp_date"]
            if local_exp > today_iso:
                # Майбутня дата — довіряємо локальному OCR одразу.
                logger.info("Tesseract знайшов дату: %s (source: %s)", local_exp, local_result.get("source"))
                result = {
                    "exp_date": local_exp,
                    "doc_type": local_result.get("doc_type"),
                    "country":  local_result.get("country"),
                    "source":   local_result.get("source", "Local OCR"),
                }
                await asyncio.to_thread(
                    save_cache_entry, img_hash,
                    result["exp_date"], result["doc_type"], result["country"], result["source"]
                )
                return result
            else:
                # Прострочена дата — підозріла (ймовірно issue/DOB). Зберігаємо як
                # fallback і йдемо на Textract/OpenAI за надійнішим результатом.
                # ── Захист від дати народження як «expiry» ──
                # Італійські ID закінчуються в день народження власника → ДН і
                # scadenza мають однаковий день/місяць. OCR на повернутих/темних
                # фото читає рік народження (19xx) замість expiry (20xx).
                # Якщо рік неправдоподібно старий для дати закінчення → це майже
                # напевно ДН: НЕ вважаємо документ простроченим (→ Невизначені,
                # ручна перевірка), а не хибно «Не_придатні».
                try:
                    _exp_year = int(str(local_exp)[:4])
                except ValueError:
                    _exp_year = 0
                if _exp_year and _exp_year < date.today().year - 14:
                    logger.info("Tesseract дата %s неправдоподібно стара для expiry "
                                "(ймовірно ДН) — НЕ прострочений, ескалація/Невизначені", local_exp)
                    _diag_ai(client_id,
                             f"Local OCR → {local_exp} занадто стара (ймовірно ДН) → "
                             f"не fallback; якщо хмара мовчить → Невизначені")
                    # local_past_fallback НЕ ставимо → без хмари буде NOT_FOUND → Невизначені
                else:
                    logger.info("Tesseract дав ПРОСТРОЧЕНУ дату %s — ескалація на хмару", local_exp)
                    _diag_ai(client_id,
                             f"Local OCR → {local_exp} PAST (source={local_result.get('source')}) "
                             f"→ не довіряємо, ескалація на Textract/OpenAI")
                    local_past_fallback = {
                        "exp_date": local_exp,
                        "doc_type": local_result.get("doc_type"),
                        "country":  local_result.get("country"),
                        "source":   local_result.get("source", "Local OCR"),
                    }
    except TimeoutError:
        logger.warning("Локальний OCR timeout (45с) — складне фото, далі по пайплайну")
        _diag_ai(client_id, "Local OCR TIMEOUT (45s)")
    except Exception as e:
        logger.warning("Локальний OCR помилка: %s", e)
        _diag_ai(client_id, f"Local OCR ERROR: {e}")

    # ── Швидка перевірка: нім. права без терміну (окремо від важкого пайплайну) ──
    # local_analyze на «глухих» фото часто впирається в 45с таймаут ДО детекції,
    # тож робимо 1 дешевий OCR-прохід із коротким лімітом. Сюди доходять лише фото
    # без майбутньої дати (валідні док. повернулись раніше). Нім. права без дати →
    # окрема категорія «ручна перевірка», а не «Невизначені».
    if local_hint != "de_license_no_expiry":
        try:
            from analysis.doc_analyzer import quick_german_license_check
            async with _local_ocr_semaphore:
                _is_de = await asyncio.wait_for(
                    asyncio.to_thread(quick_german_license_check, image_bytes),
                    timeout=20.0,
                )
            if _is_de:
                _diag_ai(client_id, "quick DE-license check → hit (нім. права без терміну)")
                await asyncio.to_thread(save_cache_entry, img_hash, None,
                                        "DE Führerschein", "DEU", "DE_NO_EXPIRY")
                return {"exp_date": None, "doc_type": "DE Führerschein", "country": "DEU",
                        "source": "DE_NO_EXPIRY", "doc_hint": "de_license_no_expiry"}
        except Exception as e:
            _diag_ai(client_id, f"quick DE-license check err: {e}")

    # Стискаємо в окремому потоці — Pillow CPU-bound, не блокує event loop.
    # Якщо стиснення впало (битий файл / неочікуваний формат) — пропускаємо
    # Textract і OpenAI: документ → невизначені.
    try:
        compressed = await asyncio.to_thread(_compress_image, image_bytes)
    except Exception as e:
        logger.warning("Не вдалось стиснути зображення для %s: %s", client_id, e)
        _diag_ai(client_id, f"_compress_image FAILED: {e} → NOT_FOUND")
        if local_past_fallback:
            await asyncio.to_thread(
                save_cache_entry, img_hash, local_past_fallback["exp_date"],
                local_past_fallback["doc_type"], local_past_fallback["country"],
                local_past_fallback["source"])
            return local_past_fallback
        await asyncio.to_thread(save_cache_entry, img_hash, None, None, None, "COMPRESS_ERROR")
        return {"exp_date": None, "doc_type": None, "country": None,
                "source": "COMPRESS_ERROR"}

    # ── Крок 2: Textract ──
    # Якщо circuit breaker уже відкритий — _textract_analyze поверне одразу
    # _error='circuit_open' без виклику AWS. Не робимо зайвих retry.
    textract_result = {"exp_date": None, "doc_type": None, "_error": None}
    for attempt in range(API_RETRY_COUNT):
        textract_result = await _textract_analyze(compressed, client_id=client_id)
        if textract_result.get("exp_date"):
            break
        err = textract_result.get("_error")
        if err == "throttling":
            await asyncio.sleep(0.5 * (attempt + 1))   # зростаюча затримка
        elif err in ("circuit_open", "disabled"):
            break  # Textract вимкнено / circuit-open — retry безглузді
        elif err:
            await asyncio.sleep(0.2)
        else:
            break  # Textract нормально відпрацював, просто дати немає — retry марний

    exp_date_str = None
    if textract_result["exp_date"]:
        raw = str(textract_result["exp_date"]).strip()
        try:
            # Textract NormalizedValue зазвичай у форматі YYYY-MM-DD або MM/DD/YYYY
            # Спробувати спочатку ISO формат
            from datetime import datetime as _dt
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y",
                        "%Y-%m-%dT%H:%M:%S", "%d %b %Y", "%b %d, %Y"):
                try:
                    exp_date_str = _dt.strptime(raw, fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
            # Fallback на dateparser якщо жоден формат не підійшов
            if not exp_date_str:
                exp_date_str = dateparser.parse(raw).strftime("%Y-%m-%d")
        except Exception:
            logger.warning("Не вдалось розпарсити Textract дату: %r", raw)

    if exp_date_str:
        doc_type_raw = (textract_result.get("doc_type") or "").upper()
        is_back_side = "BACK" in doc_type_raw

        # ── Перевірка BACK сторони: Textract часто плутає ISS/RE-ISS з EXP ──
        # Якщо Textract каже що це BACK і дата прострочена — не довіряти
        if is_back_side:
            from datetime import datetime as _dt_check
            try:
                parsed_exp = _dt_check.strptime(exp_date_str, "%Y-%m-%d")
                if parsed_exp < _dt_check.now():
                    _diag_ai(client_id,
                             f"Textract BACK side → {exp_date_str} PAST date, "
                             f"likely ISS/RE-ISS → SKIP, try OpenAI Vision")
                    exp_date_str = None  # скидаємо — не довіряємо
                else:
                    _diag_ai(client_id,
                             f"Textract BACK side → {exp_date_str} future date → OK")
            except ValueError:
                pass

    if exp_date_str:
        _diag_ai(client_id, f"Textract → {exp_date_str} (doc_type={textract_result.get('doc_type')}, country={textract_result.get('country')})")
        result = {
            "exp_date": exp_date_str,
            "doc_type": textract_result.get("doc_type"),
            "country":  textract_result.get("country"),
            "source":   "AWS Textract",
        }
        await asyncio.to_thread(
            save_cache_entry, img_hash,
            result["exp_date"], result["doc_type"], result["country"], result["source"]
        )
        return result

    # ── Крок 3: OpenAI Vision (запасний — тільки якщо Textract не дав дату) ──
    if not OPENAI_ENABLED:
        # OpenAI вимкнений — пропускаємо, щоб не спамити лог.
        # Якщо локальний OCR мав хоч прострочену дату — повертаємо її як останній варіант.
        if local_past_fallback:
            _diag_ai(client_id, f"OpenAI disabled, Textract empty → local past fallback "
                     f"{local_past_fallback['exp_date']}")
            await asyncio.to_thread(
                save_cache_entry, img_hash, local_past_fallback["exp_date"],
                local_past_fallback["doc_type"], local_past_fallback["country"],
                local_past_fallback["source"])
            return local_past_fallback
        if local_hint == "de_license_no_expiry":
            await asyncio.to_thread(save_cache_entry, img_hash, None,
                                    "DE Führerschein", "DEU", "DE_NO_EXPIRY")
            return {"exp_date": None, "doc_type": "DE Führerschein", "country": "DEU",
                    "source": "DE_NO_EXPIRY", "doc_hint": "de_license_no_expiry"}
        await asyncio.to_thread(save_cache_entry, img_hash, None, None, None, "NOT_FOUND")
        return {"exp_date": None, "doc_type": None, "country": None, "source": "NOT_FOUND"}

    _diag_ai(client_id, "Textract → no date, trying OpenAI Vision")
    logger.debug("Textract не знайшов дату — пробуємо OpenAI Vision")
    vision_result = await _openai_vision_analyze(compressed, client_id=client_id)

    if vision_result["exp_date"]:
        try:
            exp_date_str = dateparser.parse(
                str(vision_result["exp_date"]).strip()
            ).strftime("%Y-%m-%d")
            result = {
                "exp_date": exp_date_str,
                "doc_type": vision_result.get("doc_type"),
                "country":  textract_result.get("country"),
                "source":   "OpenAI Vision",
            }
            await asyncio.to_thread(
                save_cache_entry, img_hash,
                result["exp_date"], result["doc_type"], result["country"], result["source"]
            )
            return result
        except Exception:
            pass

    # Хмара нічого не дала — якщо локальний OCR мав прострочену дату, повертаємо її.
    if local_past_fallback:
        _diag_ai(client_id, f"Textract+OpenAI empty → local past fallback "
                 f"{local_past_fallback['exp_date']}")
        await asyncio.to_thread(
            save_cache_entry, img_hash, local_past_fallback["exp_date"],
            local_past_fallback["doc_type"], local_past_fallback["country"],
            local_past_fallback["source"])
        return local_past_fallback

    if local_hint == "de_license_no_expiry":
        await asyncio.to_thread(save_cache_entry, img_hash, None,
                                "DE Führerschein", "DEU", "DE_NO_EXPIRY")
        return {"exp_date": None, "doc_type": "DE Führerschein", "country": "DEU",
                "source": "DE_NO_EXPIRY", "doc_hint": "de_license_no_expiry"}
    await asyncio.to_thread(save_cache_entry, img_hash, None, None, None, "NOT_FOUND")
    return {"exp_date": None, "doc_type": None, "country": None, "source": "NOT_FOUND"}


async def analyze_client_documents(client_name: str, client_path: str) -> dict:
    """Аналізує всі зображення в папці клієнта.
    Semaphore на рівні клієнта — обмежує одночасну обробку і Textract, і OpenAI.
    Якщо кілька фото — перевіряє всі і бере перше з датою.
    Повертає: {name, status, exp_date, doc_type, is_valid, error}"""
    async with _api_semaphore:
        images = []
        for root, _, files in os.walk(client_path):
            for f in sorted(files):
                if f.lower().endswith(SUPPORTED_IMAGE_EXTS):
                    images.append(os.path.join(root, f))

        if not images:
            return {
                "name": client_name, "status": "❓ Немає фото",
                "exp_date": None, "doc_type": None,
                "is_valid": False, "error": "no_image"
            }

        def _read_image(path: str) -> bytes:
            with open(path, 'rb') as f:
                return f.read()

        # Перевіряємо всі зображення, беремо перше успішне
        last_result = None
        de_license_seen = False   # нім. права без терміну (жодне фото не дало дати)
        for img_path in images:
            try:
                img_bytes = await asyncio.to_thread(_read_image, img_path)
                result = await _analyze_single_image(img_bytes, client_id=client_name)
                last_result = result
                if result.get("doc_hint") == "de_license_no_expiry":
                    de_license_seen = True
                    break  # нім. права без терміну — класифікація визначена, решту фото не чіпаємо
                if result["exp_date"]:
                    break  # знайшли дату — достатньо
            except Exception as e:
                logger.warning("Помилка читання '%s': %s", img_path, e)

        # Перевірка після циклу (не всередині!)
        if not last_result or not last_result.get("exp_date"):
            # Нім. права без друкованого терміну → окрема категорія «ручна перевірка»
            # (а не «Невизначені»). Дату не вгадуємо — рішення юзера.
            if de_license_seen:
                return {
                    "name": client_name,
                    "status": "🇩🇪 Права без терміну — ручна перевірка",
                    "exp_date": None, "doc_type": "DE Führerschein",
                    "country": "DEU", "is_valid": False,
                    "error": "de_license_no_expiry",
                }
            return {
                "name": client_name, "status": "❓ Дата не знайдена",
                "exp_date": None, "doc_type": last_result.get("doc_type") if last_result else None,
                "is_valid": False, "error": "date_not_found"
            }

        exp_date_obj = dateparser.parse(last_result["exp_date"])
        today = datetime.now()
        days_left = (exp_date_obj - today).days
        is_valid = exp_date_obj > today

        if is_valid:
            status = f"✅ Дійсний ще {days_left} дн."
        else:
            status = f"❌ Прострочений {abs(days_left)} дн. тому"

        return {
            "name":     client_name,
            "status":   status,
            "exp_date": last_result["exp_date"],
            "doc_type": last_result.get("doc_type", "—"),
            "country":  last_result.get("country", "—"),
            "source":   last_result.get("source"),
            "is_valid": is_valid,
            "days_left": days_left,
            "error":    None
        }


# ─────────────────────────────────────────────
#  ГЕНЕРАЦІЯ EXCEL ЗВІТУ
# ─────────────────────────────────────────────

def _generate_excel_report(results: list[dict], output_path: str) -> bool:
    """Генерує Excel звіт з результатами аналізу."""
    try:
        import pandas as pd
        rows = []
        for r in sorted(results, key=lambda x: (not x["is_valid"], x["name"])):
            rows.append({
                "Клієнт":       r["name"],
                "Статус":       r["status"],
                "Дійсний до":   r.get("exp_date", "—"),
                "Тип документа": r.get("doc_type", "—"),
                "Країна":       r.get("country", "—"),
                "Розпізнав":    r.get("source", "—"),
            })
        df = pd.DataFrame(rows)
        df.to_excel(output_path, index=False)
        return True
    except Exception as e:
        logger.error("Помилка генерації Excel: %s", e)
        return False


# ─────────────────────────────────────────────
#  ЗАХИСТ ZIP
# ─────────────────────────────────────────────


async def _send_dir_as_zip_chunks(
    context,
    chat_id: int,
    source_dir: str,
    label: str,
    timestamp: str,
) -> int:
    """Архівує вміст папки та надсилає через Telegram частинами по CHUNK_ZIP_MB МБ.
    Повертає кількість відправлених частин (0 якщо папка порожня)."""

    # Збираємо всі файли з розмірами
    entries: list[tuple[str, str, int]] = []
    for root, _, files in os.walk(source_dir):
        for fname in files:
            full = os.path.join(root, fname)
            arc  = os.path.relpath(full, source_dir)
            entries.append((full, arc, os.path.getsize(full)))

    if not entries:
        return 0

    chunk_bytes = CHUNK_ZIP_MB * 1024 * 1024

    # Розбиваємо на частини за розміром
    chunks: list[list[tuple[str, str]]] = []
    cur: list[tuple[str, str]] = []
    cur_size = 0
    for full, arc, size in entries:
        if cur_size + size > chunk_bytes and cur:
            chunks.append(cur)
            cur = []
            cur_size = 0
        cur.append((full, arc))
        cur_size += size
    if cur:
        chunks.append(cur)

    total_parts = len(chunks)

    for i, chunk in enumerate(chunks, 1):
        suffix   = f"_part{i}of{total_parts}" if total_parts > 1 else ""
        zip_name = f"{label}_{timestamp}{suffix}.zip"
        zip_path = os.path.join(RESULTS_DIR, zip_name)

        try:
            await asyncio.to_thread(
                _write_zip_sync, zip_path, chunk
            )
            caption = f"📁 {label}"
            if total_parts > 1:
                caption += f"\nЧастина {i}/{total_parts}"

            sent = False
            last_err: Exception | None = None
            for attempt in range(1, 4):  # 3 спроби
                try:
                    with open(zip_path, 'rb') as f:
                        await context.bot.send_document(
                            chat_id, f,
                            filename=zip_name,
                            caption=caption,
                            write_timeout=300,
                            read_timeout=60,
                            connect_timeout=30,
                        )
                    sent = True
                    break
                except Exception as e:
                    last_err = e
                    if attempt < 3:
                        await asyncio.sleep(5 * attempt)  # 5s, 10s
            if not sent:
                raise RuntimeError(
                    f"Не вдалося надіслати частину {i}/{total_parts} після 3 спроб: {last_err}"
                )
        finally:
            try:
                os.remove(zip_path)
            except Exception:
                pass

    return total_parts


def _write_zip_sync(zip_path: str, entries: list[tuple[str, str]]) -> None:
    """Синхронна запис ZIP — виконується в окремому потоці."""
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for full, arc in entries:
            zf.write(full, arc)


# ─────────────────────────────────────────────
#  AWS S3 UPLOAD
# ─────────────────────────────────────────────

def _zip_session_sync(session_dir: str, zip_path: str) -> None:
    """Архівує всю сесійну папку у zip_path (синхронно)."""
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(session_dir):
            for fname in files:
                full = os.path.join(root, fname)
                arc  = os.path.relpath(full, session_dir)
                zf.write(full, arc)


def _upload_to_s3_sync(zip_path: str, s3_key: str, expires_in: int = 604800) -> str:
    """Завантажує файл у S3 та повертає presigned URL.
    expires_in — секунди (за замовчуванням 7 днів = 604800).
    Виконується у окремому потоці."""
    if not _S3_BUCKET or _S3_BUCKET == "your-bucket-name-here":
        raise ValueError(
            "AWS_S3_BUCKET_NAME не налаштований у token.env.\n"
            "Створіть bucket на https://s3.console.aws.amazon.com/s3 "
            "та вкажіть його назву."
        )

    # ── Lifecycle policy: автовидалення через 30 днів ──
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=_S3_BUCKET,
            LifecycleConfiguration={"Rules": [{
                "ID":     "auto-delete-ai-results",
                "Filter": {"Prefix": "ai-results/"},
                "Status": "Enabled",
                "Expiration": {"Days": 30},
            }]}
        )
    except Exception as lc_err:
        logger.debug("Lifecycle config skip: %s", lc_err)

    # Завантажуємо файл
    s3_client.upload_file(
        zip_path, _S3_BUCKET, s3_key,
        ExtraArgs={"ContentType": "application/zip"},
    )

    # Presigned URL
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": _S3_BUCKET, "Key": s3_key},
        ExpiresIn=expires_in,
    )
    return url


async def _upload_session_to_s3(
    session_dir: str, session_name: str, expires_in: int = 604800
) -> str:
    """Упаковує сесійну папку у ZIP та вивантажує на AWS S3.
    expires_in — секунди дії presigned URL (за замовчуванням 7 днів)."""
    zip_name = f"{session_name}.zip"
    zip_path = os.path.join(RESULTS_DIR, zip_name)
    s3_key   = f"ai-results/{zip_name}"

    try:
        await asyncio.to_thread(_zip_session_sync, session_dir, zip_path)
        url = await asyncio.to_thread(_upload_to_s3_sync, zip_path, s3_key, expires_in)
        return url
    finally:
        try:
            os.remove(zip_path)
        except Exception:
            pass


def _save_session_sync(
    valid_dir: str, invalid_dir: str, unknown_dir: str,
    session_dir: str, results: list[dict], excel_name: str,
    manual_dir: str | None = None,
) -> None:
    """Зберігає відсортовані папки та Excel у сесійну директорію на Desktop.
    Виконується в окремому потоці щоб не блокувати event loop."""
    import pandas as pd

    os.makedirs(session_dir, exist_ok=True)

    # Копіюємо категорії (4-та — нім. права без терміну — лише якщо передана й непорожня)
    cats = [
        (valid_dir,   "✅ Придатні"),
        (invalid_dir, "❌ Не_придатні"),
        (unknown_dir, "❓ Невизначені"),
    ]
    if manual_dir:
        cats.append((manual_dir, "🇩🇪 Перевірити_вручну"))
    for src, name in cats:
        dest = os.path.join(session_dir, name)
        if os.path.exists(src) and os.listdir(src):
            shutil.copytree(src, dest)
        else:
            os.makedirs(dest, exist_ok=True)

    # Excel звіт
    excel_path = os.path.join(session_dir, excel_name)
    try:
        rows = []
        for r in sorted(results, key=lambda x: (not x.get("is_valid", False), x.get("name", ""))):
            rows.append({
                "Клієнт":        r.get("name", "—"),
                "Статус":        r.get("status", "—"),
                "Дійсний до":    r.get("exp_date", "—"),
                "Тип документа": r.get("doc_type", "—"),
                "Країна":        r.get("country", "—"),
                "Розпізнав":     r.get("source", "—"),
            })
        pd.DataFrame(rows).to_excel(excel_path, index=False)
    except Exception as e:
        logger.error("Помилка збереження Excel у сесію: %s", e)


# ─────────────────────────────────────────────
#  CALLBACK: ВИБІР СПОСОБУ ДОСТАВКИ
# ─────────────────────────────────────────────

def _find_session_dir(code: str) -> str | None:
    """Знаходить папку сесії по callback_code (DDMMYYYYHHmm).
    Відновлює префікс "ДД.ММ.РРРР.ГГ:ХХ" і шукає в LOCAL_RESULTS_DIR."""
    if len(code) != 12:
        return None
    prefix = f"{code[0:2]}.{code[2:4]}.{code[4:8]}.{code[8:10]}.{code[10:12]}"
    for name in os.listdir(LOCAL_RESULTS_DIR):
        if name.startswith(prefix):
            full = os.path.join(LOCAL_RESULTS_DIR, name)
            if os.path.isdir(full):
                return full
    return None


@require_auth
async def handle_delivery_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє вибір способу доставки результатів після аналізу."""
    query = update.callback_query
    if not query:
        return
    await query.answer()

    raw_data: str = query.data or ""

    # Витягуємо callback_code (12 цифр: DDMMYYYYHHmm)
    m = re.match(r"deliver_(tg|s3|s3d|ch|done)_(\d{12})(?:_(\d+))?", raw_data)
    if not m:
        await query.edit_message_text("❌ Невідомий callback.")
        return

    action, code = m.group(1), m.group(2)
    extra = m.group(3)  # for s3d: number of days

    # Знаходимо папку сесії: спочатку в user_data, потім скануємо диск
    user_data     = context.user_data or {}
    session_dir: str = user_data.get(f"sess_{code}") or _find_session_dir(code) or ""

    if action == "done":
        await query.edit_message_text(
            f"✅ Файли збережено на робочому столі:\n`{session_dir}`",
            parse_mode="Markdown"
        )
        return

    # action == "s3" — показуємо меню вибору терміну дії посилання
    if action == "s3":
        expiry_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 1 день",   callback_data=f"deliver_s3d_{code}_1")],
            [InlineKeyboardButton("📅 7 днів",   callback_data=f"deliver_s3d_{code}_7")],
            [InlineKeyboardButton("📅 30 днів",  callback_data=f"deliver_s3d_{code}_30")],
            [InlineKeyboardButton("🔙 Назад",    callback_data=f"deliver_done_{code}")],
        ])
        await query.edit_message_text(
            "☁️ *AWS S3 — оберіть термін дії посилання:*",
            reply_markup=expiry_kb,
            parse_mode="Markdown",
        )
        return

    # action == "s3d" — вивантажуємо з вибраним терміном
    if action == "s3d":
        days = int(extra or "7")

        # Знаходимо папку сесії
        session_name_s3 = os.path.basename(session_dir) if session_dir else code
        await query.edit_message_text(
            f"☁️ Вивантажую архів на AWS S3 (термін: {days} дн.)...\n"
            f"Зазвичай займає 1–3 хвилини залежно від розміру."
        )

        msg_s3  = query.message
        chat_id_s3 = msg_s3.chat_id if msg_s3 else None
        if not chat_id_s3:
            return

        if not session_dir or not os.path.isdir(session_dir):
            await context.bot.send_message(
                chat_id_s3,
                "❌ Папку сесії не знайдено.\nМожливо файли було видалено вручну."
            )
            return

        try:
            presigned_url = await _upload_session_to_s3(
                session_dir, session_name_s3, expires_in=days * 86400
            )
            await context.bot.send_message(
                chat_id_s3,
                f"✅ Архів вивантажено на AWS S3!\n\n"
                f"🔗 *Посилання (дійсне {days} дн.):*\n{presigned_url}\n\n"
                f"📂 Файли також на столі:\n`{session_dir}`",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.error("Помилка S3 upload: %s", e)
            await context.bot.send_message(
                chat_id_s3,
                f"❌ Помилка вивантаження на S3:\n`{e}`\n\n"
                f"💡 Перевірте `AWS_S3_BUCKET_NAME` у token.env.",
                parse_mode="Markdown",
            )
        return

    # action == "ch" — надсилаємо в Telegram канал/групу
    if action == "ch":
        if not _TG_RESULTS_CHANNEL:
            await query.edit_message_text("❌ TG_RESULTS_CHANNEL_ID не налаштований у token.env.")
            return

        if not session_dir or not os.path.isdir(session_dir):
            await query.edit_message_text(
                "❌ Папку сесії не знайдено.\nМожливо файли було видалено вручну."
            )
            return

        await query.edit_message_text("📢 Надсилаю результати в канал/групу...")

        msg_ch  = query.message
        chat_id_ch: int = msg_ch.chat_id if msg_ch else 0
        if not chat_id_ch:
            return

        try:
            channel_id: int | str = int(_TG_RESULTS_CHANNEL)
        except ValueError:
            channel_id = _TG_RESULTS_CHANNEL

        label_suffix = code
        ch_parts = 0
        try:
            # Надсилаємо Excel
            for fname in os.listdir(session_dir):
                if fname.endswith(".xlsx"):
                    excel_path = os.path.join(session_dir, fname)
                    with open(excel_path, 'rb') as f:
                        await context.bot.send_document(
                            channel_id, f,
                            filename=fname,
                            caption=f"📊 Звіт: {os.path.basename(session_dir)}"
                        )
                    break
            # Надсилаємо ZIP по категоріях
            for cat_folder, cat_label in [
                ("✅ Придатні",    f"✅Придатні_{label_suffix}"),
                ("❌ Не_придатні", f"❌Не_придатні_{label_suffix}"),
                ("❓ Невизначені", f"❓Невизначені_{label_suffix}"),
                ("🇩🇪 Перевірити_вручну", f"🇩🇪Перевірити_вручну_{label_suffix}"),
            ]:
                cat_path = os.path.join(session_dir, cat_folder)
                if os.path.isdir(cat_path):
                    parts = await _send_dir_as_zip_chunks(
                        context, channel_id, cat_path, cat_label, label_suffix
                    )
                    ch_parts += parts
            await context.bot.send_message(
                chat_id_ch,
                f"✅ Результати надіслано в канал ({ch_parts} архів(ів)).",
            )
        except Exception as e:
            logger.error("Помилка надсилання в канал: %s", e)
            await context.bot.send_message(
                chat_id_ch,
                f"❌ Помилка надсилання в канал:\n`{e}`",
                parse_mode="Markdown",
            )
        return

    # action == "tg" — надсилаємо частинами
    if not session_dir or not os.path.isdir(session_dir):
        await query.edit_message_text(
            "❌ Папку сесії не знайдено.\n"
            "Можливо файли було видалено вручну."
        )
        return

    await query.edit_message_text("📤 Надсилаю в Telegram частинами...")

    msg      = query.message
    chat_id  = msg.chat_id if msg else None
    if not chat_id:
        return
    total_parts = 0
    # Використовуємо code як суфікс для імен ZIP-частин
    label_suffix = code

    for cat_folder, cat_label in [
        ("✅ Придатні",     f"✅Придатні_{label_suffix}"),
        ("❌ Не_придатні",  f"❌Не_придатні_{label_suffix}"),
        ("❓ Невизначені",  f"❓Невизначені_{label_suffix}"),
        ("🇩🇪 Перевірити_вручну", f"🇩🇪Перевірити_вручну_{label_suffix}"),
    ]:
        cat_path = os.path.join(session_dir, cat_folder)
        if os.path.isdir(cat_path):
            parts = await _send_dir_as_zip_chunks(context, chat_id, cat_path, cat_label, label_suffix)
            total_parts += parts

    if total_parts == 0:
        await context.bot.send_message(chat_id, "⚠️ Не знайдено файлів для відправки.")
    else:
        await context.bot.send_message(
            chat_id,
            f"✅ Надіслано {total_parts} архів(ів).\n"
            f"📂 Файли також залишаються на столі:\n`{session_dir}`",
            parse_mode="Markdown"
        )


@require_auth
async def cancel_analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє натискання кнопки 'Скасувати аналіз'."""
    query = update.callback_query
    if not query:
        return
    try:
        await query.answer("⛔ Скасування...")
    except Exception:
        pass  # query expired — не критично

    raw_data: str = query.data or ""
    m = re.match(r"cancel_analysis_(\d+)", raw_data)
    if not m:
        return

    target_chat_id = int(m.group(1))
    ev = _cancel_events.get(target_chat_id)
    if ev and not ev.is_set():
        ev.set()
        try:
            await query.edit_message_text(
                "⛔ *Аналіз скасовано.*\n"
                "Вже оброблені документи будуть збережені.",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    else:
        try:
            await query.answer("Аналіз вже завершено або скасовано.", show_alert=True)
        except Exception:
            pass


@require_auth
async def confirm_analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Натискання «▶️ Почати аналіз» / «❌ Скасувати» після preview ZIP.
    Витягує token з callback_data, з context.bot_data[token] забирає
    zip_path+work_dir+status_msg і запускає _enqueue_analysis (або чистить).
    """
    query = update.callback_query
    if not query or not query.data:
        return
    try:
        await query.answer()
    except Exception:
        pass

    raw = query.data
    pending = context.bot_data.get("pending_analysis", {})

    if raw.startswith("cancelconfirm_"):
        token = raw[len("cancelconfirm_"):]
        data = pending.pop(token, None)
        if data:
            shutil.rmtree(data.get("work_dir", ""), ignore_errors=True)
        try:
            await query.edit_message_text(
                "❌ Аналіз скасовано. Нічого не списано."
            )
        except Exception:
            pass
        return

    if not raw.startswith("confirm_analysis_"):
        return
    # Формат: confirm_analysis_{token}_tx0 (локально) | _tx1 (з Textract)
    token_part = raw[len("confirm_analysis_"):]
    use_textract = token_part.endswith("_tx1")
    token = token_part.rsplit("_tx", 1)[0]
    data = pending.pop(token, None)
    if not data:
        try:
            await query.edit_message_text(
                "⌛ Сесія підтвердження прострочена або вже запущена. "
                "Надішли архів заново."
            )
        except Exception:
            pass
        return

    # Перевірка що це той самий юзер що відкрив preview
    if update.effective_user and data.get("user_id") and update.effective_user.id != data["user_id"]:
        try:
            await query.answer("Це не твій архів 😉", show_alert=True)
        except Exception:
            pass
        return

    zip_path = data["zip_path"]
    work_dir = data["work_dir"]
    rename_sides = bool(data.get("rename_sides", True))

    # Відновлюємо «status_msg» інтерфейс через chat_id/message_id
    class _MsgProxy:
        def __init__(self, bot, chat_id, msg_id):
            self._bot = bot
            self.chat_id = chat_id
            self.message_id = msg_id
        async def edit_text(self, text, **kw):
            try:
                return await self._bot.edit_message_text(
                    chat_id=self.chat_id, message_id=self.message_id, text=text, **kw
                )
            except Exception:
                pass

    status_msg = _MsgProxy(context.bot, data["status_msg_chat_id"], data["status_msg_id"])
    mode_txt = "☁️ з Textract" if use_textract else "⚡ локально"
    sides_txt = "🏷 front/back" if rename_sides else "без підпису сторін"
    await status_msg.edit_text(f"✅ Підтверджено ({mode_txt}, {sides_txt}). Ставлю в чергу аналізу...")
    await _enqueue_analysis(update, context, zip_path, work_dir, status_msg,
                            use_textract=use_textract, rename_sides=rename_sides)


@require_auth
async def cmd_analysis_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /analysislogs — показує лог аналізів (тільки для адміна)."""
    if not update.message:
        return

    from config import ADMIN_ID
    from database import get_doc_analysis_logs
    user = update.effective_user
    if not user or user.id != ADMIN_ID:
        await update.message.reply_text("❌ Команда доступна тільки адміністратору.")
        return

    logs = await asyncio.to_thread(get_doc_analysis_logs, 15)
    if not logs:
        await update.message.reply_text("📋 Логів аналізу ще немає.")
        return

    lines = ["📋 *Останні аналізи документів:*\n"]
    for i, row in enumerate(logs, 1):
        dt = str(row.get("started_at", "—"))[:16]
        lines.append(
            f"{i}. `{dt}` — @{row.get('username','?')}\n"
            f"   📊 {row.get('total_docs',0)} doc | "
            f"✅{row.get('valid_count',0)} "
            f"❌{row.get('invalid_count',0)} "
            f"❓{row.get('unknown_count',0)} | "
            f"⏱{row.get('duration_sec',0)}с\n"
            f"   `{row.get('session_name','')}`\n"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown"
    )


def _validate_zip(zip_path: str) -> None:
    """Перевіряє ZIP *перед* розпакуванням.
    Захищає від:
      1. Занадто великого архіву на диску
      2. ZIP Bomb — підроблені метадані (file_size=0, реальний розмір ГБ)
      3. ZIP Slip — шляхи типу ../../etc/passwd виходять за межі extract_dir
    Кидає ValueError при будь-якій порушенні.
    """
    # ── 1. Розмір архіву на диску ──
    file_size_mb = os.path.getsize(zip_path) / 1024 / 1024
    if file_size_mb > MAX_ZIP_SIZE_MB:
        raise ValueError(
            f"ZIP занадто великий: {file_size_mb:.1f} MB (ліміт {MAX_ZIP_SIZE_MB} MB)"
        )

    with zipfile.ZipFile(zip_path, 'r') as zf:
        infos = zf.infolist()

        # ── 2. Підозрілий коефіцієнт стиснення (ZIP Bomb через метадані) ──
        total_compressed   = sum(i.compress_size for i in infos)
        total_uncompressed = sum(i.file_size      for i in infos)
        if total_compressed > 0:
            ratio = total_uncompressed / total_compressed
            if ratio > 100:          # стиснення більше 100:1 — підозріло
                raise ValueError(
                    f"Підозріло висока ступінь стиснення ({ratio:.0f}:1). "
                    f"Можливий ZIP Bomb."
                )

        # ── 3. Сумарний розпакований розмір за метаданими ──
        total_mb = total_uncompressed / 1024 / 1024
        if total_mb > MAX_ZIP_UNCOMPRESSED_MB:
            raise ValueError(
                f"Розпакований розмір {total_mb:.1f} MB > ліміту {MAX_ZIP_UNCOMPRESSED_MB} MB"
            )

        # ── 4. ZIP Slip — перевірка кожного шляху ──
        for member in infos:
            norm = os.path.normpath(member.filename)
            if norm.startswith("..") or os.path.isabs(norm):
                raise ValueError(
                    f"Небезпечний шлях у ZIP: '{member.filename}'. "
                    f"ZIP Slip атака відхилена."
                )


def _safe_extract_zip(zip_path: str, extract_dir: str) -> None:
    """Безпечне розпакування ZIP файлу.
    На відміну від zf.extractall():
      - Рахує РЕАЛЬНІ записані байти під час розпакування (не покладається на метадані)
      - Зупиняється негайно якщо реальний розмір перевищує ліміт
      - Повторно перевіряє кожен шлях (ZIP Slip)
    Виконується в окремому потоці через asyncio.to_thread().
    """
    max_bytes   = MAX_ZIP_UNCOMPRESSED_MB * 1024 * 1024
    written     = 0
    real_dir    = os.path.realpath(extract_dir)

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for member in zf.infolist():
            # ZIP Slip: фінальний реальний шлях має бути всередині extract_dir
            target = os.path.realpath(os.path.join(extract_dir, member.filename))
            if not target.startswith(real_dir + os.sep) and target != real_dir:
                raise ValueError(
                    f"ZIP Slip відхилено під час розпакування: '{member.filename}'"
                )

            if member.is_dir():
                os.makedirs(target, exist_ok=True)
                continue

            os.makedirs(os.path.dirname(target), exist_ok=True)

            # Записуємо по чанках, рахуємо реальний розмір
            with zf.open(member) as src, open(target, "wb") as dst:
                while True:
                    chunk = src.read(65536)   # 64 KB
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > max_bytes:
                        raise ValueError(
                            f"ZIP Bomb: реальний розмір перевищив "
                            f"{MAX_ZIP_UNCOMPRESSED_MB} MB під час розпакування. "
                            f"Розпакування зупинено."
                        )
                    dst.write(chunk)


_SEVENZIP_PATHS = (
    r"C:\Program Files\7-Zip\7z.exe",
    r"C:\Program Files (x86)\7-Zip\7z.exe",
)


def _find_7z():
    """Знаходить 7z.exe (бекенд для RAR/7z). None якщо 7-Zip не встановлено."""
    import shutil as _sh
    for p in _SEVENZIP_PATHS:
        if os.path.exists(p):
            return p
    return _sh.which("7z") or _sh.which("7z.exe")


def _detect_archive_format(path: str) -> str:
    """Формат архіву за magic-байтами: 'zip' | 'rar' | '7z' | 'unknown'."""
    try:
        with open(path, "rb") as f:
            head = f.read(8)
    except OSError:
        return "unknown"
    if head[:2] == b"PK":
        return "zip"
    if head[:4] == b"Rar!":
        return "rar"
    if head[:6] == b"7z\xbc\xaf\x27\x1c":
        return "7z"
    return "unknown"


def _extract_with_7z(archive_path: str, extract_dir: str) -> None:
    """Розпаковує RAR/7z через 7z.exe у extract_dir (ізольований per-session).

    7-Zip за замовчуванням ігнорує абсолютні та '..' шляхи — захист від
    path-traversal. Розпаковка йде в окрему сесійну теку, яка потім чиститься.
    """
    import subprocess
    seven = _find_7z()
    if not seven:
        raise RuntimeError("7z_not_installed")
    # x = extract with paths, -y = yes-to-all, -o<dir> = output (без пробілу),
    # -bso0/-bsp0 = тихо (без stdout/progress).
    cmd = [seven, "x", archive_path, f"-o{extract_dir}", "-y", "-bso0", "-bsp0"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        raise RuntimeError("7z_timeout")
    if proc.returncode != 0:
        logger.warning("7z rc=%s: %s", proc.returncode, (proc.stderr or "")[:300])
        raise RuntimeError(f"7z_failed_rc{proc.returncode}")


def _extract_archive(archive_path: str, extract_dir: str) -> None:
    """Універсальне розпакування: ZIP → safe-zip, RAR/7z → 7z.exe."""
    fmt = _detect_archive_format(archive_path)
    if fmt == "zip":
        _safe_extract_zip(archive_path, extract_dir)
    elif fmt in ("rar", "7z"):
        _extract_with_7z(archive_path, extract_dir)
    else:
        raise ValueError("Непідтримуваний формат архіву (тільки ZIP / RAR / 7z).")


def _extract_nested_zips(root: str, max_depth: int = 5) -> int:
    """Рекурсивно розпаковує вкладені .zip всередині root (архів в архіві).

    Кожен внутрішній zip розпаковується в папку поруч (ім'я = назва архіву
    без .zip), після чого сам zip-файл видаляється. Підтримує кілька рівнів
    вкладеності (до max_depth). Використовує ті самі перевірки безпеки, що й
    _safe_extract_zip (ZIP-Slip + ліміт реальних байтів на кожен архів).

    Повертає кількість успішно розпакованих вкладених архівів.
    """
    extracted = 0
    for _ in range(max_depth):
        inner_zips: list[str] = []
        for cur, _dirs, files in os.walk(root):
            for f in files:
                if f.lower().endswith(".zip"):
                    inner_zips.append(os.path.join(cur, f))
        if not inner_zips:
            break
        for zp in inner_zips:
            dest = os.path.splitext(zp)[0]
            # уникаємо колізії якщо така папка вже існує
            d, n = dest, 1
            while os.path.exists(d):
                n += 1
                d = f"{dest}_{n}"
            os.makedirs(d, exist_ok=True)
            try:
                _safe_extract_zip(zp, d)
                extracted += 1
            except Exception as e:
                logger.warning("Вкладений архів пропущено (%s): %s",
                               os.path.basename(zp), e)
            finally:
                try:
                    os.remove(zp)
                except OSError:
                    pass
    return extracted


def _folder_has_images_direct(path: str) -> bool:
    """True якщо папка БЕЗПОСЕРЕДНЬО містить хоча б одне зображення."""
    try:
        for f in os.listdir(path):
            if (f.lower().endswith(SUPPORTED_IMAGE_EXTS)
                    and os.path.isfile(os.path.join(path, f))):
                return True
    except OSError:
        return False
    return False


def _rename_sides_in_folder(folder: str) -> dict:
    """Перейменовує фото в папці клієнта на front.<ext> / back.<ext>.

    Безкоштовно (OCR detect_side). 'unknown' — лишається з оригінальною назвою.
    Колізії (2 фронти) → front.jpg, front_2.jpg. Двофазне перейменування
    (через тимчасові імена) щоб уникнути перезапису існуючих файлів.

    Повертає {'front': n, 'back': n, 'unknown': n} для статистики.
    """
    from analysis.doc_analyzer import detect_side

    stats = {"front": 0, "back": 0, "unknown": 0}
    try:
        files = [f for f in sorted(os.listdir(folder))
                 if f.lower().endswith(SUPPORTED_IMAGE_EXTS)
                 and os.path.isfile(os.path.join(folder, f))]
    except OSError:
        return stats

    # Фаза 0: визначаємо сторону кожного фото
    sides: list[str] = []
    for fn in files:
        path = os.path.join(folder, fn)
        try:
            with open(path, "rb") as fh:
                data = fh.read()
            sides.append(detect_side(data))
        except Exception as e:
            logger.debug("detect_side fail %s: %s", fn, e)
            sides.append("unknown")

    # Фаза 0.5: ЕЛІМІНАЦІЯ для пари «1 документ = 2 фото».
    # Якщо рівно 2 фото і одне впевнено визначено, а друге — unknown,
    # друге = протилежна сторона (front детектиться надійно через обличчя,
    # тож зворот добираємо за елімінацією). Це різко покращує back-детекцію.
    if len(files) == 2:
        s0, s1 = sides
        opposite = {"front": "back", "back": "front"}
        if s0 in opposite and s1 == "unknown":
            sides[1] = opposite[s0]
            logger.debug("Side elimination: %s → %s", files[1], sides[1])
        elif s1 in opposite and s0 == "unknown":
            sides[0] = opposite[s1]
            logger.debug("Side elimination: %s → %s", files[0], sides[0])

    # Фаза 1: переносимо визначені у тимчасові імена
    planned: list[tuple[str, str, str]] = []   # (tmp_path, side, ext)
    for i, (fn, side) in enumerate(zip(files, sides)):
        stats[side] = stats.get(side, 0) + 1
        if side == "unknown":
            continue
        path = os.path.join(folder, fn)
        ext = os.path.splitext(fn)[1].lower()
        tmp = os.path.join(folder, f".__side_{i}{ext}")
        try:
            os.replace(path, tmp)
            planned.append((tmp, side, ext))
        except OSError:
            pass

    # Фаза 2: фінальні імена front/back з захистом від колізій
    taken: set[str] = set()
    try:
        taken = {f.lower() for f in os.listdir(folder)
                 if not f.startswith(".__side_")}
    except OSError:
        pass
    for tmp, side, ext in planned:
        name = f"{side}{ext}"
        n = 2
        while name.lower() in taken:
            name = f"{side}_{n}{ext}"
            n += 1
        taken.add(name.lower())
        try:
            os.replace(tmp, os.path.join(folder, name))
        except OSError:
            pass
    return stats


def _discover_client_folders(root: str) -> list[tuple[str, str]]:
    """Знаходить папки клієнтів на БУДЬ-ЯКІЙ глибині вкладеності.

    Папка клієнта = директорія, що безпосередньо містить хоча б одне фото.
    Це покриває і плоский архів (root/Клієнт/фото), і вкладені архіви
    (root/drive-download-X/Клієнт/фото) після _extract_nested_zips.

    Імена-колізії з різних архівів дедуплікуються суфіксом (_2, _3…).
    Повертає список (client_name, abs_path), відсортований за іменем.
    """
    found: list[tuple[str, str]] = []
    used: dict[str, int] = {}
    for cur, dirs, _files in os.walk(root):
        if _folder_has_images_direct(cur):
            base = os.path.basename(cur.rstrip(os.sep)) or "client"
            if base in used:
                used[base] += 1
                name = f"{base}_{used[base]}"
            else:
                used[base] = 1
                name = base
            found.append((name, cur))
            dirs[:] = []   # не спускаємось глибше в папку клієнта
    return sorted(found, key=lambda x: x[0])


# ─────────────────────────────────────────────
#  ГОЛОВНИЙ ХЕНДЛЕР
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
#  GOOGLE DRIVE DOWNLOADER
# ─────────────────────────────────────────────

def _extract_gdrive_id(url: str) -> str | None:
    """Витягує file ID з різних форматів Google Drive посилань."""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'/folders/([a-zA-Z0-9_-]+)',
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


async def download_from_gdrive(gdrive_url: str, dest_path: str,
                                progress_cb=None) -> str:
    """Завантажує ZIP з Google Drive.Підтримує великі файли через bypass вірусного скану Google.Повертає шлях до збереженого файлу."""
    file_id = _extract_gdrive_id(gdrive_url)
    if not file_id:
        raise ValueError("Не вдалось витягнути ID файлу з посилання Google Drive")

    def _download():
        import time

        import requests

        session = requests.Session()
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
        }

        # Новий актуальний endpoint Google Drive (drive.usercontent.google.com)
        url = (
            f"https://drive.usercontent.google.com/download"
            f"?id={file_id}&export=download&confirm=t"
        )

        chunk_size = 1024 * 1024  # 1 MB
        max_retries = 5
        downloaded = 0

        for attempt in range(max_retries):
            try:
                req_headers = dict(headers)
                # Resume: якщо вже щось завантажили — продовжуємо з того місця
                if downloaded > 0:
                    req_headers['Range'] = f'bytes={downloaded}-'
                    logger.info("Google Drive: resume з %d МБ (спроба %d/%d)",
                                downloaded // (1024 * 1024), attempt + 1, max_retries)

                resp = session.get(url, headers=req_headers, stream=True, timeout=600)

                # 416 = Range Not Satisfiable — файл вже повністю завантажений
                if resp.status_code == 416:
                    return dest_path

                resp.raise_for_status()

                # Якщо Google повернув HTML (файл закритий або потрібен confirm-token)
                content_type = resp.headers.get('Content-Type', '')
                if 'text/html' in content_type and attempt == 0:
                    token = next(
                        (v for k, v in resp.cookies.items()
                         if k.startswith('download_warning')),
                        None
                    )
                    if token:
                        url = (f"https://drive.google.com/uc?"
                               f"export=download&id={file_id}&confirm={token}")
                        resp = session.get(url, headers=headers, stream=True, timeout=600)
                        resp.raise_for_status()
                    else:
                        raise ValueError(
                            "Google Drive повернув сторінку підтвердження замість файлу.\n"
                            "Переконайтесь що доступ до файлу встановлено: "
                            "'Усі хто має посилання → Переглядач'."
                        )

                # Загальний розмір (Content-Length або Content-Range)
                total_size = None
                cr = resp.headers.get('Content-Range', '')
                if cr and '/' in cr:
                    try:
                        total_size = int(cr.split('/')[-1])
                    except ValueError:
                        pass
                if total_size is None:
                    cl = resp.headers.get('Content-Length')
                    if cl:
                        try:
                            total_size = downloaded + int(cl)
                        except ValueError:
                            pass

                # Пишемо: append якщо resume, інакше overwrite
                mode = 'ab' if downloaded > 0 else 'wb'
                last_report = 0  # останній звіт (в МБ)
                with open(dest_path, mode) as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            # Звітуємо кожні 50 МБ
                            cur_mb = downloaded // (1024 * 1024)
                            if cur_mb - last_report >= 50:
                                last_report = cur_mb
                                if progress_cb and total_size:
                                    pct = downloaded * 100 // total_size
                                    progress_cb(downloaded, total_size, pct)
                                elif progress_cb:
                                    progress_cb(downloaded, 0, 0)

                # Фінальний звіт
                if progress_cb:
                    progress_cb(downloaded, downloaded, 100)

                # Успішно завантажили весь файл
                return dest_path

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.Timeout) as e:
                logger.warning("Google Drive: обрив на %d МБ: %s",
                               downloaded // (1024 * 1024), e)
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))  # 3, 6, 9, 12 сек
                else:
                    raise

        return dest_path

    return await asyncio.to_thread(_download)


@require_auth
async def handle_gdrive_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє Google Drive посилання надіслане в чат.Завантажує ZIP і передає в handle_zip_documents_from_path.

    ВАЖЛИВО: Аналіз запускається ТІЛЬКИ якщо юзер спочатку натиснув
    «📁 Перевірка фіз. доків» (prompt_for_zip виставляє awaiting_docs_until
    у context.user_data на 30 хв). Інакше випадково скинуте drive-посилання
    не тригерить аналіз.
    """
    if not update.message or not update.message.text:
        return

    url = update.message.text.strip()
    if 'drive.google.com' not in url:
        return

    # ── Gate: перевіряємо чи юзер у режимі очікування документів ──
    import time as _time
    awaiting_until = 0.0
    if context.user_data is not None:
        awaiting_until = float(context.user_data.get("awaiting_docs_until", 0) or 0)

    if awaiting_until < _time.time():
        await update.message.reply_text(
            "ℹ️ Щоб запустити аналіз документів — спочатку натисни кнопку "
            "«📁 Перевірка фіз. доків» у меню, а потім надішли посилання."
        )
        return

    # Споживаємо токен — один натиск кнопки = одна сесія. Якщо треба ще раз,
    # користувач повторно натисне кнопку.
    if context.user_data is not None:
        context.user_data.pop("awaiting_docs_until", None)

    status_msg = await update.message.reply_text(
        "🔗 Виявлено посилання Google Drive.\n"
        "⏳ Завантажую файл (може зайняти кілька хвилин для великих архівів)..."
    )

    work_dir = tempfile.mkdtemp()
    zip_path = os.path.join(work_dir, "gdrive.zip")

    try:
        # Прогрес завантаження — оновлюємо повідомлення в Telegram
        import threading
        _progress_state = {"text": "", "lock": threading.Lock()}

        def _on_progress(downloaded: int, total: int, pct: int):
            dl_mb = downloaded / (1024 * 1024)
            if total > 0:
                total_mb = total / (1024 * 1024)
                bar_len = 20
                filled = int(bar_len * pct / 100)
                bar = "▓" * filled + "░" * (bar_len - filled)
                txt = (
                    f"⏳ Завантаження з Google Drive...\n\n"
                    f"`[{bar}]` {pct}%\n"
                    f"📥 {dl_mb:.0f} / {total_mb:.0f} МБ"
                )
            else:
                txt = f"⏳ Завантаження з Google Drive...\n📥 {dl_mb:.0f} МБ завантажено"
            with _progress_state["lock"]:
                _progress_state["text"] = txt

        # Фонова задача оновлення повідомлення (щоб не блокувати потік)
        _progress_stop = asyncio.Event()

        async def _update_tg_progress():
            last_text = ""
            while not _progress_stop.is_set():
                await asyncio.sleep(3)
                with _progress_state["lock"]:
                    txt = _progress_state["text"]
                if txt and txt != last_text:
                    try:
                        await status_msg.edit_text(txt, parse_mode="Markdown")
                        last_text = txt
                    except Exception:
                        pass

        progress_task = asyncio.create_task(_update_tg_progress())

        await download_from_gdrive(url, zip_path, progress_cb=_on_progress)

        _progress_stop.set()
        await progress_task

        file_size_mb = os.path.getsize(zip_path) / 1024 / 1024
        await status_msg.edit_text(
            f"✅ Завантажено: {file_size_mb:.1f} МБ\n"
            f"🔎 Аналізую структуру архіву..."
        )

        # Preview + confirm — юзер бачить вартість до запуску OCR
        await _ask_confirm_analysis(update, context, zip_path, work_dir, status_msg)

    except Exception as e:
        logger.error("Помилка Google Drive: %s", e)
        await status_msg.edit_text(
            f"❌ Помилка завантаження: {e}\n\n"
            "💡 Переконайтесь що:\n"
            "• Файл відкритий для перегляду ('Усі хто має посилання')\n"
            "• Посилання на .zip файл, не на папку"
        )
        shutil.rmtree(work_dir, ignore_errors=True)


async def _process_zip_file(update: Update, context: ContextTypes.DEFAULT_TYPE,
                           zip_path: str, work_dir: str, status_msg) -> None:
    """Спільна логіка обробки ZIP — використовується і для Telegram, і для Google Drive."""
    extract_dir = os.path.join(work_dir, "extracted")
    valid_dir   = os.path.join(work_dir, "Придатні")
    invalid_dir = os.path.join(work_dir, "Не_придатні")
    unknown_dir = os.path.join(work_dir, "Невизначені")
    manual_dir  = os.path.join(work_dir, "Перевірити_вручну")   # нім. права без терміну

    for d in (extract_dir, valid_dir, invalid_dir, unknown_dir, manual_dir):
        os.makedirs(d)

    try:
        # ── Визначаємо формат архіву (ZIP / RAR / 7z) ──
        fmt = _detect_archive_format(zip_path)
        if fmt == "unknown":
            await status_msg.edit_text(
                "❌ Невідомий формат архіву.\nПідтримується: ZIP, RAR, 7z."
            )
            return
        if fmt in ("rar", "7z") and not _find_7z():
            await status_msg.edit_text(
                "❌ Архів RAR/7z, але 7-Zip не встановлено на сервері.\n"
                "Встанови 7-Zip або надішли ZIP."
            )
            return

        # ── ZIP захист: перевірка ДО розпакування (ZIP Bomb + ZIP Slip) ──
        if fmt == "zip":
            try:
                await asyncio.to_thread(_validate_zip, zip_path)
            except ValueError as e:
                await status_msg.edit_text(f"❌ Перевірка ZIP не пройшла:\n{e}")
                return

        # ── Безпечне розпакування (ZIP — контроль байтів; RAR/7z — через 7z) ──
        try:
            await asyncio.to_thread(_extract_archive, zip_path, extract_dir)
        except ValueError as e:
            await status_msg.edit_text(f"❌ Помилка розпакування:\n{e}")
            return
        except RuntimeError as e:
            await status_msg.edit_text(
                f"❌ Не вдалось розпакувати {fmt.upper()}-архів ({e}).\n"
                "Спробуй надіслати ZIP."
            )
            return

        # ── Вкладені архіви (архів в архіві) — розпаковуємо рекурсивно ──
        nested = await asyncio.to_thread(_extract_nested_zips, extract_dir)
        if nested:
            await status_msg.edit_text(
                f"📦 Розпаковано вкладених архівів: {nested}. Шукаю папки клієнтів..."
            )

        # ── Папки клієнтів на будь-якій глибині (плоский АБО вкладені архіви) ──
        client_folders = await asyncio.to_thread(_discover_client_folders, extract_dir)
        total = len(client_folders)

        # ── Діагностика: початок сесії ──
        from analysis.doc_analyzer import _diag, _diag_ctx
        _diag_ctx.client_id = ""
        _diag(f"{'#' * 80}")
        _diag(f"SESSION START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {total} clients")
        _diag(f"{'#' * 80}")

        # ── Скидаємо Textract circuit breaker для нової сесії ──
        # Якщо попередня сесія його відкрила (AWS лежав), а зараз AWS живий —
        # маємо шанс знов спробувати. Лічильник скине circuit breaker сам, якщо
        # перший виклик буде успішним.
        global _textract_consecutive_fails, _textract_disabled_for_session
        _textract_consecutive_fails = 0
        _textract_disabled_for_session = False

        if total == 0:
            await status_msg.edit_text("⚠️ В архіві не знайдено папок клієнтів.")
            return

        start_time = datetime.now()
        chat_id = update.effective_chat.id if update.effective_chat else None

        # ── Перевіряємо/реєструємо подію скасування ──
        if chat_id:
            _cancel_events[chat_id] = asyncio.Event()

        await status_msg.edit_text(
            f"🔍 Знайдено {total} клієнтів.\n"
            f"⚡ Запускаю аналіз ({MAX_CONCURRENT_API} паралельно)...\n"
            f"Textract → OpenAI Vision (запасний)"
        )

        # ── Паралельна обробка клієнтів з подвійним контролем паралельності ──
        # _task_gate  — обмежує скільки _process_one АКТИВНІ одночасно (пам'ять)
        # _api_semaphore — обмежує скільки з них РОБЛЯТЬ API виклики (Textract/OpenAI)
        #
        # Принцип: _task_gate = 2 × MAX_CONCURRENT_API
        #   → max (2×15)=30 корутин активні, 470 чекають на шлюзі
        #   → max 15 з них роблять API виклики
        #   → 15 "буферних" — готові одразу зайняти API слот як тільки він звільниться
        # Це запобігає вибуху пам'яті при 500-5000 клієнтів.
        _task_gate = asyncio.Semaphore(MAX_CONCURRENT_API * 2)

        completed_count = 0
        results_map: dict[int, dict] = {}
        source_counts: dict[str, int] = {}  # лічильник джерел: "Local OCR" → N, "AWS Textract" → N

        async def _process_one(idx: int, name: str, path: str) -> None:
            nonlocal completed_count
            # ── Шлюз: обмежуємо скільки задач активні одночасно ──
            async with _task_gate:
                # Перевіряємо скасування
                if chat_id and _cancel_events.get(chat_id, asyncio.Event()).is_set():
                    results_map[idx] = {
                        "name": name, "status": "⛔ Скасовано",
                        "exp_date": None, "doc_type": None,
                        "is_valid": False, "error": "cancelled"
                    }
                    completed_count += 1
                    return
                try:
                    result = await analyze_client_documents(name, path)
                except Exception as e:
                    logger.error("Помилка аналізу клієнта '%s': %s", name, e)
                    result = {
                        "name": name, "status": "❌ Помилка обробки",
                        "exp_date": None, "doc_type": None,
                        "is_valid": False, "error": "exception"
                    }
                results_map[idx] = result
                # Рахуємо джерело розпізнавання
                src = result.get("source", "?")
                # Спрощуємо назву для лічильника
                if "Кеш" in src:
                    src_key = "🗄 Кеш"
                elif "Local" in src:
                    src_key = "🖥 Локальний"
                elif "Textract" in src:
                    src_key = "☁️ Textract"
                elif "Vision" in src or "OpenAI" in src:
                    src_key = "🤖 OpenAI"
                else:
                    src_key = "❓ Інше"
                source_counts[src_key] = source_counts.get(src_key, 0) + 1
                completed_count += 1  # завжди рахуємо, навіть при помилці

        # Фоновий таск — оновлює прогрес кожні PROGRESS_UPDATE_SEC секунд
        _progress_stop = asyncio.Event()

        async def _progress_updater() -> None:
            while not _progress_stop.is_set():
                await asyncio.sleep(PROGRESS_UPDATE_SEC)
                if _progress_stop.is_set():
                    break
                done = completed_count
                elapsed = (datetime.now() - start_time).seconds
                speed = done / elapsed if elapsed > 0 else 0
                eta = int((total - done) / speed) if speed > 0 else 0
                bar = _get_progress_bar(done, total)
                cancel_kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "🛑 Скасувати аналіз",
                        callback_data=f"cancel_analysis_{chat_id}"
                    )
                ]])
                try:
                    # Статистика джерел
                    src_text = " | ".join(f"{k}: {v}" for k, v in sorted(source_counts.items()))
                    await status_msg.edit_text(
                        f"⚙️ Аналіз документів...\n"
                        f"{bar}\n"
                        f"✅ Оброблено: {done}/{total}\n"
                        f"⚡ Швидкість: {speed:.1f} кл/с\n"
                        f"⏳ Залишилось: ~{eta // 60}хв {eta % 60}с\n"
                        f"📊 {src_text}" if src_text else "",
                        reply_markup=cancel_kb,
                    )
                except Exception:
                    pass

        progress_task = asyncio.create_task(_progress_updater())

        # Запускаємо всі задачі
        all_tasks = [
            asyncio.create_task(_process_one(i, name, path))
            for i, (name, path) in enumerate(client_folders)
        ]

        # Чекаємо завершення АБО скасування
        cancel_event = _cancel_events.get(chat_id) if chat_id else None
        done_normally = True

        if cancel_event:
            # Перевіряємо кожну секунду чи не натиснули "Скасувати"
            while not all(t.done() for t in all_tasks):
                if cancel_event.is_set():
                    # Скасовуємо всі незавершені задачі
                    for t in all_tasks:
                        if not t.done():
                            t.cancel()
                    # Чекаємо завершення вже запущених (max 5 сек)
                    await asyncio.wait(all_tasks, timeout=5.0)
                    done_normally = False
                    break
                await asyncio.sleep(1.0)
        else:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        # Якщо все завершилось нормально, збираємо винятки
        if done_normally:
            for t in all_tasks:
                if t.done() and not t.cancelled():
                    try:
                        exc = t.exception()
                        if exc:
                            logger.error("Task exception: %s", exc)
                    except Exception:
                        pass

        _progress_stop.set()
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

        # Відновлюємо порядок результатів (fallback якщо якийсь індекс відсутній)
        cancelled_label = "⛔ Скасовано" if not done_normally else "❌ Не оброблено"
        results = [
            results_map.get(i, {
                "name": client_folders[i][0], "status": cancelled_label,
                "exp_date": None, "doc_type": None,
                "is_valid": False, "error": "cancelled" if not done_normally else "missing"
            })
            for i in range(total)
        ]

        elapsed_total = (datetime.now() - start_time).seconds

        # ── Діагностика: підсумок сесії ──
        from analysis.doc_analyzer import _diag, _diag_ctx
        _diag_ctx.client_id = ""
        _diag(f"{'#' * 80}")
        _diag(f"SESSION END: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
              f"{elapsed_total}s | {completed_count}/{total}")
        _diag(f"Sources: {source_counts}")
        for i in range(total):
            r = results_map.get(i)
            if r:
                name = r.get("name", client_folders[i][0])
                exp = r.get("exp_date", "—")
                src = r.get("source", "?")
                status = r.get("status", "?")
                _diag(f"  {name:30s} exp={exp or '—'!s:12s} src={src or '?'!s:25s} {status}")
        _diag(f"{'#' * 80}")

        # Розкладаємо по папках (+ перейменування front/back) — з прогресом,
        # бо для великих батчів ця фаза помітна (detect_side на кожному фото).
        valid_count = invalid_count = unknown_count = cancelled_count = manual_count = 0
        _sort_total = len(client_folders)
        for _si, ((client_name, client_path), r) in enumerate(zip(client_folders, results), 1):
            err = r.get("error", "")
            if err in ("cancelled", "missing"):
                # Скасовані/необроблені — в невизначені
                dest = os.path.join(unknown_dir, client_name)
                cancelled_count += 1
            elif err == "de_license_no_expiry":
                # Нім. права без друкованого терміну → окрема папка «ручна перевірка»
                dest = os.path.join(manual_dir, client_name)
                manual_count += 1
            elif err in ("no_image", "date_not_found"):
                dest = os.path.join(unknown_dir, client_name)
                unknown_count += 1
            elif r.get("is_valid", False):
                dest = os.path.join(valid_dir, client_name)
                valid_count += 1
            else:
                dest = os.path.join(invalid_dir, client_name)
                invalid_count += 1

            if os.path.exists(client_path):
                # ── Перейменування front/back перед переміщенням ──
                # Кожен документ у папці → front.<ext>/back.<ext> (unknown лишаємо).
                # Вмикається/вимикається користувачем на екрані підтвердження
                # (_RUN_RENAME_SIDES). ПРИ СКАСУВАННІ — пропускаємо (це лише
                # косметика), щоб швидко зберегти вже оброблене.
                _cancelled = bool(cancel_event and cancel_event.is_set())
                if _RUN_RENAME_SIDES and not _cancelled:
                    try:
                        await asyncio.to_thread(_rename_sides_in_folder, client_path)
                    except Exception as e:
                        logger.debug("rename sides fail %s: %s", client_name, e)
                shutil.move(client_path, dest)

            # Прогрес фази збереження кожні 25 клієнтів
            if _si % 25 == 0 or _si == _sort_total:
                _phase = "Розкладаю + front/back" if _RUN_RENAME_SIDES else "Розкладаю"
                try:
                    await status_msg.edit_text(f"💾 {_phase}: {_si}/{_sort_total}...")
                except Exception:
                    pass

        # Підсумок
        elapsed_min = elapsed_total // 60
        elapsed_sec = elapsed_total % 60
        processed = valid_count + invalid_count + unknown_count + manual_count
        _manual_line = f"🇩🇪 Права без терміну (перевірити): {manual_count}\n" if manual_count else ""
        if not done_normally:
            summary = (
                f"⛔ Аналіз скасовано!\n\n"
                f"✅ Дійсних: {valid_count}\n"
                f"❌ Прострочених: {invalid_count}\n"
                f"❓ Невизначених: {unknown_count}\n"
                f"{_manual_line}"
                f"⛔ Не оброблено: {cancelled_count}\n"
                f"📊 Оброблено: {processed}/{total}\n\n"
                f"⏱ Час: {elapsed_min}хв {elapsed_sec}с\n\n"
                f"💾 Результати оброблених документів збережено."
            )
        else:
            summary = (
                f"✅ AI Аналіз завершено!\n\n"
                f"✅ Дійсних: {valid_count}\n"
                f"❌ Прострочених: {invalid_count}\n"
                f"❓ Невизначених: {unknown_count}\n"
                f"{_manual_line}"
                f"📊 Всього: {total}\n\n"
                f"⏱ Час: {elapsed_min}хв {elapsed_sec}с"
            )
        await status_msg.edit_text(summary)

        # ── Excel звіт ──
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_name = f"Звіт_AI_Аналіз_{timestamp}.xlsx"

        # ── Назва сесії: "ДД.ММ.РРРР.ГГ:ХХ - N документів" ──
        dt_now        = datetime.now()
        dt_display    = dt_now.strftime("%d.%m.%Y.%H.%M")
        session_name  = f"{dt_display} - {total} документів"
        session_dir   = os.path.join(LOCAL_RESULTS_DIR, session_name)
        # callback_code = DDMMYYYYHHmm (12 цифр, унікально до хвилини)
        callback_code = dt_now.strftime("%d%m%Y%H%M")

        await status_msg.edit_text("💾 Зберігаю результати на робочий стіл...")
        await asyncio.to_thread(_save_session_sync,
                                valid_dir, invalid_dir, unknown_dir,
                                session_dir, results, excel_name, manual_dir)

        excel_path = os.path.join(session_dir, excel_name)
        has_excel  = os.path.exists(excel_path)
        logger.info("Сесія збережена: %s", session_dir)

        # ── Лог аналізу в БД ──
        try:
            from database import log_doc_analysis
            user = update.effective_user
            username = user.username or user.first_name if user else "unknown"
            await asyncio.to_thread(
                log_doc_analysis,
                chat_id or 0, username, session_name,
                total, valid_count, invalid_count, unknown_count,
                elapsed_total, start_time, datetime.now()
            )
        except Exception as e:
            logger.warning("Помилка запису лога аналізу: %s", e)

        if not chat_id:
            return

        # ── Завжди надсилаємо Excel через Telegram ──
        if has_excel:
            try:
                with open(excel_path, 'rb') as f:
                    await context.bot.send_document(
                        chat_id, f,
                        filename=excel_name,
                        caption="📊 Excel звіт з датами і типами документів"
                    )
            except Exception as e:
                logger.warning("Не вдалось надіслати Excel: %s", e)

        # ── Зберігаємо дані сесії для callback ──
        if context.user_data is not None:
            context.user_data[f"sess_{callback_code}"] = session_dir

        # ── Показуємо вибір способу доставки ──
        kb_rows = [
            [InlineKeyboardButton(
                "📨 Надіслати в Telegram частинами",
                callback_data=f"deliver_tg_{callback_code}"
            )],
            [InlineKeyboardButton(
                "☁️ Вивантажити на AWS S3",
                callback_data=f"deliver_s3_{callback_code}"
            )],
        ]
        if _TG_RESULTS_CHANNEL:
            kb_rows.append([InlineKeyboardButton(
                "📢 Надіслати в канал/групу",
                callback_data=f"deliver_ch_{callback_code}"
            )])
        kb_rows.append([InlineKeyboardButton(
            "✅ Файли вже на робочому столі — достатньо",
            callback_data=f"deliver_done_{callback_code}"
        )])
        keyboard = InlineKeyboardMarkup(kb_rows)
        await context.bot.send_message(
            chat_id,
            f"📂 Результати збережено:\n`{session_dir}`\n\n"
            f"Оберіть спосіб отримання файлів:",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        # Очищаємо подію скасування
        if chat_id:
            _cancel_events.pop(chat_id, None)

    except Exception as e:
        logger.error("Критична помилка handle_zip_documents: %s", e)
        try:
            await status_msg.edit_text(
                f"❌ Критична помилка: {e}\n\n"
                f"💾 Якщо аналіз вже завершився — перевір папку `results/` "
                f"або скористайся /myresults"
            )
        except Exception:
            pass
    finally:
        # Видаляємо тільки тимчасову папку розпакування
        # results/ НЕ чіпаємо — там зберігаються готові ZIP
        shutil.rmtree(work_dir, ignore_errors=True)


@require_auth
async def handle_zip_documents(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отримує ZIP до 50 МБ через Telegram і запускає аналіз."""
    if not update.message or not update.message.document:
        return
    doc = update.message.document
    if not (doc.file_name or "").lower().endswith('.zip'):
        return

    file_size_mb = (doc.file_size or 0) / 1024 / 1024
    if file_size_mb > MAX_ZIP_SIZE_MB_TELEGRAM:
        await update.message.reply_text(
            f"⚠️ Файл занадто великий для Telegram ({file_size_mb:.1f} МБ > {MAX_ZIP_SIZE_MB_TELEGRAM} МБ)\n\n"
            f"📎 **Для великих архівів:**\n"
            f"1. Завантаж ZIP на **Google Drive**\n"
            f"2. Відкрий доступ: _Усі хто має посилання → Переглядач_\n"
            f"3. Надішли посилання сюди в чат\n\n"
            f"Бот автоматично завантажить і проаналізує файл будь-якого розміру.",
            parse_mode="Markdown"
        )
        return

    status_msg = await update.message.reply_text("⏳ Завантажую архів з Telegram...")
    work_dir = tempfile.mkdtemp()
    zip_path = os.path.join(work_dir, "uploaded.zip")

    try:
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(zip_path)
        # Preview + confirm замість прямого enqueue — юзер підтверджує вартість
        await _ask_confirm_analysis(update, context, zip_path, work_dir, status_msg)
    except Exception as e:
        logger.error("Помилка завантаження з Telegram: %s", e)
        await status_msg.edit_text(f"❌ Помилка: {e}")
        shutil.rmtree(work_dir, ignore_errors=True)


@require_auth
async def cmd_myresults(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /myresults — показує список сесій з Desktop і дозволяє надіслати будь-яку."""
    if not update.message:
        return

    # Читаємо сесії з Desktop папки (формат: "ДД.ММ.РРРР.ГГ:ХХ - N документів")
    _sess_pattern = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})\.(\d{2})\.(\d{2})")
    sessions = sorted(
        [d for d in os.listdir(LOCAL_RESULTS_DIR)
         if _sess_pattern.match(d) and os.path.isdir(os.path.join(LOCAL_RESULTS_DIR, d))],
        reverse=True
    )

    if not sessions:
        await update.message.reply_text(
            f"📂 Збережених сесій не знайдено.\n"
            f"Папка: `{LOCAL_RESULTS_DIR}`\n\n"
            f"Результати з'являться після наступного аналізу.",
            parse_mode="Markdown"
        )
        return

    # Показуємо список з кнопками (до 5 останніх)
    lines = [f"📂 *Збережені сесії ({len(sessions)} шт.):*\n"]
    buttons = []

    for i, sess in enumerate(sessions[:5], 1):
        # callback_code з імені: DDMMYYYYHHmm
        cm = _sess_pattern.match(sess)
        if not cm:
            continue
        dd, mm, yyyy, hh, mi = cm.groups()
        callback_code = f"{dd}{mm}{yyyy}{hh}{mi}"

        lines.append(f"{i}. 📅 {sess}")
        buttons.append([InlineKeyboardButton(
            f"📨 Надіслати сесію {i}",
            callback_data=f"deliver_tg_{callback_code}"
        )])

    if len(sessions) > 5:
        lines.append(f"\n_...та ще {len(sessions) - 5} старіших сесій_")

    lines.append(f"\n📁 Всі файли на столі:\n`{LOCAL_RESULTS_DIR}`")
    lines.append(f"\n🧹 Сесії старіші {SESSION_KEEP_DAYS} днів видаляються автоматично.\n"
                 f"Або вручну: /cleanup")

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────────
#  ОЧИЩЕННЯ СТАРИХ СЕСІЙ
# ─────────────────────────────────────────────

def _cleanup_old_sessions_sync(keep_days: int, dry_run: bool = False) -> tuple[int, int]:
    """Видаляє сесійні папки старіші keep_days днів з LOCAL_RESULTS_DIR.
    Повертає (видалено, помилок).

    dry_run=True — НЕ видаляє, лише рахує скільки БУЛО б видалено.
    Виконується в окремому потоці (IO-bound).
    """
    _sess_pattern = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})\.(\d{2})\.(\d{2})")
    cutoff = datetime.now().timestamp() - keep_days * 86400
    removed = 0
    errors  = 0

    try:
        entries = os.listdir(LOCAL_RESULTS_DIR)
    except OSError:
        return 0, 1

    for name in entries:
        if not _sess_pattern.match(name):
            continue
        full_path = os.path.join(LOCAL_RESULTS_DIR, name)
        if not os.path.isdir(full_path):
            continue
        # Перевіряємо час останньої модифікації папки
        try:
            mtime = os.path.getmtime(full_path)
            if mtime < cutoff:
                if not dry_run:
                    shutil.rmtree(full_path)
                    logger.info("Cleanup: видалено стару сесію '%s'", name)
                removed += 1
        except Exception as e:
            logger.warning("Cleanup: помилка видалення '%s': %s", name, e)
            errors += 1

    return removed, errors


async def run_auto_cleanup() -> None:
    """Автоматичне очищення старих сесій — викликається при старті бота
    і може бути підключено до планувальника."""
    removed, errors = await asyncio.to_thread(_cleanup_old_sessions_sync, SESSION_KEEP_DAYS)
    if removed > 0 or errors > 0:
        logger.info(
            "Auto-cleanup: видалено %d сесій, помилок %d (keep_days=%d)",
            removed, errors, SESSION_KEEP_DAYS
        )


@require_auth
async def cmd_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /cleanup — ручне очищення старих сесій."""
    if not update.message:
        return

    # Парсимо аргументи: /cleanup [днів] [confirm]
    # Без confirm — dry-run (показує скільки БУЛО б видалено, нічого не чіпає).
    # З confirm — реально видаляє. Захист від випадкового тапу.
    args = (context.args or [])
    force = False
    if args and args[-1].lower() in ("confirm", "yes", "force"):
        force = True
        args = args[:-1]
    try:
        days = int(args[0]) if args else SESSION_KEEP_DAYS
        if days < 1:
            raise ValueError
    except (ValueError, IndexError):
        await update.message.reply_text(
            f"⚠️ Невірний аргумент. Використання:\n"
            f"`/cleanup [днів]` — показати що БУДЕ видалено (preview)\n"
            f"`/cleanup [днів] confirm` — реально видалити\n\n"
            f"За замовчуванням: {SESSION_KEEP_DAYS} днів",
            parse_mode="Markdown"
        )
        return

    if not force:
        # Dry-run — рахуємо скільки сесій підпадає, нічого не видаляємо
        would_remove, errors = await asyncio.to_thread(_cleanup_old_sessions_sync, days, True)
        if would_remove == 0:
            await update.message.reply_text(
                f"ℹ️ Сесій старіших {days} днів немає — нічого видаляти.",
            )
            return
        await update.message.reply_text(
            f"🔎 <b>Preview</b> — знайдено <code>{would_remove}</code> сесій старіших {days} днів.\n\n"
            f"Щоб реально видалити — напиши:\n"
            f"<code>/cleanup {days} confirm</code>",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        f"🧹 Видаляю сесії старіші {days} днів...",
    )

    removed, errors = await asyncio.to_thread(_cleanup_old_sessions_sync, days, False)

    if removed == 0 and errors == 0:
        await update.message.reply_text(
            f"✅ Нічого не знайдено для видалення\n"
            f"_(сесій старіших {days} днів немає)_",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"✅ Очищення завершено:\n"
            f"🗑 Видалено: *{removed}* сесій\n"
            f"{'⚠️ Помилок: ' + str(errors) if errors else ''}\n\n"
            f"📁 Залишилось: `{LOCAL_RESULTS_DIR}`",
            parse_mode="Markdown"
        )
