"""
handlers_analysis.py — Хендлери Telegram-бота для аналізу документів.

Команди:
  /checkdoc — аналіз документа на валідність (фото або файл)
  /checkdeps — перевірка встановлених залежностей OCR

Кнопка: 🔍 Перевірка документа
"""
from __future__ import annotations

import asyncio
import io
import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from analysis.doc_analyzer import analyze_document, format_report, check_dependencies

logger = logging.getLogger(__name__)

# ── Стани ──
ANALYSIS_WAIT_PHOTO = 50


# ── Команда /checkdoc ──────────────────────────────────────────────────

async def cmd_checkdoc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Початок аналізу — просить надіслати фото документа."""
    if not update.message and not update.callback_query:
        return ANALYSIS_WAIT_PHOTO

    text = (
        "🔍 **Аналіз документа на валідність**\n\n"
        "📋 **Що перевіряється:**\n"
        "• MRZ (Machine Readable Zone) — контрольні цифри\n"
        "• Дати — народження, закінчення, логічність\n"
        "• Формат — тип документа, код країни, стать\n"
        "• Крос-перевірка MRZ vs текст документа\n\n"
        "📷 **Надішліть фото документа** (паспорт або ID-карта)\n\n"
        "_Підтримуються: фото, зображення як файл (JPG/PNG)_\n"
        "_Для кращого результату фото має бути чітким та рівним_"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Скасувати", callback_data="analysis_exit")],
    ])

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text, parse_mode="Markdown", reply_markup=kb
        )
    elif update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

    return ANALYSIS_WAIT_PHOTO


async def handle_analysis_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє надіслане фото для аналізу."""
    if not update.message:
        return ANALYSIS_WAIT_PHOTO

    image_bytes: bytes | None = None

    # Фото
    if update.message.photo:
        photo_file = await update.message.photo[-1].get_file()
        data = await photo_file.download_as_bytearray()
        image_bytes = bytes(data)

    # Файл (зображення)
    elif update.message.document:
        doc = update.message.document
        mime = doc.mime_type or ""
        if mime.startswith("image/") or (doc.file_name or "").lower().endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp')):
            tg_file = await doc.get_file()
            data = await tg_file.download_as_bytearray()
            image_bytes = bytes(data)

    if not image_bytes:
        await update.message.reply_text(
            "❌ Не вдалося отримати зображення.\n"
            "Надішліть фото або файл зображення (JPG/PNG)."
        )
        return ANALYSIS_WAIT_PHOTO

    # Показуємо прогрес
    progress_msg = await update.message.reply_text(
        "⏳ **Аналізую документ...**\n\n"
        "🔍 Крок 1/4: Перевірка зображення...",
        parse_mode="Markdown"
    )

    try:
        # Оновлюємо прогрес
        async def update_progress(step: int, text: str) -> None:
            try:
                steps = [
                    "🔍 Крок 1/4: Перевірка зображення...",
                    "📖 Крок 2/4: Зчитування MRZ...",
                    "🔤 Крок 3/4: OCR розпізнавання тексту...",
                    "✅ Крок 4/4: Валідація та крос-перевірка...",
                ]
                bar = "▓" * (step + 1) + "░" * (4 - step - 1)
                await progress_msg.edit_text(
                    f"⏳ **Аналізую документ...**\n\n"
                    f"`[{bar}]` {step + 1}/4\n"
                    f"{steps[min(step, 3)]}",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

        await update_progress(0, "Зображення")

        # Запускаємо аналіз в окремому потоці (OCR повільний)
        result = await asyncio.to_thread(analyze_document, image_bytes, True)

        await update_progress(3, "Готово")

        # Форматуємо звіт
        report = format_report(result)

        # Telegram обмеження: 4096 символів
        if len(report) > 4000:
            # Розбиваємо на частини
            parts = _split_report(report, 3900)
            await progress_msg.edit_text(parts[0], parse_mode="Markdown")
            chat = update.effective_chat
            if chat:
                for part in parts[1:]:
                    await chat.send_message(part, parse_mode="Markdown")
        else:
            await progress_msg.edit_text(report, parse_mode="Markdown")

        # Кнопки після аналізу
        chat = update.effective_chat
        if chat:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Аналізувати ще", callback_data="analysis_again")],
                [InlineKeyboardButton("❌ Закрити", callback_data="analysis_exit")],
            ])
            await chat.send_message(
                "Надішліть ще фото для аналізу або закрийте меню:",
                reply_markup=kb
            )

    except Exception as e:
        logger.error("Помилка аналізу документа: %s", e, exc_info=True)
        await progress_msg.edit_text(
            f"❌ **Помилка аналізу:**\n`{e}`\n\n"
            "Спробуйте інше фото з кращою якістю.",
            parse_mode="Markdown"
        )

    return ANALYSIS_WAIT_PHOTO


async def handle_analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє callback-кнопки аналізу."""
    from telegram.ext import ConversationHandler

    query = update.callback_query
    if not query or not query.data:
        return ANALYSIS_WAIT_PHOTO

    await query.answer()

    if query.data == "analysis_exit":
        await query.edit_message_text("👋 Аналіз документів закрито.")
        return ConversationHandler.END

    if query.data == "analysis_again":
        return await cmd_checkdoc(update, context)

    return ANALYSIS_WAIT_PHOTO


# ── /checkdeps — перевірка залежностей ──────────────────────────────────

async def cmd_checkdeps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показує які OCR-бібліотеки встановлені."""
    if not update.message:
        return

    deps = check_dependencies()

    lines = ["🔧 **Залежності для аналізу документів:**\n"]
    dep_names = {
        "easyocr": "EasyOCR (OCR-рушій)",
        "passporteye": "PassportEye (MRZ-рідер)",
        "pillow": "Pillow (обробка зображень)",
        "numpy": "NumPy (масиви)",
    }

    all_ok = True
    for key, name in dep_names.items():
        ok = deps.get(key, False)
        icon = "✅" if ok else "❌"
        lines.append(f"  {icon} {name}")
        if not ok:
            all_ok = False

    lines.append("")

    if all_ok:
        lines.append("✅ Всі залежності встановлені — аналіз готовий до роботи!")
    else:
        lines.append("⚠️ **Деякі залежності відсутні.**\n")
        lines.append("Встановіть через pip:")
        if not deps.get("easyocr"):
            lines.append("  `pip install easyocr`")
        if not deps.get("passporteye"):
            lines.append("  `pip install passporteye`")
        if not deps.get("pillow"):
            lines.append("  `pip install Pillow`")
        if not deps.get("numpy"):
            lines.append("  `pip install numpy`")
        lines.append("")
        lines.append("_EasyOCR — обов'язковий. PassportEye — рекомендований (підвищує точність MRZ)._")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── Утиліти ─────────────────────────────────────────────────────────────

def _split_report(text: str, max_len: int) -> list[str]:
    """Розбиває довгий текст на частини по max_len символів."""
    parts: list[str] = []
    lines = text.split('\n')
    current: list[str] = []
    current_len = 0

    for line in lines:
        if current_len + len(line) + 1 > max_len and current:
            parts.append('\n'.join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1

    if current:
        parts.append('\n'.join(current))

    return parts


# ── ConversationHandler builder ─────────────────────────────────────────

def build_analysis_conversation():
    """Створює ConversationHandler для аналізу документів."""
    from telegram.ext import (
        ConversationHandler, CommandHandler, CallbackQueryHandler,
        MessageHandler, filters
    )

    return ConversationHandler(
        entry_points=[
            CommandHandler("checkdoc", cmd_checkdoc),
            MessageHandler(filters.Regex(r"^🔍 Перевірка документа$"), cmd_checkdoc),
        ],
        states={
            ANALYSIS_WAIT_PHOTO: [
                MessageHandler(filters.PHOTO, handle_analysis_photo),
                MessageHandler(filters.Document.IMAGE, handle_analysis_photo),
                CallbackQueryHandler(handle_analysis_callback,
                                     pattern=r"^analysis_(exit|again)$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(handle_analysis_callback, pattern=r"^analysis_exit$"),
            CommandHandler("checkdoc", cmd_checkdoc),
        ],
        allow_reentry=True,
        name="analysis_conversation",
    )
