"""
handlers/documents.py — Генерація сертифікатів (Certificate of Incorporation) з .docx-шаблону.

Потік:
  1. /newdoc (або кнопка «🪪 Документи») → інструкція + приклад формату TXT.
  2. Користувач надсилає TXT-файл з кількома компаніями.
  3. Бот парсить файл, для кожної компанії підставляє дані у .docx-шаблон
     (назва / CIN / адреса — з TXT; дві дати та підписант — генеруються автоматично),
     і повертає ZIP з готовими .docx.

Команди:
  /newdoc      — генератор сертифікатів (приймає TXT)
  /previewdoc  — приклад одного сертифіката (візуальна перевірка шаблону)
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from documents import pdf_convert
from documents.generator import (
    SAMPLE_TXT,
    get_template,
    list_templates,
    parse_companies_txt,
)
from handlers.admin import require_auth

logger = logging.getLogger(__name__)

# ── Стани ──
DOC_WAIT = 40

# Скільки документів максимум за один TXT
_MAX_COMPANIES = 500


# ── Хелпери ──────────────────────────────────────────────────────────────

def _active_template():
    """Повертає активний шаблон (india_incorporation або перший доступний)."""
    names = list_templates()
    if not names:
        return None
    if "india_incorporation" in names:
        return get_template("india_incorporation")
    return get_template(names[0])


def _decode_txt(raw: bytes) -> str:
    """Декодує TXT з типовими кодуваннями."""
    for enc in ("utf-8-sig", "utf-8", "cp1251", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


async def _render_sample(tpl):
    """Рендерить один демо-сертифікат у бажаному форматі → (bytes, filename).

    PDF, якщо доступний LibreOffice; інакше — .docx (fallback без помилки).
    """
    sample = parse_companies_txt(SAMPLE_TXT)[0]
    if pdf_convert.pdf_available():
        try:
            doc = await asyncio.to_thread(tpl.render_pdf, sample)
            return doc, "certificate_preview.pdf"
        except pdf_convert.PdfConversionError as e:
            logger.warning("Прев'ю: PDF недоступний, fallback у .docx: %s", e)
    doc = await asyncio.to_thread(tpl.render, sample)
    return doc, "certificate_preview.docx"


def _menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Приклад TXT формату", callback_data="doc_sample")],
        [InlineKeyboardButton("👁 Приклад сертифіката", callback_data="doc_preview")],
        [InlineKeyboardButton("❌ Закрити", callback_data="doc_close")],
    ])


def _intro_text(tpl) -> str:
    return (
        "🪪 **Генератор сертифікатів**\n"
        f"_{tpl.description}_\n\n"
        "Надішліть **TXT-файл** з компаніями. Формат — блоки, розділені порожнім рядком:\n\n"
        "```\n"
        "Company: SOME PRIVATE LIMITED\n"
        "CIN: U55101KL2025PTC095056\n"
        "Address: BUILDING NO..., Kerala, 695582-India\n"
        "\n"
        "Company: OTHER PRIVATE LIMITED\n"
        "CIN: U74999MH2022PTC123456\n"
        "Address: 12 MG ROAD, Mumbai, 400069-India\n"
        "```\n"
        "На виході — **ZIP** з готовими **PDF** (по одному на компанію).\n\n"
        "📌 З TXT беруться: **назва, CIN, адреса**.\n"
        "🤖 Автоматично: **дата інкорпорації**, **дата видачі** (строго пізніше) і **підписант**.\n\n"
        "💡 Можна надіслати **TXT-експорт із результатів пошуку** (кнопка TXT) "
        "напряму — формат `#N` з полями розпізнається автоматично, зайві поля "
        "(Статус, Штат, RoC, Посилання…) ігноруються."
    )


# ─────────────────────────────────────────────
#  Екран: меню / інструкція
# ─────────────────────────────────────────────

@require_auth
async def show_doc_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    tpl = _active_template()
    if tpl is None:
        text = (
            "🪪 **Генератор сертифікатів**\n\n"
            "⚠️ Шаблон не знайдено.\n"
            "Покладіть папку з `template.docx` і `config.json` у `documents/templates/`."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Закрити", callback_data="doc_close")]])
    else:
        text = _intro_text(tpl)
        kb = _menu_kb()

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    return DOC_WAIT


async def handle_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query or not query.data:
        return DOC_WAIT
    await query.answer()

    if query.data == "doc_close":
        await query.edit_message_text("👋 Генератор сертифікатів закрито.")
        return ConversationHandler.END

    chat = update.effective_chat

    if query.data == "doc_sample":
        if chat:
            await chat.send_document(
                document=SAMPLE_TXT.encode("utf-8"),
                filename="companies_example.txt",
                caption="📄 Приклад TXT. Заповніть своїми компаніями і надішліть назад.",
            )
        return DOC_WAIT

    if query.data == "doc_preview":
        tpl = _active_template()
        if tpl and chat:
            doc, filename = await _render_sample(tpl)
            await chat.send_document(
                document=doc,
                filename=filename,
                caption=(
                    "👁 Приклад сертифіката з демо-даними.\n"
                    "Дати й підписант згенеровані автоматично."
                ),
            )
        return DOC_WAIT

    return DOC_WAIT


# ─────────────────────────────────────────────
#  Обробка TXT-файлу → пакетна генерація
# ─────────────────────────────────────────────

@require_auth
async def handle_txt_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.document:
        return DOC_WAIT

    doc = update.message.document
    file_name = doc.file_name or "file"
    if not file_name.lower().endswith(".txt"):
        await update.message.reply_text(
            "❌ Потрібен файл `.txt`. Надішліть текстовий файл зі списком компаній.",
            parse_mode="Markdown",
        )
        return DOC_WAIT

    tpl = _active_template()
    if tpl is None:
        await update.message.reply_text("⚠️ Шаблон недоступний.")
        return ConversationHandler.END

    await update.message.reply_text(f"📥 Читаю `{file_name}`...", parse_mode="Markdown")

    tmp_path = None
    try:
        tg_file = await doc.get_file()
        tmp_path = os.path.join(tempfile.gettempdir(), f"doc_{doc.file_unique_id}.txt")
        await tg_file.download_to_drive(tmp_path)

        with open(tmp_path, "rb") as f:
            text = _decode_txt(f.read())

        companies = parse_companies_txt(text)
        if not companies:
            await update.message.reply_text(
                "⚠️ Не знайшов жодної компанії у файлі.\n"
                "Перевірте формат (кнопка «📄 Приклад TXT формату»).",
            )
            return DOC_WAIT

        if len(companies) > _MAX_COMPANIES:
            await update.message.reply_text(
                f"⚠️ Максимум {_MAX_COMPANIES} компаній за раз (у файлі {len(companies)})."
            )
            return DOC_WAIT

        want_pdf = pdf_convert.pdf_available()
        fmt = "pdf" if want_pdf else "docx"
        await update.message.reply_text(
            f"⏳ Генерую **{len(companies)}** сертифікатів ({fmt.upper()})...",
            parse_mode="Markdown",
        )

        note = ""
        try:
            zip_bytes = await asyncio.to_thread(tpl.render_zip, companies, fmt)
        except pdf_convert.PdfConversionError as e:
            # LibreOffice є, але конвертація зламалась — не втрачаємо роботу,
            # віддаємо .docx і чесно повідомляємо чому.
            logger.warning("PDF-конвертація не вдалась, fallback у .docx: %s", e)
            fmt = "docx"
            note = "\n⚠️ PDF-конвертація не вдалась — надсилаю у форматі DOCX."
            zip_bytes = await asyncio.to_thread(tpl.render_zip, companies, "docx")

        chat = update.effective_chat
        if chat:
            await chat.send_document(
                document=zip_bytes,
                filename=f"certificates_{len(companies)}_{fmt}.zip",
                caption=(
                    f"✅ Готово: **{len(companies)}** сертифікатів ({fmt.upper()}).{note}\n"
                    f"Натисніть /newdoc щоб згенерувати ще."
                ),
                parse_mode="Markdown",
            )
            logger.info("Згенеровано %d сертифікатів (%s) для чату %s",
                        len(companies), fmt, chat.id)

    except Exception as e:
        logger.error("Помилка генерації сертифікатів: %s", e)
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return ConversationHandler.END


# ─────────────────────────────────────────────
#  /previewdoc — приклад сертіфіката
# ─────────────────────────────────────────────

@require_auth
async def cmd_preview_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    tpl = _active_template()
    if tpl is None:
        await update.message.reply_text("⚠️ Шаблон не знайдено.")
        return

    await update.message.reply_text("⏳ Генерую приклад сертифіката...")
    try:
        doc, filename = await _render_sample(tpl)
        await update.message.reply_document(
            document=doc,
            filename=filename,
            caption=(
                f"👁 **{tpl.description}**\n"
                "Демо-дані; дати й підписант згенеровані автоматично."
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")


# ─────────────────────────────────────────────
#  ConversationHandler
# ─────────────────────────────────────────────

def build_doc_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            CommandHandler("newdoc", show_doc_menu),
            MessageHandler(filters.Regex(r"^🪪 Документи$"), show_doc_menu),
        ],
        states={
            DOC_WAIT: [
                CallbackQueryHandler(
                    handle_menu_button,
                    pattern=r"^(doc_sample|doc_preview|doc_close)$",
                ),
                MessageHandler(filters.Document.ALL, handle_txt_file),
            ],
        },
        fallbacks=[
            CommandHandler("newdoc", show_doc_menu),
            CallbackQueryHandler(handle_menu_button, pattern=r"^doc_close$"),
        ],
        allow_reentry=True,
        name="doc_conversation",
    )
