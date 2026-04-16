"""
handlers_documents.py — Меню генерації документів із PSD-шаблонів

Можливості:
  - Покрокове заповнення полів (прогрес-бар, пропуск, повернення)
  - Вставка фото з Telegram
  - Авто-MRZ (поля з auto: пропускаються, обчислюються при рендері)
  - Вибір формату: PNG / JPEG / PDF
  - Редагування окремого поля з екрану підтвердження
  - Збереження останніх даних (кнопка «Попередні дані»)
  - Batch-генерація з CSV / Excel → ZIP
  - Валідація полів (pattern, choices, length)
  - Лічильник символів для полів з обмеженням

Команди:
  /newdoc       — генератор документів
  /previewdoc   — превью координат шаблону
"""
from __future__ import annotations

import asyncio
import io
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes, ConversationHandler,
    CommandHandler, CallbackQueryHandler, MessageHandler, filters
)

from documents.generator import (
    get_template, list_templates, validate_field, DocumentGenerator
)
from documents.translit import transliterate_if_needed
from documents.random_person import generate_person

logger = logging.getLogger(__name__)

# ── Стани ConversationHandler ──
DOC_MENU    = 40
DOC_SELECT  = 41
DOC_FILL    = 42
DOC_CONFIRM = 43
DOC_BATCH   = 44

# ── Ключі context.user_data ──
_K_TPL       = "doc_tpl"
_K_FILLED    = "doc_filled"
_K_QUEUE     = "doc_queue"
_K_IDX       = "doc_idx"
_K_TOTAL     = "doc_total"
_K_EDIT_KEY  = "doc_edit_key"     # режим редагування одного поля
_K_FMT       = "doc_fmt"          # обраний формат

# Збережені дані зберігаються під ключем "doc_last_{tpl_name}"


# ── Хелпери ─────────────────────────────────────────────────────────────

def _ud(context: ContextTypes.DEFAULT_TYPE) -> dict:
    if context.user_data is None:
        context.user_data = {}  # type: ignore[assignment]
    return context.user_data  # type: ignore[return-value]


async def _send_text(update: Update, text: str,
                     reply_markup: InlineKeyboardMarkup | None = None) -> None:
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, parse_mode="Markdown", reply_markup=reply_markup)
            return
        except Exception:
            pass
    chat = update.effective_chat
    if chat:
        await chat.send_message(text, parse_mode="Markdown", reply_markup=reply_markup)


def _progress_bar(current: int, total: int, length: int = 10) -> str:
    filled = int(current / total * length) if total else 0
    bar = "▓" * filled + "░" * (length - filled)
    return f"`[{bar}] {current}/{total}`"


# ── Клавіатури ──────────────────────────────────────────────────────────

def _template_list_kb(include_back: bool = True) -> InlineKeyboardMarkup:
    names = list_templates()
    rows = []
    for name in names:
        gen = get_template(name)
        desc = gen.config.get("description", name) if gen else name
        rows.append([InlineKeyboardButton(desc, callback_data=f"docsel_{name}")])
    if include_back:
        rows.append([InlineKeyboardButton("🔙 Назад", callback_data="doc_back_menu"),
                     InlineKeyboardButton("❌ Вийти",  callback_data="doc_exit")])
    return InlineKeyboardMarkup(rows)


def _field_kb(is_first: bool, is_photo: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if not is_photo:
        rows.append([
            InlineKeyboardButton("⏭ Пропустити", callback_data="doc_skip"),
            InlineKeyboardButton("⏩ Пропустити всі", callback_data="doc_skip_all"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("⏭ Без фото", callback_data="doc_skip"),
        ])
    if not is_first:
        rows.append([InlineKeyboardButton("🔙 Попереднє", callback_data="doc_prev")])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="doc_exit")])
    return InlineKeyboardMarkup(rows)


def _confirm_kb(ud: dict, gen: DocumentGenerator) -> InlineKeyboardMarkup:
    """Клавіатура підтвердження з вибором формату."""
    rows = [
        # Формат
        [InlineKeyboardButton("🖼 PNG", callback_data="doc_gen_PNG"),
         InlineKeyboardButton("📷 JPEG", callback_data="doc_gen_JPEG"),
         InlineKeyboardButton("📄 PDF", callback_data="doc_gen_PDF")],
        # Дії
        [InlineKeyboardButton("✏️ Редагувати поле", callback_data="doc_edit_list")],
        [InlineKeyboardButton("🔄 Інший шаблон", callback_data="doc_back_select"),
         InlineKeyboardButton("❌ Вийти", callback_data="doc_exit")],
    ]
    return InlineKeyboardMarkup(rows)


def _edit_fields_kb(gen: DocumentGenerator) -> InlineKeyboardMarkup:
    """Кнопки для вибору поля для редагування."""
    input_fields = gen.get_input_fields()
    rows = []
    row: list[InlineKeyboardButton] = []
    for key, cfg in input_fields:
        label = cfg.get("label", key)
        # Скорочуємо для кнопки
        short = label[:18] + "…" if len(label) > 18 else label
        row.append(InlineKeyboardButton(short, callback_data=f"doc_ef_{key}"))
        if len(row) >= 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="doc_back_confirm")])
    return InlineKeyboardMarkup(rows)


# ── Робота з полями ─────────────────────────────────────────────────────

def _current_field_cfg(context: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, dict | None]:
    ud = _ud(context)
    tpl_name: str | None = ud.get(_K_TPL)
    if not tpl_name:
        return None, None
    gen = get_template(tpl_name)
    if not gen:
        return None, None
    queue: list = ud.get(_K_QUEUE, [])
    idx: int = ud.get(_K_IDX, 0)
    if idx >= len(queue):
        return None, None
    key = queue[idx]
    return key, gen.config["fields"].get(key, {})


def _build_summary(context: ContextTypes.DEFAULT_TYPE) -> str:
    ud = _ud(context)
    tpl_name: str | None = ud.get(_K_TPL)
    if not tpl_name:
        return ""
    gen = get_template(tpl_name)
    if not gen:
        return ""

    filled: dict = ud.get(_K_FILLED, {})
    lines = [f"📋 **{gen.config.get('description', tpl_name)}**\n"]

    for key, cfg in gen.config.get("fields", {}).items():
        if cfg.get("auto"):
            continue  # авто-поля не показуємо

        label = cfg.get("label", key)
        field_type = cfg.get("type", "text")

        if field_type == "photo":
            has_photo = isinstance(filled.get(key), (bytes, bytearray))
            lines.append(f"• {label}: {'📷 завантажено' if has_photo else '—'}")
        else:
            val = filled.get(key, cfg.get("default", "—"))
            if isinstance(val, str):
                display = val if len(val) <= 40 else val[:37] + "..."
            else:
                display = str(val)
            lines.append(f"• {label}: `{display}`")

    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ЕКРАН 1 — Головне меню генератора
# ─────────────────────────────────────────────

async def show_doc_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    names = list_templates()
    count = len(names)

    if count == 0:
        text = (
            "🪪 **Генератор документів**\n\n"
            "⚠️ Шаблони не знайдено.\n\n"
            "Покладіть папку з `background.png` і `config.json`\n"
            "у директорію `templates/` поруч із ботом."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Закрити", callback_data="doc_exit")]])
    else:
        template_lines = []
        for name in names:
            gen = get_template(name)
            if gen:
                desc = gen.config.get("description", name)
                input_count = len(gen.get_input_fields())
                auto_count = sum(1 for v in gen.config.get("fields", {}).values() if v.get("auto"))
                extra = f" + {auto_count} авто" if auto_count else ""
                template_lines.append(f"  {desc} — {input_count} полів{extra}")

        text = (
            "🪪 **Генератор документів**\n\n"
            "📌 **Можливості:**\n"
            "• Заповнення полів вручну або з файлу (batch)\n"
            "• Вставка фото у шаблон\n"
            "• Авто-генерація MRZ\n"
            "• Вивід у PNG / JPEG / PDF\n\n"
            f"📂 **Доступні шаблони [{count}]:**\n"
            + "\n".join(template_lines)
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 Обрати шаблон →", callback_data="doc_open_select")],
            [InlineKeyboardButton("❌ Закрити", callback_data="doc_exit")],
        ])

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)

    return DOC_MENU


# ─────────────────────────────────────────────
#  ЕКРАН 2 — Вибір шаблону
# ─────────────────────────────────────────────

async def show_template_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()

    names = list_templates()
    desc_lines = []
    for name in names:
        gen = get_template(name)
        if gen:
            desc = gen.config.get("description", name)
            fcount = len(gen.get_input_fields())
            desc_lines.append(f"**{desc}** — {fcount} полів")

    text = "📄 **Оберіть шаблон документа:**\n\n" + "\n".join(desc_lines)
    await _send_text(update, text, _template_list_kb(include_back=True))
    return DOC_SELECT


async def handle_template_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір шаблону — показує опції (ручне / batch / попередні)."""
    query = update.callback_query
    if not query or not query.data:
        return DOC_SELECT
    tpl_name = query.data[len("docsel_"):]
    gen = get_template(tpl_name)
    if not gen:
        await query.answer("⚠️ Шаблон не знайдено", show_alert=True)
        return DOC_SELECT
    await query.answer()

    ud = _ud(context)
    ud[_K_TPL] = tpl_name

    # Підготовка черги полів (без auto)
    input_fields = gen.get_input_fields()
    ud[_K_QUEUE] = [k for k, _ in input_fields]
    ud[_K_IDX]   = 0
    ud[_K_TOTAL] = len(input_fields)
    ud[_K_FILLED] = {}

    desc = gen.config.get("description", tpl_name)
    fcount = ud[_K_TOTAL]
    auto_count = sum(1 for v in gen.config.get("fields", {}).values() if v.get("auto"))

    auto_note = f"\n🤖 Авто-полів (MRZ): **{auto_count}** — обчислюються автоматично" if auto_count else ""

    # Перевіряємо наявність збережених даних
    saved_key = f"doc_last_{tpl_name}"
    has_saved = saved_key in ud

    rows = [
        [InlineKeyboardButton("▶️ Заповнити вручну", callback_data="doc_start_fill")],
        [InlineKeyboardButton("🎲 Рандом", callback_data="doc_random"),
         InlineKeyboardButton("🎲🎲 Рандом ×5", callback_data="doc_random_5")],
        [InlineKeyboardButton("📊 Batch з CSV/Excel", callback_data="doc_start_batch")],
    ]
    if has_saved:
        rows.insert(1, [InlineKeyboardButton("📋 Попередні дані", callback_data="doc_use_saved")])
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="doc_back_menu")])

    await query.edit_message_text(
        f"✅ Обрано: **{desc}**\n\n"
        f"Полів для введення: **{fcount}**{auto_note}\n\n"
        f"Оберіть спосіб заповнення:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(rows)
    )
    return DOC_SELECT


async def handle_template_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробка кнопок після вибору шаблону."""
    query = update.callback_query
    if not query or not query.data:
        return DOC_SELECT
    await query.answer()
    ud = _ud(context)

    if query.data == "doc_start_fill":
        return await _ask_field(update, context, from_edit=False)

    if query.data == "doc_use_saved":
        tpl_name = ud.get(_K_TPL, "")
        saved = ud.get(f"doc_last_{tpl_name}", {})
        if saved:
            ud[_K_FILLED] = dict(saved)
            ud[_K_IDX] = ud.get(_K_TOTAL, 0)
            return await show_confirm(update, context)
        return await _ask_field(update, context, from_edit=False)

    if query.data == "doc_random":
        return await _handle_random_gen(update, context, count=1)

    if query.data == "doc_random_5":
        return await _handle_random_gen(update, context, count=5)

    if query.data == "doc_start_batch":
        await query.edit_message_text(
            "📊 **Batch-генерація**\n\n"
            "Надішліть файл **CSV** або **Excel** (`.xlsx`).\n\n"
            "Стовпці мають відповідати ключам полів шаблону:\n"
            + _batch_columns_hint(ud.get(_K_TPL, "")) +
            "\n\nКожен рядок = один документ.\n"
            "Фото-поля в batch не підтримуються.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Скасувати", callback_data="doc_exit")]
            ])
        )
        return DOC_BATCH

    return DOC_SELECT


async def _handle_random_gen(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              count: int = 1) -> int:
    """Генерує документ(и) з рандомними даними."""
    query = update.callback_query
    ud = _ud(context)
    tpl_name = ud.get(_K_TPL, "")
    gen = get_template(tpl_name)
    if not gen:
        await _send_text(update, "⚠️ Шаблон недоступний.")
        return ConversationHandler.END

    # Визначаємо код країни з шаблону
    country_code = "DE"
    for key, cfg in gen.config.get("fields", {}).items():
        if key == "country_code" and cfg.get("default"):
            country_code = cfg["default"]
            break

    if count == 1:
        # ── Одна генерація → прямо в чат ──
        person = generate_person(country_code)

        if query:
            await query.edit_message_text(
                f"🎲 **Рандом:** `{person['given_name']} {person['surname']}`\n"
                f"📅 {person['birth_date']} | ⚧ {person['sex']} | 📍 {person['birth_place']}\n\n"
                f"⏳ Генерую документ...",
                parse_mode="Markdown"
            )

        try:
            img_bytes = await asyncio.to_thread(gen.render, person, "PNG")
            doc_name = gen.config.get("description", tpl_name)
            chat = update.effective_chat
            if chat:
                await chat.send_document(
                    document=img_bytes,
                    filename=f"{tpl_name}_{person['surname']}_{person['given_name']}.png",
                    caption=(
                        f"🎲 **{doc_name}**\n"
                        f"`{person['given_name']} {person['surname']}` | "
                        f"{person['birth_date']} | {person['sex']}\n\n"
                        f"Натисніть /newdoc щоб створити ще."
                    ),
                    parse_mode="Markdown"
                )
        except Exception as e:
            logger.error("Random gen помилка: %s", e)
            await _send_text(update, f"❌ Помилка: `{e}`")
    else:
        # ── Кілька генерацій → ZIP ──
        if query:
            await query.edit_message_text(
                f"🎲🎲 Генерую **{count}** рандомних документів...",
                parse_mode="Markdown"
            )

        try:
            zip_bytes = await asyncio.to_thread(
                _random_batch_render, gen, tpl_name, country_code, count
            )
            chat = update.effective_chat
            if chat:
                await chat.send_document(
                    document=zip_bytes,
                    filename=f"random_{tpl_name}_{count}.zip",
                    caption=f"🎲 Згенеровано **{count}** рандомних документів.",
                    parse_mode="Markdown"
                )
        except Exception as e:
            logger.error("Random batch помилка: %s", e)
            await _send_text(update, f"❌ Помилка: `{e}`")

    _clear_doc_state(context)
    return ConversationHandler.END


def _random_batch_render(gen: DocumentGenerator, tpl_name: str,
                         country_code: str, count: int) -> bytes:
    """Генерує count рандомних документів → ZIP."""
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i in range(count):
            person = generate_person(country_code)
            try:
                img_bytes = gen.render(person, "PNG")
                fname = f"{i + 1}_{person['surname']}_{person['given_name']}.png"
                zf.writestr(fname, img_bytes)
            except Exception as e:
                logger.warning("Random batch #%d помилка: %s", i + 1, e)
    zip_buf.seek(0)
    return zip_buf.getvalue()


def _batch_columns_hint(tpl_name: str) -> str:
    gen = get_template(tpl_name)
    if not gen:
        return ""
    lines = []
    for key, cfg in gen.get_input_fields():
        if cfg.get("type") == "photo":
            continue
        label = cfg.get("label", key)
        lines.append(f"  `{key}` — {label}")
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ЕКРАН 3 — Заповнення полів
# ─────────────────────────────────────────────

async def _ask_field(update: Update, context: ContextTypes.DEFAULT_TYPE,
                     from_edit: bool = False) -> int:
    ud = _ud(context)

    # Режим редагування одного поля
    edit_key = ud.get(_K_EDIT_KEY)
    if edit_key:
        tpl_name = ud.get(_K_TPL, "")
        gen = get_template(tpl_name)
        if not gen:
            return DOC_CONFIRM
        cfg = gen.config.get("fields", {}).get(edit_key, {})
        current_val = ud.get(_K_FILLED, {}).get(edit_key, cfg.get("default", ""))

        is_photo = cfg.get("type") == "photo"
        if is_photo:
            prompt = (
                f"✏️ **Редагування: {cfg.get('label', edit_key)}**\n\n"
                f"Надішліть нове фото або натисніть ⏭ щоб пропустити:"
            )
        else:
            prompt = (
                f"✏️ **Редагування: {cfg.get('label', edit_key)}**\n\n"
                f"Поточне значення: `{current_val}`\n\n"
            )
            prompt += _validation_hints(cfg)
            prompt += "Введіть нове значення:"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏭ Залишити поточне", callback_data="doc_edit_cancel")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="doc_exit")],
        ])
        await _send_text(update, prompt, kb)
        return DOC_FILL

    # Звичайний покроковий режим
    field_key, field_cfg = _current_field_cfg(context)

    if field_key is None:
        return await show_confirm(update, context)

    idx: int   = ud.get(_K_IDX, 0)
    total: int = ud.get(_K_TOTAL, 1)

    label = field_cfg.get("label", field_key) if field_cfg else field_key
    default = field_cfg.get("default", "") if field_cfg else ""
    is_photo = (field_cfg or {}).get("type") == "photo"

    progress = _progress_bar(idx + 1, total)
    is_first = (idx == 0)

    if is_photo:
        text = (
            f"📷 **Поле {idx + 1} з {total}** (фото)\n"
            f"{progress}\n\n"
            f"📌 **{label}**\n\n"
            f"Надішліть фото або натисніть ⏭ щоб пропустити:"
        )
    else:
        text = (
            f"✏️ **Поле {idx + 1} з {total}**\n"
            f"{progress}\n\n"
            f"📌 **{label}**\n\n"
        )
        text += _validation_hints(field_cfg or {})
        if (field_cfg or {}).get("transliterate"):
            text += "🔤 _Можна вводити кирилицею — автотранслітерація_\n\n"
        text += (
            f"Стандартне значення: `{default or '(порожнє)'}`\n\n"
            f"Введіть нове значення або натисніть ⏭ щоб залишити стандартне:"
        )

    kb = _field_kb(is_first, is_photo)

    if from_edit:
        await _send_text(update, text, kb)
    else:
        chat = update.effective_chat
        if chat:
            await chat.send_message(text, parse_mode="Markdown", reply_markup=kb)

    return DOC_FILL


def _validation_hints(cfg: dict) -> str:
    """Формує підказки валідації для поля."""
    rules = cfg.get("validation", {})
    if not rules:
        return ""
    hints = []
    if "max_length" in rules:
        hints.append(f"📏 Макс: {rules['max_length']} символів")
    if "length" in rules:
        hints.append(f"📏 Рівно: {rules['length']} символів")
    if "choices" in rules:
        hints.append(f"🔤 Варіанти: `{'`, `'.join(rules['choices'])}`")
    if "hint" in rules:
        hints.append(f"💡 {rules['hint']}")
    return "\n".join(hints) + "\n\n" if hints else ""


async def handle_field_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє текстове введення (і для звичайного, і для edit-single режиму)."""
    if not update.message or not update.message.text:
        return DOC_FILL

    text = update.message.text.strip()
    if not text:
        return DOC_FILL

    ud = _ud(context)

    # ── Режим редагування одного поля ──
    edit_key = ud.get(_K_EDIT_KEY)
    if edit_key:
        tpl_name = ud.get(_K_TPL, "")
        gen = get_template(tpl_name)
        if gen:
            cfg = gen.config.get("fields", {}).get(edit_key, {})
            # Транслітерація
            if cfg.get("transliterate"):
                text, was_translit = transliterate_if_needed(text)
                if was_translit:
                    await update.message.reply_text(
                        f"🔤 Транслітеровано → `{text}`", parse_mode="Markdown"
                    )
            # Валідація
            error = validate_field(text, cfg.get("validation", {}))
            if error:
                await update.message.reply_text(
                    f"❌ {error}\nСпробуйте ще раз:",
                    parse_mode="Markdown"
                )
                return DOC_FILL
            ud.setdefault(_K_FILLED, {})[edit_key] = text
        ud.pop(_K_EDIT_KEY, None)
        return await show_confirm(update, context)

    # ── Звичайний покроковий режим ──
    field_key, field_cfg = _current_field_cfg(context)
    if field_key and field_cfg:
        # Транслітерація
        if field_cfg.get("transliterate"):
            text, was_translit = transliterate_if_needed(text)
            if was_translit and update.message:
                await update.message.reply_text(
                    f"🔤 Транслітеровано → `{text}`", parse_mode="Markdown"
                )
        # Валідація
        error = validate_field(text, field_cfg.get("validation", {}))
        if error:
            await update.message.reply_text(
                f"❌ {error}\nСпробуйте ще раз:",
                parse_mode="Markdown"
            )
            return DOC_FILL
        ud.setdefault(_K_FILLED, {})[field_key] = text

    ud[_K_IDX] = ud.get(_K_IDX, 0) + 1
    return await _ask_field(update, context, from_edit=False)


async def handle_field_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє надіслане фото для поля типу photo."""
    if not update.message or not update.message.photo:
        return DOC_FILL

    ud = _ud(context)

    # Завантажуємо фото найвищої якості
    photo_file = await update.message.photo[-1].get_file()
    photo_bytes = await photo_file.download_as_bytearray()

    # Режим редагування одного поля
    edit_key = ud.get(_K_EDIT_KEY)
    if edit_key:
        ud.setdefault(_K_FILLED, {})[edit_key] = bytes(photo_bytes)
        ud.pop(_K_EDIT_KEY, None)
        await update.message.reply_text("📷 Фото оновлено!")
        return await show_confirm(update, context)

    # Звичайний режим
    field_key, _ = _current_field_cfg(context)
    if field_key:
        ud.setdefault(_K_FILLED, {})[field_key] = bytes(photo_bytes)
        await update.message.reply_text("📷 Фото збережено ✅")

    ud[_K_IDX] = ud.get(_K_IDX, 0) + 1
    return await _ask_field(update, context, from_edit=False)


async def handle_skip_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer("⏭ Пропущено")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    ud = _ud(context)

    # Якщо це edit-single — скасовуємо редагування
    if ud.get(_K_EDIT_KEY):
        ud.pop(_K_EDIT_KEY, None)
        return await show_confirm(update, context)

    field_key, field_cfg = _current_field_cfg(context)
    if field_key:
        cfg = field_cfg or {}
        if cfg.get("type") != "photo":
            ud.setdefault(_K_FILLED, {})[field_key] = cfg.get("default", "")

    ud[_K_IDX] = ud.get(_K_IDX, 0) + 1
    return await _ask_field(update, context, from_edit=False)


async def handle_skip_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer("⏩ Всі поля — стандартні значення")

    ud = _ud(context)
    tpl_name: str | None = ud.get(_K_TPL)
    if tpl_name:
        gen = get_template(tpl_name)
        if gen:
            filled: dict[str, Any] = ud.get(_K_FILLED, {})
            for key, cfg in gen.get_input_fields():
                if key not in filled and cfg.get("type") != "photo":
                    filled[key] = cfg.get("default", "")
            ud[_K_FILLED] = filled
            ud[_K_IDX] = ud.get(_K_TOTAL, 0)

    return await show_confirm(update, context)


async def handle_prev_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()

    ud = _ud(context)
    idx = ud.get(_K_IDX, 0)
    ud[_K_IDX] = max(0, idx - 1)

    queue: list = ud.get(_K_QUEUE, [])
    new_idx: int = ud[_K_IDX]
    if new_idx < len(queue):
        filled: dict = ud.get(_K_FILLED, {})
        filled.pop(queue[new_idx], None)

    return await _ask_field(update, context, from_edit=True)


async def handle_edit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасування редагування одного поля — повернення до підтвердження."""
    query = update.callback_query
    if query:
        await query.answer()
    ud = _ud(context)
    ud.pop(_K_EDIT_KEY, None)
    return await show_confirm(update, context)


# ─────────────────────────────────────────────
#  ЕКРАН 4 — Підтвердження
# ─────────────────────────────────────────────

async def show_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = _ud(context)
    summary = _build_summary(context)
    tpl_name = ud.get(_K_TPL, "")
    gen = get_template(tpl_name)
    if not gen:
        await _send_text(update, "⚠️ Шаблон недоступний.")
        return ConversationHandler.END

    # Примітка про авто-поля
    auto_names = [cfg.get("label", k) for k, cfg in gen.config.get("fields", {}).items() if cfg.get("auto")]
    auto_note = ""
    if auto_names:
        auto_note = f"\n🤖 _Авто-поля ({', '.join(auto_names)}) обчисляться при генерації_\n"

    text = (
        f"{summary}\n{auto_note}\n"
        "─────────────────────\n"
        "🖨 Оберіть формат для генерації документа:"
    )

    kb = _confirm_kb(ud, gen)
    await _send_text(update, text, kb)
    return DOC_CONFIRM


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query or not query.data:
        return DOC_CONFIRM
    await query.answer()

    action = query.data
    ud = _ud(context)

    if action == "doc_exit":
        await query.edit_message_text("👋 Генератор документів закрито.")
        _clear_doc_state(context)
        return ConversationHandler.END

    if action == "doc_back_select":
        _clear_doc_state(context)
        return await show_template_select(update, context)

    if action == "doc_back_confirm":
        return await show_confirm(update, context)

    # ── Список полів для редагування ──
    if action == "doc_edit_list":
        tpl_name = ud.get(_K_TPL, "")
        gen = get_template(tpl_name)
        if not gen:
            return DOC_CONFIRM
        await query.edit_message_text(
            "✏️ **Оберіть поле для редагування:**",
            parse_mode="Markdown",
            reply_markup=_edit_fields_kb(gen)
        )
        return DOC_CONFIRM

    # ── Вибір конкретного поля для редагування ──
    if action.startswith("doc_ef_"):
        field_key = action[len("doc_ef_"):]
        ud[_K_EDIT_KEY] = field_key
        return await _ask_field(update, context, from_edit=True)

    # ── Генерація у вибраному форматі ──
    if action.startswith("doc_gen_"):
        fmt = action[len("doc_gen_"):]  # PNG / JPEG / PDF
        return await _generate_and_send(update, context, fmt)

    return DOC_CONFIRM


async def _generate_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE,
                             fmt: str) -> int:
    """Генерує документ і відправляє в чат."""
    query = update.callback_query
    if query:
        await query.edit_message_text(
            f"⏳ **Генерую документ ({fmt})...**", parse_mode="Markdown"
        )

    ud = _ud(context)
    tpl_name: str | None = ud.get(_K_TPL)
    if not tpl_name:
        await _send_text(update, "⚠️ Шаблон не обрано.")
        _clear_doc_state(context)
        return ConversationHandler.END

    gen = get_template(tpl_name)
    if not gen:
        await _send_text(update, "⚠️ Шаблон недоступний.")
        _clear_doc_state(context)
        return ConversationHandler.END

    filled: dict = ud.get(_K_FILLED, {})
    fields_cfg: dict = gen.config.get("fields", {})

    # Збираємо дані: введене + дефолти для пропущених
    data: dict[str, Any] = {}
    for key, cfg in fields_cfg.items():
        if cfg.get("auto"):
            continue  # обчислиться в render
        if key in filled:
            data[key] = filled[key]
        elif cfg.get("type") != "photo":
            data[key] = cfg.get("default", "")

    ext_map = {"PNG": "png", "JPEG": "jpg", "PDF": "pdf"}
    ext = ext_map.get(fmt, "png")

    try:
        img_bytes = await asyncio.to_thread(gen.render, data, fmt)
        doc_name = gen.config.get("description", tpl_name)

        chat = update.effective_chat
        if chat:
            await chat.send_document(
                document=img_bytes,
                filename=f"{tpl_name}.{ext}",
                caption=(
                    f"✅ **{doc_name}**\n\n"
                    f"📄 Формат: {fmt} | Натисніть /newdoc щоб створити ще один."
                ),
                parse_mode="Markdown"
            )
            logger.info("Документ '%s' (%s) відправлено в чат %d", tpl_name, fmt, chat.id)

        # Зберігаємо дані для повторного використання (без фото — занадто великі)
        save_data = {k: v for k, v in filled.items() if isinstance(v, str)}
        ud[f"doc_last_{tpl_name}"] = save_data

    except Exception as e:
        logger.error("Помилка генерації '%s': %s", tpl_name, e)
        await _send_text(
            update,
            f"❌ **Помилка генерації:**\n`{e}`\n\n"
            "Перевірте що `background.png` існує в папці шаблону."
        )

    _clear_doc_state(context)
    return ConversationHandler.END


# ─────────────────────────────────────────────
#  BATCH — генерація з CSV/Excel
# ─────────────────────────────────────────────

async def handle_batch_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє завантажений CSV/Excel для batch-генерації."""
    if not update.message or not update.message.document:
        await _send_text(update, "❌ Надішліть файл CSV або Excel (.xlsx)")
        return DOC_BATCH

    ud = _ud(context)
    tpl_name = ud.get(_K_TPL, "")
    gen = get_template(tpl_name)
    if not gen:
        await _send_text(update, "⚠️ Шаблон недоступний.")
        return ConversationHandler.END

    doc = update.message.document
    file_name = doc.file_name or "file"

    if not (file_name.endswith(".csv") or file_name.endswith(".xlsx") or file_name.endswith(".xls")):
        await update.message.reply_text("❌ Підтримуються тільки `.csv` та `.xlsx` файли.")
        return DOC_BATCH

    await update.message.reply_text(f"📥 Завантажую `{file_name}`...", parse_mode="Markdown")

    try:
        tg_file = await doc.get_file()
        tmp_path = os.path.join(tempfile.gettempdir(), f"batch_{file_name}")
        await tg_file.download_to_drive(tmp_path)

        import pandas as pd
        if file_name.endswith(".csv"):
            df = pd.read_csv(tmp_path, dtype=str).fillna("")
        else:
            df = pd.read_excel(tmp_path, dtype=str).fillna("")

        row_count = len(df)
        if row_count == 0:
            await update.message.reply_text("⚠️ Файл порожній.")
            return ConversationHandler.END
        if row_count > 500:
            await update.message.reply_text("⚠️ Максимум 500 рядків за раз.")
            return ConversationHandler.END

        await update.message.reply_text(
            f"⏳ Генерую **{row_count}** документів...", parse_mode="Markdown"
        )

        # Генерація в окремому потоці
        zip_bytes = await asyncio.to_thread(
            _batch_render, gen, df, tpl_name
        )

        chat = update.effective_chat
        if chat:
            await chat.send_document(
                document=zip_bytes,
                filename=f"batch_{tpl_name}_{row_count}.zip",
                caption=f"✅ Згенеровано **{row_count}** документів.",
                parse_mode="Markdown"
            )

    except Exception as e:
        logger.error("Batch помилка: %s", e)
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    _clear_doc_state(context)
    return ConversationHandler.END


def _batch_render(gen: DocumentGenerator, df: Any, tpl_name: str) -> bytes:
    """Рендерить документи для кожного рядка DataFrame → ZIP bytes."""
    import pandas as pd

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, row in df.iterrows():
            data: dict[str, str] = {col: str(val) for col, val in row.items() if pd.notna(val)}
            try:
                img_bytes = gen.render(data, "PNG")
                # Ім'я файлу: перше текстове поле або номер
                first_val = next((v for v in data.values() if v.strip()), str(i))
                safe_name = "".join(c for c in first_val if c.isalnum() or c in (' ', '_', '-'))[:40]
                zf.writestr(f"{i + 1}_{safe_name.strip()}.png", img_bytes)
            except Exception as e:
                logger.warning("Batch рядок %d помилка: %s", i + 1, e)
    zip_buf.seek(0)
    return zip_buf.getvalue()


# ─────────────────────────────────────────────
#  Навігація
# ─────────────────────────────────────────────

async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query or not query.data:
        return ConversationHandler.END
    await query.answer()

    action = query.data

    if action == "doc_exit":
        await query.edit_message_text("👋 Генератор документів закрито.")
        _clear_doc_state(context)
        return ConversationHandler.END

    if action == "doc_open_select":
        return await show_template_select(update, context)

    if action == "doc_back_menu":
        _clear_doc_state(context)
        return await show_doc_menu(update, context)

    return DOC_MENU


def _clear_doc_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    ud = _ud(context)
    for key in (_K_TPL, _K_FILLED, _K_QUEUE, _K_IDX, _K_TOTAL, _K_EDIT_KEY, _K_FMT):
        ud.pop(key, None)
    # doc_last_{tpl} НЕ видаляємо — це збережені дані


# ─────────────────────────────────────────────
#  Адмін: превью шаблону
# ─────────────────────────────────────────────

async def cmd_preview_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    args = context.args or []
    if not args:
        names = list_templates()
        if not names:
            await update.message.reply_text("⚠️ Шаблони не знайдено.")
            return
        await update.message.reply_text(
            "📄 Доступні шаблони:\n" + "\n".join(f"• `{n}`" for n in names) +
            "\n\nВикористання: `/previewdoc <назва>`",
            parse_mode="Markdown"
        )
        return

    name = args[0]
    gen = get_template(name)
    if not gen:
        await update.message.reply_text(f"⚠️ Шаблон `{name}` не знайдено.", parse_mode="Markdown")
        return

    await update.message.reply_text(f"⏳ Генерую превью `{name}`...", parse_mode="Markdown")
    try:
        img_bytes = gen.preview()
        await update.message.reply_document(
            document=img_bytes,
            filename=f"preview_{name}.png",
            caption=(
                f"👁 **Превью:** {gen.config.get('description', name)}\n"
                f"_Поля показані як [назва\\_поля] для перевірки координат_"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")


# ─────────────────────────────────────────────
#  Швидка команда /quickdoc
# ─────────────────────────────────────────────

async def cmd_quickdoc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/quickdoc <шаблон> <прізвище> <ім'я> <дата_народж> <стать> [місце_народж]

    Приклад: /quickdoc germany_passport Мустерманн Еріка 12.08.1983 F Berlin
    Всі інші поля заповнюються автоматично.
    """
    if not update.message:
        return

    args = context.args or []
    if len(args) < 5:
        names = list_templates()
        await update.message.reply_text(
            "⚡ **Швидка генерація документа**\n\n"
            "Використання:\n"
            "`/quickdoc <шаблон> <прізвище> <ім'я> <дата_народж> <стать> [місце]`\n\n"
            "Приклад:\n"
            "`/quickdoc germany_passport Müller Hans 15.03.1990 M Hamburg`\n"
            "`/quickdoc germany_passport Шевченко Тарас 09.03.1814 M Моринці`\n\n"
            "🔤 _Кирилиця транслітерується автоматично_\n\n"
            f"Доступні шаблони: {', '.join(f'`{n}`' for n in names)}",
            parse_mode="Markdown"
        )
        return

    tpl_name = args[0]
    gen = get_template(tpl_name)
    if not gen:
        await update.message.reply_text(f"⚠️ Шаблон `{tpl_name}` не знайдено.", parse_mode="Markdown")
        return

    surname_raw = args[1]
    given_raw = args[2]
    birth_date = args[3]
    sex = args[4].upper()
    birth_place = args[5] if len(args) > 5 else "BERLIN"

    # Транслітерація
    surname, s_tr = transliterate_if_needed(surname_raw)
    given, g_tr = transliterate_if_needed(given_raw)
    birth_place_lat, bp_tr = transliterate_if_needed(birth_place)

    translit_note = ""
    if s_tr or g_tr or bp_tr:
        translit_note = f"\n🔤 Транслітерація: `{surname}` `{given}`"
        if bp_tr:
            translit_note += f" `{birth_place_lat}`"

    data: dict[str, Any] = {
        "surname": surname,
        "given_name": given,
        "birth_date": birth_date,
        "sex": sex,
        "birth_place": birth_place_lat,
    }

    await update.message.reply_text(
        f"⚡ **Генерую:** `{surname} {given}`{translit_note}\n⏳ Зачекайте...",
        parse_mode="Markdown"
    )

    try:
        img_bytes = await asyncio.to_thread(gen.render, data, "PNG")
        doc_name = gen.config.get("description", tpl_name)
        await update.message.reply_document(
            document=img_bytes,
            filename=f"{tpl_name}_{surname}_{given}.png",
            caption=f"✅ **{doc_name}**\n`{surname} {given}` | {birth_date} | {sex}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error("quickdoc помилка: %s", e)
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")


# ─────────────────────────────────────────────
#  ConversationHandler
# ─────────────────────────────────────────────

def build_doc_conversation() -> ConversationHandler:
    nav_pattern = r"^(doc_exit|doc_open_select|doc_back_menu)$"

    return ConversationHandler(
        entry_points=[
            CommandHandler("newdoc", show_doc_menu),
            MessageHandler(filters.Regex(r"^🪪 Документи$"), show_doc_menu),
        ],
        states={
            DOC_MENU: [
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
            ],
            DOC_SELECT: [
                CallbackQueryHandler(handle_template_choice, pattern=r"^docsel_"),
                CallbackQueryHandler(handle_template_action,
                                     pattern=r"^(doc_start_fill|doc_use_saved|doc_start_batch|doc_random|doc_random_5)$"),
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
            ],
            DOC_FILL: [
                CallbackQueryHandler(handle_skip_field, pattern=r"^doc_skip$"),
                CallbackQueryHandler(handle_skip_all,   pattern=r"^doc_skip_all$"),
                CallbackQueryHandler(handle_prev_field, pattern=r"^doc_prev$"),
                CallbackQueryHandler(handle_edit_cancel, pattern=r"^doc_edit_cancel$"),
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
                # Фото
                MessageHandler(filters.PHOTO, handle_field_photo),
                # Текст
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_field_text),
            ],
            DOC_CONFIRM: [
                CallbackQueryHandler(handle_confirm,
                                     pattern=r"^(doc_gen_|doc_edit_list|doc_ef_|doc_back_select|doc_back_confirm|doc_exit)"),
            ],
            DOC_BATCH: [
                MessageHandler(filters.Document.ALL, handle_batch_file),
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(handle_navigation, pattern=r"^doc_exit$"),
            CommandHandler("newdoc", show_doc_menu),
        ],
        allow_reentry=True,
        name="doc_conversation",
    )
