"""
handlers_documents.py — Меню генерації документів із PSD-шаблонів

Флоу:
  Кнопка "🪪 Документи" у головному меню
    → Екран вітання (що це, які шаблони)
    → Вибір шаблону (inline-кнопки)
    → Покрокове заповнення полів (прогрес-бар, /skip)
    → Підтвердження (підсумок всіх значень)
    → Генерація → PNG у чат

Команди:
  /newdoc      — аналог кнопки "🪪 Документи"
  /previewdoc  — превью координат шаблону (для адміна)
"""
from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes, ConversationHandler,
    CommandHandler, CallbackQueryHandler, MessageHandler, filters
)

from document_generator import load_all_templates, get_template, list_templates

logger = logging.getLogger(__name__)

# ── Стани ConversationHandler ──
DOC_MENU    = 40   # головне меню генератора
DOC_SELECT  = 41   # вибір шаблону
DOC_FILL    = 42   # заповнення полів
DOC_CONFIRM = 43   # підтвердження

# ── Ключі context.user_data ──
_K_TPL     = "doc_tpl"       # назва обраного шаблону
_K_FILLED  = "doc_filled"    # {field_key: value}
_K_QUEUE   = "doc_queue"     # [field_key, ...] черга
_K_IDX     = "doc_idx"       # поточний індекс у черзі
_K_TOTAL   = "doc_total"     # загальна кількість полів


# ─────────────────────────────────────────────
#  Утиліти
# ─────────────────────────────────────────────

def _progress_bar(current: int, total: int, length: int = 10) -> str:
    """▓▓▓░░░░░░░  3/10"""
    filled = int(current / total * length) if total else 0
    bar = "▓" * filled + "░" * (length - filled)
    return f"`[{bar}] {current}/{total}`"


def _template_list_kb(include_back: bool = True) -> InlineKeyboardMarkup:
    """Кнопки вибору шаблону."""
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


def _field_kb(is_first: bool) -> InlineKeyboardMarkup:
    """Кнопки під час заповнення поля."""
    rows = [
        [InlineKeyboardButton("⏭ Пропустити", callback_data="doc_skip"),
         InlineKeyboardButton("⏩ Пропустити всі", callback_data="doc_skip_all")],
    ]
    if not is_first:
        rows.append([InlineKeyboardButton("🔙 Попереднє", callback_data="doc_prev")])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="doc_exit")])
    return InlineKeyboardMarkup(rows)


def _confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Згенерувати документ", callback_data="doc_generate")],
        [InlineKeyboardButton("✏️ Редагувати поля",      callback_data="doc_restart"),
         InlineKeyboardButton("🔄 Інший шаблон",         callback_data="doc_back_select")],
        [InlineKeyboardButton("❌ Вийти",                callback_data="doc_exit")],
    ])


def _current_field_cfg(context: ContextTypes.DEFAULT_TYPE):
    """Повертає (field_key, field_cfg) для поточного кроку."""
    tpl_name = context.user_data.get(_K_TPL)
    gen = get_template(tpl_name)
    if not gen:
        return None, None
    queue: list = context.user_data.get(_K_QUEUE, [])
    idx: int = context.user_data.get(_K_IDX, 0)
    if idx >= len(queue):
        return None, None
    key = queue[idx]
    return key, gen.config["fields"].get(key, {})


def _build_summary(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Формує текстовий підсумок всіх введених значень."""
    tpl_name = context.user_data.get(_K_TPL)
    gen = get_template(tpl_name)
    if not gen:
        return ""
    fields_cfg = gen.config.get("fields", {})
    filled     = context.user_data.get(_K_FILLED, {})

    lines = [f"📋 **{gen.config.get('description', tpl_name)}**\n"]
    for key, cfg in fields_cfg.items():
        val = filled.get(key, cfg.get("default", "—"))
        label = cfg.get("label", key)
        # Скорочуємо MRZ-рядки для читабельності
        display = val if len(val) <= 40 else val[:37] + "..."
        lines.append(f"• {label}: `{display}`")
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ЕКРАН 1 — Головне меню генератора
# ─────────────────────────────────────────────

async def show_doc_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Точка входу: кнопка '🪪 Документи' або /newdoc"""
    if not list_templates():
        load_all_templates()

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
        # Список доступних шаблонів
        template_lines = []
        for name in names:
            gen = get_template(name)
            if gen:
                desc  = gen.config.get("description", name)
                flds  = len(gen.config.get("fields", {}))
                template_lines.append(f"  {desc} — {flds} полів")

        templates_block = "\n".join(template_lines)

        text = (
            "🪪 **Генератор документів**\n\n"
            "Бот заповнює шаблони документів даними і надсилає готове зображення.\n\n"
            "📌 **Як це працює:**\n"
            "1️⃣ Оберіть шаблон документа\n"
            "2️⃣ Введіть значення для кожного поля\n"
            "   _(або натисніть_ ⏭ _щоб залишити стандартне)_\n"
            "3️⃣ Перевірте підсумок і підтвердіть\n"
            "4️⃣ Отримайте готовий документ PNG\n\n"
            f"📂 **Доступні шаблони [{count}]:**\n"
            f"{templates_block}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 Обрати шаблон →", callback_data="doc_open_select")],
            [InlineKeyboardButton("❌ Закрити",          callback_data="doc_exit")],
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
    """Показує список шаблонів з описами."""
    query = update.callback_query
    if query:
        await query.answer()

    names = list_templates()

    # Будуємо детальний опис кожного шаблону
    desc_lines = []
    for name in names:
        gen = get_template(name)
        if gen:
            desc   = gen.config.get("description", name)
            fields = gen.config.get("fields", {})
            fcount = len(fields)
            desc_lines.append(f"**{desc}** — {fcount} полів")

    text = (
        "📄 **Оберіть шаблон документа:**\n\n"
        + "\n".join(desc_lines)
    )

    kb = _template_list_kb(include_back=True)

    if query:
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.effective_chat.send_message(text, parse_mode="Markdown", reply_markup=kb)

    return DOC_SELECT


async def handle_template_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір конкретного шаблону."""
    query = update.callback_query
    if not query:
        return DOC_SELECT
    tpl_name = query.data[len("docsel_"):]
    gen = get_template(tpl_name)
    if not gen:
        await query.answer("⚠️ Шаблон не знайдено", show_alert=True)
        return DOC_SELECT

    await query.answer()

    context.user_data[_K_TPL]   = tpl_name
    fields_cfg = gen.config.get("fields", {})
    context.user_data[_K_QUEUE] = list(fields_cfg.keys())
    context.user_data[_K_IDX]   = 0
    context.user_data[_K_TOTAL] = len(fields_cfg)
    context.user_data[_K_FILLED] = {}

    desc   = gen.config.get("description", tpl_name)
    fcount = context.user_data[_K_TOTAL]

    await query.edit_message_text(
        f"✅ Обрано: **{desc}**\n\n"
        f"Потрібно заповнити **{fcount}** полів.\n"
        f"Натисніть ⏭ щоб пропустити поле і залишити стандартне значення.\n"
        f"Натисніть ⏩ щоб пропустити **всі** поля і одразу перейти до підтвердження.\n\n"
        f"Починаємо → ",
        parse_mode="Markdown"
    )

    return await _ask_field(update, context, from_edit=False)


# ─────────────────────────────────────────────
#  ЕКРАН 3 — Заповнення полів (покроково)
# ─────────────────────────────────────────────

async def _ask_field(update: Update, context: ContextTypes.DEFAULT_TYPE,
                     from_edit: bool = False) -> int:
    """Запитує значення поточного поля."""
    field_key, field_cfg = _current_field_cfg(context)

    if field_key is None:
        # Всі поля заповнені — переходимо до підтвердження
        return await show_confirm(update, context)

    idx: int   = context.user_data.get(_K_IDX, 0)
    total: int = context.user_data.get(_K_TOTAL, 1)

    label   = field_cfg.get("label", field_key)
    default = field_cfg.get("default", "")

    progress = _progress_bar(idx + 1, total)
    is_first = (idx == 0)

    text = (
        f"✏️ **Поле {idx + 1} з {total}**\n"
        f"{progress}\n\n"
        f"📌 **{label}**\n\n"
        f"Стандартне значення: `{default or '(порожнє)'}`\n\n"
        f"Введіть нове значення або натисніть ⏭ щоб залишити стандартне:"
    )

    kb = _field_kb(is_first)

    if from_edit:
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.effective_chat.send_message(
                    text, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            await update.effective_chat.send_message(
                text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.effective_chat.send_message(
            text, parse_mode="Markdown", reply_markup=kb)

    return DOC_FILL


async def handle_field_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє введений текст як значення поля."""
    if not update.message or not update.message.text:
        return DOC_FILL

    text = update.message.text.strip()
    if not text:
        return DOC_FILL

    field_key, _ = _current_field_cfg(context)
    if field_key:
        context.user_data.setdefault(_K_FILLED, {})[field_key] = text

    context.user_data[_K_IDX] = context.user_data.get(_K_IDX, 0) + 1
    return await _ask_field(update, context, from_edit=False)


async def handle_skip_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """⏭ Пропустити одне поле — залишити стандартне значення."""
    query = update.callback_query
    if query:
        await query.answer("⏭ Пропущено, залишено стандартне значення")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    field_key, field_cfg = _current_field_cfg(context)
    if field_key:
        default = (field_cfg or {}).get("default", "")
        context.user_data.setdefault(_K_FILLED, {})[field_key] = default

    context.user_data[_K_IDX] = context.user_data.get(_K_IDX, 0) + 1
    return await _ask_field(update, context, from_edit=False)


async def handle_skip_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """⏩ Пропустити всі поля — заповнити стандартними значеннями."""
    query = update.callback_query
    if query:
        await query.answer("⏩ Всі поля заповнено стандартними значеннями")

    tpl_name = context.user_data.get(_K_TPL)
    gen = get_template(tpl_name)
    if gen:
        filled = {}
        for key, cfg in gen.config.get("fields", {}).items():
            filled[key] = cfg.get("default", "")
        context.user_data[_K_FILLED] = filled
        context.user_data[_K_IDX] = context.user_data.get(_K_TOTAL, 0)

    return await show_confirm(update, context)


async def handle_prev_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Повернутись до попереднього поля."""
    query = update.callback_query
    if query:
        await query.answer()

    idx = context.user_data.get(_K_IDX, 0)
    context.user_data[_K_IDX] = max(0, idx - 1)

    # Видаляємо попереднє заповнення щоб можна було ввести знову
    queue: list = context.user_data.get(_K_QUEUE, [])
    new_idx = context.user_data[_K_IDX]
    if new_idx < len(queue):
        context.user_data.get(_K_FILLED, {}).pop(queue[new_idx], None)

    return await _ask_field(update, context, from_edit=True)


# ─────────────────────────────────────────────
#  ЕКРАН 4 — Підтвердження
# ─────────────────────────────────────────────

async def show_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показує підсумок і кнопки підтвердження."""
    summary = _build_summary(context)

    text = (
        f"{summary}\n\n"
        "─────────────────────\n"
        "🖨 **Все вірно?** Натисніть **Згенерувати** щоб отримати документ."
    )

    kb = _confirm_kb()

    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.effective_chat.send_message(
                text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        await update.effective_chat.send_message(
            text, parse_mode="Markdown", reply_markup=kb)

    return DOC_CONFIRM


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Генерує і відправляє документ."""
    query = update.callback_query
    if not query:
        return DOC_CONFIRM
    await query.answer()

    action = query.data

    if action == "doc_exit":
        await query.edit_message_text("👋 Генератор документів закрито.")
        _clear_doc_state(context)
        return ConversationHandler.END

    if action == "doc_back_select":
        _clear_doc_state(context)
        return await show_template_select(update, context)

    if action == "doc_restart":
        # Перезапускаємо заповнення з початку
        tpl_name = context.user_data.get(_K_TPL)
        gen = get_template(tpl_name)
        if gen:
            context.user_data[_K_IDX]    = 0
            context.user_data[_K_FILLED]  = {}
            context.user_data[_K_QUEUE]   = list(gen.config.get("fields", {}).keys())
            context.user_data[_K_TOTAL]   = len(gen.config.get("fields", {}))
        return await _ask_field(update, context, from_edit=True)

    if action == "doc_generate":
        await query.edit_message_text("⏳ **Генерую документ...**", parse_mode="Markdown")

        tpl_name = context.user_data.get(_K_TPL)
        gen = get_template(tpl_name)
        if not gen:
            await query.edit_message_text("⚠️ Шаблон недоступний. Спробуйте знову.")
            _clear_doc_state(context)
            return ConversationHandler.END

        filled     = context.user_data.get(_K_FILLED, {})
        fields_cfg = gen.config.get("fields", {})
        data       = {key: filled.get(key, cfg.get("default", ""))
                      for key, cfg in fields_cfg.items()}

        try:
            img_bytes = gen.render(data, output_format="PNG")
            doc_name  = gen.config.get("description", tpl_name)

            await query.message.reply_document(
                document=img_bytes,
                filename=f"{tpl_name}.png",
                caption=(
                    f"✅ **{doc_name}**\n\n"
                    f"📄 Документ згенеровано успішно.\n"
                    f"Натисніть /newdoc щоб створити ще один."
                ),
                parse_mode="Markdown"
            )
            logger.info("Документ '%s' відправлено в чат %d",
                        tpl_name, update.effective_chat.id)
        except Exception as e:
            logger.error("Помилка генерації '%s': %s", tpl_name, e)
            await query.edit_message_text(
                f"❌ **Помилка генерації:**\n`{e}`\n\n"
                "Перевірте що `background.png` існує в папці шаблону.",
                parse_mode="Markdown"
            )

        _clear_doc_state(context)
        return ConversationHandler.END

    return DOC_CONFIRM


# ─────────────────────────────────────────────
#  Навігація (спільні callback)
# ─────────────────────────────────────────────

async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє кнопки навігації між екранами."""
    query = update.callback_query
    if not query:
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
    for key in (_K_TPL, _K_FILLED, _K_QUEUE, _K_IDX, _K_TOTAL):
        context.user_data.pop(key, None)


# ─────────────────────────────────────────────
#  Адмін: превью шаблону
# ─────────────────────────────────────────────

async def cmd_preview_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/previewdoc <назва> — превью з placeholder-текстом (для адміна)."""
    if not update.message:
        return

    if not list_templates():
        load_all_templates()

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
                f"_Поля показані як [назва_поля] для перевірки координат_"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: `{e}`", parse_mode="Markdown")


# ─────────────────────────────────────────────
#  Реєстрація ConversationHandler
# ─────────────────────────────────────────────

def build_doc_conversation() -> ConversationHandler:
    """Повертає готовий ConversationHandler для реєстрації у bot.py."""

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
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
            ],
            DOC_FILL: [
                # Кнопки
                CallbackQueryHandler(handle_skip_field, pattern=r"^doc_skip$"),
                CallbackQueryHandler(handle_skip_all,   pattern=r"^doc_skip_all$"),
                CallbackQueryHandler(handle_prev_field, pattern=r"^doc_prev$"),
                CallbackQueryHandler(handle_navigation, pattern=nav_pattern),
                # Текстове введення
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_field_text),
            ],
            DOC_CONFIRM: [
                CallbackQueryHandler(handle_confirm,
                                     pattern=r"^(doc_generate|doc_restart|doc_back_select|doc_exit)$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(handle_navigation, pattern=r"^doc_exit$"),
            CommandHandler("newdoc", show_doc_menu),
        ],
        allow_reentry=True,
        name="doc_conversation",
    )
