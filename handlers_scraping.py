import asyncio
import logging
import os
import threading

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

import database
from config import ADMIN_ID
from scraper import run_scraping
from state import (
    scraping_status, _status_lock, MAX_PARALLEL_TASKS,
    SELECT_SITE, TYPING_KEYWORD, TYPING_COUNT, TYPING_YEAR, SELECT_FORMAT, SELECT_UK_MODE
)
from keyboards import (
    get_sites_kb, get_back_kb, get_formats_kb, get_uk_mode_kb,
    get_main_panel, get_stop_kb, get_schedule_kb
)

logger = logging.getLogger(__name__)


def get_progress_bar(current: int, total: int, length: int = 12) -> str:
    if total <= 0:
        return "░" * length
    progress = int((current / total) * length)
    percent = int((current / total) * 100)
    bar = "█" * progress + "░" * (length - progress)
    return f"`[{bar}] {percent}%`"


async def safe_answer(query) -> None:
    if query:
        try:
            await query.answer()
        except Exception:
            pass


async def safe_edit(query, text: str, reply_markup=None) -> None:
    from telegram.error import BadRequest
    if query:
        try:
            await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode='Markdown')
        except BadRequest:
            pass


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or context.user_data is None:
        return ConversationHandler.END
    user = update.effective_user
    # Адмін автоматично в whitelist
    if user and ADMIN_ID and user.id == ADMIN_ID:
        database.add_user(user.id, user.username or "", role="admin")
    # Перевірка доступу
    if user and not database.is_user_allowed(user.id):
        await update.message.reply_text(
            f"⛔️ **Доступ заборонено**\n\nТвій ID: `{user.id}`\n"
            "Зверніться до адміністратора.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END
    context.user_data.clear()
    await update.message.reply_text("🤖 Робоча панель активована.", reply_markup=get_main_panel())
    await update.message.reply_text("🌍 **Оберіть сайт для пошуку:**",
                                    reply_markup=get_sites_kb(), parse_mode="Markdown")
    return SELECT_SITE


async def site_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return SELECT_SITE
    await safe_answer(query)
    site = query.data.replace("site_", "")
    context.user_data['site'] = site
    await safe_edit(query, f"🌍 Обрано: **{site}**\n\n🔎 **Введіть ключове слово** для пошуку:",
                    get_back_kb("start"))
    return TYPING_KEYWORD


async def save_kw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_KEYWORD
    raw_kw = update.message.text.strip()
    context.user_data['kw'] = raw_kw

    # Підтримка кількох ключових слів через кому
    keywords = [k.strip() for k in raw_kw.split(',') if k.strip()]
    if len(keywords) > 1:
        kw_preview = '\n'.join(f"  • `{k}`" for k in keywords)
        header = f"🔑 Ключових слів: **{len(keywords)}**\n{kw_preview}"
        note = "\n\n💡 Бот пройдеться по кожному слову і об'єднає результати."
    else:
        header = f"🔑 Ключ: `{raw_kw}`"
        note = ""

    await update.message.reply_text(
        f"{header}{note}\n\n🔢 **Скільки компаній зібрати** (загалом)?",
        reply_markup=get_back_kb("kw"), parse_mode="Markdown"
    )
    return TYPING_COUNT


async def save_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_COUNT
    if not update.message.text.isdigit():
        await update.message.reply_text("❌ Будь ласка, введіть число (наприклад: 50).")
        return TYPING_COUNT
    context.user_data['count'] = update.message.text
    await update.message.reply_text(
        "📅 **Починаючи з якого року реєстрації шукати компанії?**\n\n"
        "Введіть рік (наприклад: `2022`) — бот знайде компанії за **2022, 2023, 2024...** і далі.\n"
        "Або введіть `0` — без обмежень по даті.",
        parse_mode="Markdown"
    )
    return TYPING_YEAR


async def save_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_YEAR

    raw = update.message.text.strip()

    # Базова валідація: лише цифри, або "0"
    if raw != "0" and (not raw.isdigit() or len(raw) != 4):
        await update.message.reply_text(
            "❌ Введіть 4-значний рік (наприклад: `2022`) або `0` для всіх дат.",
            parse_mode="Markdown"
        )
        return TYPING_YEAR

    context.user_data['target_year'] = raw

    # Підтверджувальний текст
    if raw == "0":
        year_note = "📅 Рік: **без обмежень**"
    else:
        year_note = f"📅 Рік реєстрації: **від {raw} і пізніше**"

    # Для UK — спочатку питаємо про режим завантаження PDF
    if context.user_data.get('site') == 'UnitedKingdom':
        await update.message.reply_text(
            f"{year_note}\n\n"
            "🇬🇧 **Режим збору UK документів:**\n\n"
            "📥 **Завантажити PDF** — зберегти всі файли локально на диск\n"
            "🔗 **Тільки посилання** — швидко, без завантаження, тільки лінк NEWINC у таблицю",
            reply_markup=get_uk_mode_kb(), parse_mode="Markdown"
        )
        return SELECT_UK_MODE

    await update.message.reply_text(
        f"{year_note}\n\n📁 **Оберіть формат файлу** для збереження результатів:",
        reply_markup=get_formats_kb(), parse_mode="Markdown"
    )
    return SELECT_FORMAT


async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return ConversationHandler.END
    await safe_answer(query)

    if query.data == "cancel_search":
        context.user_data.clear()
        await safe_edit(query, "❌ **Пошук скасовано.**")
        return ConversationHandler.END

    nav_map = {
        "back_start": (SELECT_SITE, "🌍 **Оберіть сайт для пошуку:**", get_sites_kb()),
        "back_kw": (TYPING_KEYWORD,
                    f"🔎 **Введіть ключове слово** для `{context.user_data.get('site', '...')}`:",
                    get_back_kb("start")),
        "back_count": (TYPING_COUNT,
                       f"🔢 **Скільки компаній зібрати?** (ключ: `{context.user_data.get('kw', '')}`)",
                       get_back_kb("kw")),
    }
    if query.data in nav_map:
        state, text, kb = nav_map[query.data]
        await safe_edit(query, text, kb)
        return state

    return ConversationHandler.END


async def stop_scraping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not update.effective_chat:
        return
    await safe_answer(query)
    chat_id = update.effective_chat.id
    async with _status_lock:
        if chat_id in scraping_status:
            scraping_status[chat_id]['is_running'] = False
    await safe_edit(query, "🛑 **Зупинка процесу...**")
    return ConversationHandler.END


async def select_uk_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return SELECT_UK_MODE

    if "back" in query.data or "cancel" in query.data:
        return await handle_navigation(update, context)

    await safe_answer(query)

    # Зберігаємо вибір користувача
    context.user_data['uk_download_pdf'] = (query.data == "ukmode_download")

    mode_label = "📥 завантаження PDF" if context.user_data['uk_download_pdf'] else "🔗 тільки посилання"
    await safe_edit(query, f"✅ Режим: **{mode_label}**\n\n📁 **Оберіть формат файлу** для результатів:", get_formats_kb())
    return SELECT_FORMAT


async def run_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None or not update.effective_chat:
        return ConversationHandler.END

    if "back" in query.data or "cancel" in query.data:
        return await handle_navigation(update, context)

    await safe_answer(query)
    fmt = query.data.replace("fmt_", "")
    chat_id = update.effective_chat.id

    # Якщо обрано "Запланувати" — показуємо вибір розкладу
    if fmt == "SCHEDULE":
        await safe_edit(query, "📅 **Оберіть розклад запуску:**", get_schedule_kb())
        return SELECT_FORMAT
    ud = context.user_data

    # ── Атомарна перевірка + запис статусу (один лок!) ──
    # Два окремих блоки `async with _status_lock` створюють вікно для race condition:
    # між першим і другим блоком інший запит міг зайняти слот.
    async with _status_lock:
        active_tasks = sum(1 for s in scraping_status.values() if s.get('is_running'))
        if active_tasks >= MAX_PARALLEL_TASKS:
            await safe_edit(
                query,
                f"⏳ **Черга заповнена**\n\n"
                f"Зараз виконується `{active_tasks}` з `{MAX_PARALLEL_TASKS}` задач.\n"
                f"Зачекай поки завершиться один з поточних пошуків і спробуй знову.",
            )
            return ConversationHandler.END
        # Записуємо запис в ТОМ САМОМУ блоці лока — атомарно
        scraping_status[chat_id] = {
            'current':           0,
            'max':               int(ud.get('count', 1)),
            'last_name':         "Ініціалізація...",
            'is_running':        True,
            'file_path':         None,
            'target_year':       ud.get('target_year', '2025'),
            'uk_download_pdf':   ud.get('uk_download_pdf', True),
            'filtered_inactive': 0,   # лічильник відфільтрованих неактивних
            'filtered_duplicate': 0,  # лічильник пропущених дублікатів
        }

    msg_id = query.message.message_id if query.message else 0
    await safe_edit(query, "⚙️ **Запускаю браузер...**", reply_markup=get_stop_kb())

    # Зберігаємо в історію
    database.save_search_history(
        chat_id=chat_id,
        site=ud.get('site', ''),
        keyword=ud.get('kw', ''),
        count=int(ud.get('count', 1)),
        year=ud.get('target_year', '0'),
        file_format=fmt
    )
    # Зберігаємо fmt для можливого повтору
    ud['file_format'] = fmt

    threading.Thread(
        target=run_scraping,
        args=(chat_id, ud.get('kw'), int(ud.get('count', 1)),
              ud.get('site'), fmt, scraping_status[chat_id]),
        daemon=True
    ).start()

    if msg_id > 0:
        asyncio.create_task(status_updater(context, chat_id, msg_id))
    return ConversationHandler.END


# ─────────────────────────────────────────────
#  СТАТУС ОНОВЛЕННЯ
# ─────────────────────────────────────────────

async def status_updater(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
    """Оновлює прогрес скрапінгу в Telegram.

    Виправлення race condition:
    - Читаємо snapshot словника під _status_lock (коротко)
    - НЕ тримаємо лок під час await context.bot.edit_message_text
    - pop() виконуємо під локом — атомарно
    """
    last_count = -1
    stop_kb   = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Зупинити збір", callback_data="stop_scraping")]])
    repeat_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Почати новий пошук", callback_data="repeat_search")]])

    while True:
        # ── Короткий snapshot під локом ──
        async with _status_lock:
            st = scraping_status.get(chat_id)
            is_running = st.get('is_running', False) if st else False
            current    = st.get('current', 0)        if st else 0
            total      = st.get('max', 1)             if st else 1
            last_name  = st.get('last_name', '...')  if st else '...'

        if not st or not is_running:
            break

        if current != last_count:
            p_bar = get_progress_bar(current, total)
            text  = (
                f"🚀 **Процес збору даних...**\n{p_bar}\n\n"
                f"🏢 Опрацьовується:\n`{last_name}`\n\n"
                f"✅ Зібрано: **{current}** з **{total}**"
            )
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=text, parse_mode='Markdown', reply_markup=stop_kb
                )
                last_count = current
            except Exception:
                pass

        await asyncio.sleep(3)

    # ── Фінальний стан: читаємо і видаляємо атомарно ──
    async with _status_lock:
        st = scraping_status.pop(chat_id, None)

    if st:
        collected         = st.get('current', 0)
        filtered_inactive = st.get('filtered_inactive', 0)
        filtered_dup      = st.get('filtered_duplicate', 0)

        # ── Рядки статистики фільтрації ──
        stats_lines = []
        if filtered_inactive:
            stats_lines.append(f"  ⛔ Неактивних відфільтровано: `{filtered_inactive}`")
        if filtered_dup:
            stats_lines.append(f"  🔁 Дублікатів пропущено: `{filtered_dup}`")
        stats_block = ("\n\n📊 *Статистика фільтрації:*\n" + "\n".join(stats_lines)) if stats_lines else ""

        if st.get('file_path'):
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🏁 **Роботу завершено!** Зібрано: **{collected}** компаній.{stats_block}\n\n"
                    f"Ваш файл готовий 👇"
                ),
                parse_mode="Markdown", reply_markup=repeat_kb
            )
            try:
                with open(st['file_path'], 'rb') as f:
                    await context.bot.send_document(chat_id=chat_id, document=f)
                os.remove(st['file_path'])
            except Exception as e:
                logger.error("Помилка відправки файлу: %s", e)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ **Збір зупинено.** Дані не знайдено або процес перервано.{stats_block}"
                ),
                parse_mode="Markdown", reply_markup=repeat_kb
            )


async def repeat_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not update.effective_chat or context.user_data is None:
        return ConversationHandler.END
    await safe_answer(query)
    context.user_data.clear()
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🌍 **Оберіть сайт для пошуку:**",
        reply_markup=get_sites_kb(), parse_mode="Markdown"
    )
    return SELECT_SITE
