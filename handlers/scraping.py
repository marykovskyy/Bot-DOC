import asyncio
import html as _html
import logging
import os
import threading

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

import database
from constants import STATUS_UPDATE_SEC
from scrapers.main import run_scraping
from state import (
    ASK_VALIDATE,
    MAX_PARALLEL_TASKS,
    SELECT_FORMAT,
    SELECT_INDIA_STATE,
    SELECT_SITE,
    SELECT_UK_MODE,
    TYPING_COUNT,
    TYPING_KEYWORD,
    TYPING_YEAR,
    _status_lock,
    scraping_status,
)

# ── Межі валідації ────────────────────────────────────────────────────────
_MAX_KEYWORD_LEN  = 200    # максимальна довжина одного keyword
_MAX_COUNT        = 1000   # максимум компаній за один пошук
_MIN_YEAR         = 1900   # нижня межа валідації року
_MAX_YEAR         = 2100   # верхня межа валідації року
from handlers.admin import require_auth
from keyboards import (
    GROUP_LABELS,
    get_back_kb,
    get_count_quick_kb,
    get_formats_kb,
    get_group_kb,
    get_india_state_kb,
    get_main_panel,
    get_schedule_kb,
    get_sites_kb,
    get_stop_kb,
    get_uk_mode_kb,
    get_unsupported_country_kb,
    get_validate_choice_kb,
    get_year_quick_kb,
)

# Країни де Google Address Validation API НЕ працює — на них показуємо
# попередження якщо юзер обрав "✅ З перевіркою адрес".
_UNSUPPORTED_AV_COUNTRIES = {"Thailand", "Turkey", "India"}

logger = logging.getLogger(__name__)


def get_progress_bar(current: int, total: int, length: int = 12) -> str:
    if total <= 0:
        return "░" * length
    progress = int((current / total) * length)
    percent = int((current / total) * 100)
    bar = "█" * progress + "░" * (length - progress)
    return f"`[{bar}] {percent}%`"


async def safe_answer(query, text: str | None = None, show_alert: bool = False) -> None:
    if query:
        try:
            if text:
                await query.answer(text=text, show_alert=show_alert)
            else:
                await query.answer()
        except Exception as e:
            logger.debug("safe_answer: %s", e)


async def safe_edit(query, text: str, reply_markup=None, parse_mode: str = 'Markdown') -> None:
    from telegram.error import BadRequest
    if query:
        try:
            await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        except BadRequest:
            pass


@require_auth
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Перевірка доступу — через @require_auth (декоратор бере на себе whitelist
    # + автододавання адміна + повідомлення "Доступ заборонено"). Ручну
    # дубльовану перевірку прибрано як пункт аудиту безпеки.
    if not update.message or context.user_data is None:
        return ConversationHandler.END
    context.user_data.clear()
    await update.message.reply_text("🤖 Робоча панель активована.", reply_markup=get_main_panel())

    # ── ENTRY: спочатку питаємо чи перевіряти адреси ──
    # validate_address прапор зберігається в context.user_data на весь flow
    # і використовується у scrapers/main.py:_persist_result.
    await update.message.reply_text(
        "🗺 <b>Перевіряти адреси компаній?</b>\n\n"
        "✅ <b>З перевіркою:</b> Google Address Validation API\n"
        "  • колонки 🟢 OK / 🟡 Risk / 🔴 Bad для кожної адреси\n"
        "  • ~5 000 безкоштовних/міс, далі $0.017 за компанію\n"
        "  • потрібно для anti-Circumvention перевірки Google Ads\n\n"
        "⚡ <b>Без перевірки:</b> швидше, дешевше, як було раніше",
        reply_markup=get_validate_choice_kb(),
        parse_mode="HTML",
    )
    return ASK_VALIDATE


async def validate_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Юзер обрав 'З перевіркою' / 'Без' — переходимо до вибору сайту."""
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return ASK_VALIDATE
    await safe_answer(query)

    if query.data == "validate_yes":
        context.user_data['validate_address'] = True
        prefix = "🗺 <b>Адреси будуть перевірятись через Google Maps</b>\n\n"
    elif query.data == "validate_skip":
        # Юзер натиснув "Продовжити без перевірки" в попередженні про
        # непідтриману країну — зберігаємо вибір site, вимикаємо validate.
        context.user_data['validate_address'] = False
        prefix = "⚡ <b>Без перевірки адрес</b> (країна не підтримується)\n\n"
        site = context.user_data.get('site', '')
        if site:
            await safe_edit(
                query,
                f"{prefix}🌍 Обрано: <b>{site}</b>\n\n"
                "🔎 <b>Введіть ключове слово</b> для пошуку:",
                get_back_kb("start"),
                parse_mode="HTML",
            )
            return TYPING_KEYWORD
    else:  # validate_no
        context.user_data['validate_address'] = False
        prefix = "⚡ <b>Без перевірки адрес</b> (швидкий режим)\n\n"

    await safe_edit(
        query,
        f"{prefix}🌍 <b>Оберіть сайт для пошуку:</b>",
        get_sites_kb(),
        parse_mode="HTML",
    )
    return SELECT_SITE


async def site_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return SELECT_SITE
    await safe_answer(query)
    site = query.data.replace("site_", "")
    context.user_data['site'] = site

    # Якщо юзер вибрав "З перевіркою адрес" + країну що Google не валідує —
    # показуємо попередження і даємо вибір: продовжити без перевірки чи вибрати іншу країну.
    if (context.user_data.get('validate_address')
            and site in _UNSUPPORTED_AV_COUNTRIES):
        await safe_edit(
            query,
            f"⚠️ <b>Google Address Validation API НЕ підтримує {site}</b>\n\n"
            f"Адреси з цієї країни неможливо перевірити автоматично.\n\n"
            f"Можна продовжити без перевірки (як 'швидкий режим') або обрати іншу країну.",
            get_unsupported_country_kb(),
            parse_mode="HTML",
        )
        return ASK_VALIDATE

    # India: як на data.gov.in — спершу обов'язковий вибір штату
    # (Company State Code). Далі — опційний фільтр за назвою, рік, формат.
    if site == "India":
        await safe_edit(
            query,
            "🇮🇳 Обрано: <b>India</b>\n\n"
            "Дані MCA фільтруються за <b>штатом</b> (Company State Code).\n\n"
            "🗺 <b>Спершу оберіть штат:</b>",
            get_india_state_kb(), parse_mode="HTML",
        )
        return SELECT_INDIA_STATE

    await safe_edit(query, f"🌍 Обрано: **{site}**\n\n🔎 **Введіть ключове слово** для пошуку:",
                    get_back_kb("start"))
    return TYPING_KEYWORD


async def india_state_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """India: юзер обрав Company State Code → зберігаємо і переходимо до keyword."""
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return SELECT_INDIA_STATE
    if "back" in query.data or "cancel" in query.data:
        return await handle_navigation(update, context)
    await safe_answer(query)

    from scrapers.india import INDIA_STATE_CODES
    try:
        idx = int(query.data.split("_", 1)[1])
        state = INDIA_STATE_CODES[idx]
    except (ValueError, IndexError):
        return SELECT_INDIA_STATE

    context.user_data['india_state'] = state
    await safe_edit(
        query,
        f"🇮🇳 Штат: <b>{state.title()}</b>\n\n"
        "🔎 <b>Фільтр за назвою</b> (необов'язково):\n"
        "• введіть слово — залишаться лише компанії, чия назва його містить\n"
        "• введіть <code>*</code> — <b>усі</b> компанії цього штату",
        get_back_kb("istate"), parse_mode="HTML",
    )
    return TYPING_KEYWORD


async def group_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє натискання на групу-батька (напр. 'USA') — показує submenu."""
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return SELECT_SITE
    await safe_answer(query)
    group = query.data.replace("group_", "")
    _, label = GROUP_LABELS.get(group, ("", group))
    await safe_edit(
        query,
        f"🌍 <b>{label}</b> — оберіть штат:",
        get_group_kb(group),
        parse_mode="HTML",
    )
    return SELECT_SITE


async def save_kw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_KEYWORD
    raw_kw = update.message.text.strip()

    # ── Валідація: довжина ─────────────────────────────────────────
    if not raw_kw:
        await update.message.reply_text(
            "❌ Ключове слово не може бути порожнім. Введи назву/галузь або натисни «Назад».",
            reply_markup=get_back_kb("start"),
        )
        return TYPING_KEYWORD
    if len(raw_kw) > _MAX_KEYWORD_LEN:
        await update.message.reply_text(
            f"❌ Ключ занадто довгий: {len(raw_kw)} символів. Максимум: {_MAX_KEYWORD_LEN}.",
            reply_markup=get_back_kb("start"),
        )
        return TYPING_KEYWORD

    context.user_data['kw'] = raw_kw

    # Підтримка кількох ключових слів через кому.
    # ВАЖЛИВО: parse_mode=HTML + html.escape, бо юзерський ввід може містити
    # `_` `*` backtick які ламають Markdown рендер (BadRequest → тиша).
    keywords = [k.strip() for k in raw_kw.split(',') if k.strip()]
    if len(keywords) > 1:
        kw_preview = '\n'.join(f"  • <code>{_html.escape(k)}</code>" for k in keywords)
        header = f"🔑 Ключових слів: <b>{len(keywords)}</b>\n{kw_preview}"
        note = "\n\n💡 Бот пройдеться по кожному слову і об'єднає результати."
    else:
        header = f"🔑 Ключ: <code>{_html.escape(raw_kw)}</code>"
        note = ""

    await update.message.reply_text(
        f"{header}{note}\n\n🔢 <b>Скільки компаній зібрати?</b>\n"
        "Натисніть готовий варіант або виберіть «Інше число».",
        reply_markup=get_count_quick_kb(), parse_mode="HTML"
    )
    return TYPING_COUNT


async def _advance_to_year(update_or_msg, context: ContextTypes.DEFAULT_TYPE,
                           *, edit: bool = False) -> int:
    """Спільна логіка переходу до кроку «рік». Працює і для message, і для callback.

    edit=True — редагуємо повідомлення з кнопками (callback flow);
    edit=False — надсилаємо нове (text-input flow).
    """
    text = ("📅 <b>Починаючи з якого року реєстрації шукати?</b>\n\n"
            "Бот знайде компанії від обраного року і пізніше.\n"
            "Натисніть варіант або «Інший рік» / «Усі».")
    kb = get_year_quick_kb()
    if edit:
        await safe_edit(update_or_msg, text, kb, parse_mode="HTML")
    else:
        await update_or_msg.reply_text(text, reply_markup=kb, parse_mode="HTML")
    return TYPING_YEAR


def _validate_count(raw: str) -> tuple[int | None, str]:
    """Перевіряє рядок як count. Повертає (value, error_message)."""
    if not raw.isdigit():
        return None, "❌ Будь ласка, введіть число (наприклад: 50)."
    try:
        v = int(raw)
    except ValueError:
        return None, "❌ Невірний формат числа. Приклад: 50"
    if not (1 <= v <= _MAX_COUNT):
        return None, f"❌ Кількість має бути від 1 до {_MAX_COUNT}. Ви ввели: {v}."
    return v, ""


async def count_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback-handler для кнопок [10][50]...[custom] на кроці count."""
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return TYPING_COUNT
    await safe_answer(query)

    if query.data == "count_custom":
        # Юзер натиснув "✏ Інше" — показуємо prompt і ЗАЛИШАЄМОСЬ у TYPING_COUNT.
        # save_count нижче обробить його текстове число як раніше.
        await safe_edit(
            query,
            f"🔢 <b>Введіть кількість компаній</b> (1–{_MAX_COUNT}):",
            get_back_kb("kw"), parse_mode="HTML"
        )
        return TYPING_COUNT

    # count_<N> — швидкий вибір
    try:
        count_val = int(query.data.split("_", 1)[1])
    except (ValueError, IndexError):
        return TYPING_COUNT
    if not (1 <= count_val <= _MAX_COUNT):
        return TYPING_COUNT

    context.user_data['count'] = str(count_val)
    return await _advance_to_year(query, context, edit=True)


async def save_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback: юзер ввів count текстом (через '✏ Інше число' або напряму)."""
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_COUNT
    count_val, err = _validate_count(update.message.text)
    if count_val is None:
        await update.message.reply_text(err, reply_markup=get_count_quick_kb())
        return TYPING_COUNT
    context.user_data['count'] = str(count_val)
    return await _advance_to_year(update.message, context, edit=False)


def _validate_year(raw: str) -> tuple[str | None, str]:
    """Перевіряє рядок як year. '0' = усі. Повертає (raw_value, err)."""
    if raw == "0":
        return "0", ""
    if not raw.isdigit() or len(raw) != 4:
        return None, "❌ Введіть 4-значний рік (наприклад: 2022) або 0 для всіх дат."
    year_val = int(raw)
    if not (_MIN_YEAR <= year_val <= _MAX_YEAR):
        return None, f"❌ Рік має бути в діапазоні {_MIN_YEAR}–{_MAX_YEAR}. Ви ввели: {year_val}."
    return raw, ""


async def _advance_after_year(target_send, context: ContextTypes.DEFAULT_TYPE,
                              raw_year: str, *, edit: bool = False) -> int:
    """Після того як рік встановлений — гілка UK або стандарт (format)."""
    year_note = ("📅 Рік: <b>без обмежень</b>"
                 if raw_year == "0"
                 else f"📅 Рік реєстрації: <b>від {raw_year} і пізніше</b>")

    if context.user_data.get('site') == 'UnitedKingdom':
        text = (f"{year_note}\n\n"
                "🇬🇧 <b>Режим збору UK документів:</b>\n\n"
                "📥 <b>Завантажити PDF</b> — зберегти всі файли локально\n"
                "🔗 <b>Тільки посилання</b> — швидко, лінк NEWINC у таблицю")
        kb = get_uk_mode_kb()
        next_state = SELECT_UK_MODE
    else:
        text = f"{year_note}\n\n📁 <b>Оберіть формат файлу</b> для результатів:"
        kb = get_formats_kb()
        next_state = SELECT_FORMAT

    if edit:
        await safe_edit(target_send, text, kb, parse_mode="HTML")
    else:
        await target_send.reply_text(text, reply_markup=kb, parse_mode="HTML")
    return next_state


async def year_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback-handler для кнопок [2026][2025]...[Усі][custom] на кроці year."""
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return TYPING_YEAR
    await safe_answer(query)

    if query.data == "year_custom":
        # Юзер хоче ввести свій рік — показуємо prompt, залишаємось в state
        await safe_edit(
            query,
            "📅 <b>Введіть 4-значний рік</b> (наприклад: 2022) або <code>0</code> для всіх дат:",
            get_back_kb("count"), parse_mode="HTML"
        )
        return TYPING_YEAR

    raw = query.data.split("_", 1)[1] if "_" in query.data else ""
    valid, err = _validate_year(raw)
    if valid is None:
        # Не повинно бути — наші кнопки видають валідні значення
        await safe_answer(query, text=err, show_alert=True)
        return TYPING_YEAR

    context.user_data['target_year'] = valid
    return await _advance_after_year(query, context, valid, edit=True)


async def save_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback: юзер ввів рік текстом (через '✏ Інший рік' або напряму)."""
    if not update.message or not update.message.text or context.user_data is None:
        return TYPING_YEAR

    raw = update.message.text.strip()
    valid, err = _validate_year(raw)
    if valid is None:
        await update.message.reply_text(err, reply_markup=get_year_quick_kb())
        return TYPING_YEAR

    context.user_data['target_year'] = valid
    return await _advance_after_year(update.message, context, valid, edit=False)


async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or context.user_data is None:
        return ConversationHandler.END
    await safe_answer(query)

    if query.data == "cancel_search":
        context.user_data.clear()
        await safe_edit(query, "❌ **Пошук скасовано.**")
        return ConversationHandler.END

    # HTML escape — kw/site можуть містити спецсимволи. Markdown тут ламався.
    _site_esc = _html.escape(str(context.user_data.get('site', '...')))
    _kw_esc = _html.escape(str(context.user_data.get('kw', '')))
    _year_esc = _html.escape(str(context.user_data.get('target_year', '')))
    _count_esc = _html.escape(str(context.user_data.get('count', '')))
    nav_map = {
        "back_validate": (ASK_VALIDATE,
                          "🗺 <b>Перевіряти адреси компаній?</b>",
                          get_validate_choice_kb()),
        "back_start": (SELECT_SITE, "🌍 <b>Оберіть сайт для пошуку:</b>", get_sites_kb()),
        "back_sites": (SELECT_SITE, "🌍 <b>Оберіть сайт для пошуку:</b>", get_sites_kb()),
        "back_istate": (SELECT_INDIA_STATE,
                        "🇮🇳 <b>Оберіть штат</b> (Company State Code):",
                        get_india_state_kb()),
        "back_kw": (TYPING_KEYWORD,
                    f"🔎 <b>Введіть ключове слово</b> для <code>{_site_esc}</code>:",
                    get_back_kb("start")),
        "back_count": (TYPING_COUNT,
                       f"🔢 <b>Скільки компаній зібрати?</b> (ключ: <code>{_kw_esc}</code>)\n"
                       "Натисніть варіант або «Інше число».",
                       get_count_quick_kb()),
        "back_year": (TYPING_YEAR,
                      f"📅 <b>Рік реєстрації</b> (ключ: <code>{_kw_esc}</code>, "
                      f"к-ть: <code>{_count_esc}</code>)\n"
                      "Натисніть варіант або «Інший рік» / «Усі».",
                      get_year_quick_kb()),
    }
    if query.data in nav_map:
        state, text, kb = nav_map[query.data]
        await safe_edit(query, text, kb, parse_mode="HTML")
        return state

    return ConversationHandler.END


async def stop_scraping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not update.effective_chat:
        return
    # Миттєвий popup у юзера (замість тиші до наступного status_updater циклу)
    await safe_answer(query, text="🛑 Зупиняю...", show_alert=False)
    chat_id = update.effective_chat.id
    async with _status_lock:
        if chat_id in scraping_status:
            scraping_status[chat_id]['is_running'] = False
            current = scraping_status[chat_id].get('current', 0)
        else:
            current = 0
    # Пояснюємо юзеру що зупинка асинхронна — він не думає що бот завис
    await safe_edit(
        query,
        f"🛑 <b>Зупинка процесу...</b>\n\n"
        f"⏳ Закриваю браузер та зберігаю зібрані результати "
        f"(<code>{current}</code> компаній). Це займе до {STATUS_UPDATE_SEC + 5} сек.",
        parse_mode="HTML",
    )
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
    import time as _time
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
            # Окремий ZIP-архів (для Turkey: PDF звіти ITO). Якщо встановлено —
            # status_updater відправить його як другий документ після Excel.
            'extra_zip_path':    None,
            'target_year':       ud.get('target_year', '2025'),
            'uk_download_pdf':   ud.get('uk_download_pdf', True),
            # India: обраний Company State Code (порожньо → старий режим за датою)
            'state_code':        ud.get('india_state', ''),
            'filtered_inactive': 0,   # лічильник відфільтрованих неактивних
            'filtered_duplicate': 0,  # лічильник пропущених дублікатів
            'started_at':        _time.time(),  # для ETA в status_updater
            'site':              ud.get('site', ''),
            # Прапор перевірки адрес — читається у scrapers/main.py
            'validate_address':  bool(ud.get('validate_address', False)),
            # Лічильники AV-валідації для фінального summary
            'av_ok':             0,
            'av_risk':           0,
            'av_bad':            0,
            'av_skipped':        0,
            'av_cached':         0,
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
        from state import create_tracked_task
        create_tracked_task(status_updater(context, chat_id, msg_id))
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

    import time as _time
    while True:
        # ── Короткий snapshot під локом ──
        async with _status_lock:
            st = scraping_status.get(chat_id)
            is_running = st.get('is_running', False) if st else False
            current    = st.get('current', 0)        if st else 0
            total      = st.get('max', 1)             if st else 1
            last_name  = st.get('last_name', '...')  if st else '...'
            started_at = st.get('started_at', 0)     if st else 0

        if not st or not is_running:
            break

        if current != last_count:
            p_bar = get_progress_bar(current, total)
            # ── ETA і швидкість (якщо прогрес > 0) ──
            eta_line = ""
            if current > 0 and started_at > 0:
                elapsed = max(_time.time() - started_at, 0.1)
                speed = current / elapsed  # comp/sec
                remaining = max(total - current, 0)
                if speed > 0:
                    eta_sec = int(remaining / speed)
                    eta_h, rem = divmod(eta_sec, 3600)
                    eta_m, eta_s = divmod(rem, 60)
                    if eta_h:
                        eta_str = f"{eta_h}г {eta_m}хв"
                    elif eta_m:
                        eta_str = f"{eta_m}хв {eta_s}с"
                    else:
                        eta_str = f"{eta_s}с"
                    # швидкість: /хв якщо повільно, /с якщо швидко
                    speed_str = f"{speed*60:.0f}/хв" if speed < 2 else f"{speed:.1f}/с"
                    eta_line = f"⏱ ETA: ~{eta_str}  ·  ⚡ {speed_str}\n"
            text  = (
                f"🚀 **Процес збору даних...**\n{p_bar}\n"
                f"{eta_line}\n"
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

        await asyncio.sleep(STATUS_UPDATE_SEC)

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

        # ── Підсумок Address Validation (якщо була увімкнена) ──
        if st.get('validate_address'):
            av_ok = st.get('av_ok', 0)
            av_risk = st.get('av_risk', 0)
            av_bad = st.get('av_bad', 0)
            av_skip = st.get('av_skipped', 0)
            av_cached = st.get('av_cached', 0)
            av_total = av_ok + av_risk + av_bad + av_skip
            if av_total > 0:
                stats_block += (
                    "\n\n🗺 *Перевірка адрес (Google Maps):*\n"
                    f"  🟢 OK: `{av_ok}` | 🟡 Risk: `{av_risk}` | 🔴 Bad: `{av_bad}`"
                )
                if av_skip:
                    stats_block += f"\n  ⏭ Skip/Error: `{av_skip}`"
                if av_cached:
                    stats_block += f"\n  💾 З кешу: `{av_cached}`"

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

            # Додатковий ZIP (Turkey PDF звіти) — відправляємо окремим документом.
            # Файл залишається на диску в turkey_reports/ для повторного доступу.
            extra_zip = st.get('extra_zip_path')
            if extra_zip and os.path.exists(extra_zip):
                try:
                    with open(extra_zip, 'rb') as f:
                        await context.bot.send_document(
                            chat_id=chat_id, document=f,
                            caption="📑 PDF-звіти ITO (Firma Detayları)"
                        )
                except Exception as e:
                    logger.error("Помилка відправки ZIP з PDF: %s", e)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ **Збір зупинено.** Дані не знайдено або процес перервано.{stats_block}"
                ),
                parse_mode="Markdown", reply_markup=repeat_kb
            )


@require_auth
async def repeat_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # @require_auth сам обробляє callback-query: якщо доступу немає,
    # робить query.answer("Доступ заборонено", show_alert=True) і return.
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
