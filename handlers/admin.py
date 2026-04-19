import functools
import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

import database
from config import ADMIN_ID
from constants import COUNTRY_FLAGS
from state import scraping_status

logger = logging.getLogger(__name__)


def is_admin(update: Update) -> bool:
    """Перевіряє, чи є відправник адміністратором бота."""
    if not ADMIN_ID:
        return True  # якщо ADMIN_ID не задано — дозволяємо всім (режим розробки)
    user = update.effective_user
    return user is not None and user.id == ADMIN_ID


def require_auth(func):
    """Декоратор: перевіряє whitelist перед виконанням хендлера."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        # Адмін завжди має доступ
        if ADMIN_ID and user.id == ADMIN_ID:
            database.add_user(user.id, user.username or "", role="admin")
            return await func(update, context, *args, **kwargs)
        # Перевірка whitelist
        if not database.is_user_allowed(user.id):
            msg = (
                "⛔️ **Доступ заборонено**\n\n"
                "Ти не в списку дозволених користувачів.\n"
                f"Твій ID: `{user.id}`\n\n"
                "Зверніться до адміністратора для отримання доступу."
            )
            if update.message:
                await update.message.reply_text(msg, parse_mode="Markdown")
            elif update.callback_query:
                await update.callback_query.answer("⛔️ Доступ заборонено", show_alert=True)
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


@require_auth
async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_stats = database.get_global_stats()

    text = (
        "📊 **Статистика сесії**\n\n"
        "🟢 **Статус бота:** Активний\n"
        f"🤖 **Поточних завдань:** `{len(scraping_status)}`\n"
        "🛡 **Захист Cloudflare:** Обхід увімкнено (DrissionPage)\n\n"
    )

    if db_stats:
        text += "🌍 **ГЛОБАЛЬНА СТАТИСТИКА:**\n"
        text += f"🏢 Всього зібрано: **{db_stats['total']}** компаній\n"
        text += f"📅 Додано сьогодні: **{db_stats['today']}**\n\n"
        if db_stats['by_country']:
            text += "📊 **По країнах:**\n"
            for country, count in db_stats['by_country']:
                flag = COUNTRY_FLAGS.get(country, "📍")
                text += f"{flag} {country}: **{count}**\n"
    else:
        text += "_⚠️ Глобальна статистика наразі недоступна._"

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")


def _help_nav_kb(current: str) -> InlineKeyboardMarkup:
    """Навігація між розділами довідки."""
    sections = [
        ("🔍 Пошук",       "help_search"),
        ("📁 AI Сортер",   "help_docs"),
        ("📋 Команди",     "help_cmds"),
        ("🌐 Проксі",      "help_proxy"),
        ("⏱ Розклад",      "help_sched"),
    ]
    row = []
    for label, cb in sections:
        if cb == current:
            row.append(InlineKeyboardButton(f"[{label.split()[1]}]", callback_data="noop"))
        else:
            row.append(InlineKeyboardButton(label, callback_data=cb))
    return InlineKeyboardMarkup([row[:3], row[3:]])


def _esc(text: str) -> str:
    """Екранує спеціальні символи для MarkdownV2."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def _section_search() -> str:
    return (
        "\U0001F50D *Пошук юридичних документів*\n"
        "\n"
        "*Як запустити:*\n"
        "1\\. Натисни `\U0001F50D Пошук юр\\. доків`\n"
        "2\\. Обери країну зі списку\n"
        "3\\. Введи ключове слово \\(назва або галузь\\)\n"
        "4\\. Вкажи кількість компаній \\(напр: `20`\\)\n"
        "5\\. Введи рік — бот знайде компанії *ВІД цього року і пізніше*\n"
        "   Наприклад: `2022` → компанії 2022, 2023, 2024\\.\\.\\.\n"
        "   Або `0` — без обмежень по даті\n"
        "6\\. Для \U0001F1EC\U0001F1E7 UK — обери режим PDF\n"
        "7\\. Оберіть формат \\→ отримаєш файл у чаті\n"
        "\n"
        "*Доступні країни:*\n"
        "\U0001F1EB\U0001F1F7 Франція \U0001F1EB\U0001F1EE Фінляндія \U0001F1E9\U0001F1F0 Данія\n"
        "\U0001F1E8\U0001F1FF Чехія \U0001F1F1\U0001F1FB Латвія \U0001F1EC\U0001F1E7 Велика Британія\n"
        "\U0001F1FA\U0001F1F8 Каліфорнія \U0001F1F3\U0001F1FF Нова Зеландія\n"
        "\U0001F1F9\U0001F1ED Таїланд \U0001F1F9\U0001F1F7 Туреччина\n"
        "\n"
        "*Підтримка фільтру за роком:*\n"
        "\U0001F1EB\U0001F1F7 Франція — так \\(pappers\\.ai JSON API\\)\n"
        "\U0001F1EC\U0001F1E7 Велика Британія — так \\(Companies House API\\)\n"
        "Інші країни — фільтр за роком не застосовується\n"
        "\n"
        "*Формати результату:*\n"
        "\U0001F4C4 TXT \U0001F4CA Excel \U0001F9E9 JSON \U0001F4C5 Запланувати\n"
        "\n"
        "\U0001F4A1 _Порада: Excel — для таблиць, JSON — для інтеграцій_"
    )


def _section_docs() -> str:
    return (
        "\U0001F4C1 *AI Сортер фізичних документів*\n"
        "\n"
        "*Що робить:*\n"
        "Перевіряє термін дії паспортів та ID\\-карт,\n"
        "сортує клієнтів на придатних і непридатних\\.\n"
        "\n"
        "*Як використовувати:*\n"
        "1\\. Натисни `\U0001F4C1 Перевірка фіз\\. доків`\n"
        "   \U000026A0 _Без цієї кнопки посилання Google Drive_\n"
        "   _не активує аналіз \\(запобігає випадковим запускам\\)_\n"
        "2\\. Підготуй ZIP\\-архів:\n"
        "   • Кожна папка \\= один клієнт\n"
        "   • Всередині — фото документа \\(JPG/PNG\\)\n"
        "   • Назва папки \\= ім'я клієнта\n"
        "3\\. Надішли ZIP у чат \\(до 50 МБ\\)\n"
        "   або посилання Google Drive \\(до 500 МБ\\)\n"
        "   Вікно очікування після натискання — *30 хв*\n"
        "4\\. Стеж за прогресом у реальному часі\n"
        "5\\. Після завершення обери спосіб доставки:\n"
        "\n"
        "*Варіанти доставки результатів:*\n"
        "\U0001F4AC Telegram — безпосередньо в чат \\(архів \\+ Excel\\)\n"
        "\U0001F4E6 AWS S3 — хмарне посилання на 1 / 7 / 30 днів\n"
        "\U0001F4E1 Канал — надіслати в заданий TG\\-канал\n"
        "\n"
        "*Структура результату:*\n"
        "`Sorted\\_Result\\.zip`\n"
        "`├── ✅ Придатні/`\n"
        "`└── ❌ Не придатні/`\n"
        "\n"
        "*Як розпізнає дати:*\n"
        "Етап 1 — AWS Textract \\(швидко і дешево\\)\n"
        "Етап 2 — GPT\\-4o Vision \\(якщо Textract не впорався\\)\n"
        "Кеш — повторні фото не надсилаються в API\n"
        "\n"
        "/myresults — переглянути останні сесії\n"
        "/cleanup \\[днів\\] — видалити сесії старіші N днів\n"
        "\n"
        "\U000026A0 _Ліміт ZIP у чаті: 50 МБ \\| через Google Drive: до 500 МБ_"
    )


def _section_cmds() -> str:
    return (
        "\U0001F4CB *Команди користувача*\n"
        "\n"
        "*Пошук компаній:*\n"
        "/start — відкрити головне меню\n"
        "/history — останні 10 пошуків з кнопкою 🔄 Повторити\n"
        "/schedule — активні заплановані задачі\n"
        "/digest — нові компанії за 24 год\n"
        "/digest 48 — нові компанії за 48 год\n"
        "/status — uptime, активні задачі, навантаження\n"
        "\n"
        "*AI Сортер документів:*\n"
        "/myresults — переглянути останні 5 сесій аналізу\n"
        "/cleanup — видалити сесії старіші 30 днів \\(за замовчуванням\\)\n"
        "/cleanup 7 — видалити сесії старіші 7 днів\n"
        "\n"
        "*Кнопки меню:*\n"
        "`\U0001F50D Пошук юр\\. доків` — запустити збір компаній\n"
        "`\U0001F4C1 Перевірка фіз\\. доків` — AI сортер документів\n"
        "`\U0001F4CA Статистика` — кількість зібраних компаній\n"
        "`\U0001F4CA Статус бота` — uptime та навантаження\n"
        "`\U0001F4CB Історія` — швидкий доступ до /history\n"
        "`\U0001F310 Налаштування проксі` — керування проксі\n"
        "`\U0001F504 Перезапустити бота` — тільки адмін\n"
        "\n"
        "\U0001F4A1 _Під час пошуку натисни_ `\U0001F6D1 Зупинити збір` _—_\n"
        "_бот збереже вже знайдені результати та надішле файл_\n"
        "\n"
        "\U0001F4A1 _Під час аналізу документів натисни_ `\U0001F6AB Скасувати` _—_\n"
        "_аналіз зупиниться, вже оброблені результати збережуться_"
    )


def _section_proxy() -> str:
    return (
        "\U0001F310 *Налаштування проксі*\n"
        "\n"
        "*Навіщо потрібні:*\n"
        "Сайти блокують повторні запити з однієї IP\\.\n"
        "Проксі допомагають обходити ці обмеження\\.\n"
        "\n"
        "*Додати через файл:*\n"
        "1\\. Створи `.txt` файл\n"
        "2\\. Кожен рядок у форматі:\n"
        "   `ip:port:user:pass`\n"
        "3\\. Надішли файл у чат → обери країну\n"
        "\n"
        "*Додати вручну \\(один рядок у чат\\):*\n"
        "`192\\.168\\.1\\.1:8080:login:password`\n"
        "\n"
        "*Меню \\(`\U0001F310 Налаштування проксі`\\):*\n"
        "• Перевірити — пінгує кожен проксі\n"
        "• Видалити неробочі — автоматично після перевірки\n"
        "• Увімкнути / Вимкнути — toggle\n"
        "• Очистити — скидає весь список\n"
        "\n"
        "\U0001F4A1 _Проксі прив'язуються до країни\\.\n"
        "Франція і Фінляндія мають окремі пули_"
    )


def _section_sched() -> str:
    return (
        "\U000023F1 *Планувальник*\n"
        "\n"
        "*Як запланувати пошук:*\n"
        "1\\. Запусти пошук звичайним способом\n"
        "2\\. На кроці вибору формату натисни:\n"
        "   `\U0001F4C5 Запланувати \\(Excel\\)`\n"
        "3\\. Обери розклад запуску:\n"
        "   • Щодня о 08:00 або 20:00\n"
        "   • Щопонеділка о 09:00\n"
        "   • Кожні 12 годин\n"
        "\n"
        "*Керування задачами:*\n"
        "/schedule — список активних задач\n"
        "Натисни `\U0001F5D1 Видалити` поруч із задачею — скасувати\n"
        "\n"
        "*Дайджест нових компаній:*\n"
        "/digest — нові за 24 год \\(по країнах\\)\n"
        "/digest 48 — нові за 48 год\n"
        "\n"
        "*Timezone:*\n"
        "Розклад прив'язаний до часового поясу з `token\\.env`\n"
        "Змінна: `BOT\\_TIMEZONE=Europe/Kyiv`\n"
        "\n"
        "\U0001F4A1 _Після перезапуску бота всі задачі\n"
        "автоматично відновлюються з БД_"
    )


def _get_admin_help() -> str:
    return (
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "\U0001F451 *Команди адміністратора*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "*Користувачі:*\n"
        "/users — список з ролями та статусом \\(🟢 активний / 🔴 заблокований\\)\n"
        "/adduser `<chat\\_id>` — додати як user\n"
        "/adduser `<chat\\_id>` admin — додати як адміна\n"
        "_Якщо юзер був заблокований — автоматично реактивується_\n"
        "/removeuser `<chat\\_id>` — заблокувати \\(не видаляє з БД\\)\n"
        "/unblockuser `<chat\\_id>` — розблокувати раніше заблокованого\n"
        "\n"
        "*AI Сортер — адмін:*\n"
        "/analysislogs — лог останніх 20 сесій аналізу \\(хто, коли, скільки, результат\\)\n"
        "/cleanup \\[днів\\] — вручну очистити старі сесії з Desktop\n"
        "\n"
        "*Система:*\n"
        "/restart — перезапустити бот\n"
        "/status — uptime, активні задачі, ліміти\n"
        "Health\\-check: `http://localhost:8080/health`\n"
        "\n"
        "\U0001F4A1 *Як додати нового користувача:*\n"
        "Нехай напише `/start` — бот поверне його `chat\\_id`\\.\n"
        "Потім виконай: `/adduser 123456789`"
    )


_HELP_SECTIONS = {
    "help_search": "search",
    "help_docs":   "docs",
    "help_cmds":   "cmds",
    "help_proxy":  "proxy",
    "help_sched":  "sched",
}

_SECTION_RENDERERS = {
    "search": _section_search,
    "docs":   _section_docs,
    "cmds":   _section_cmds,
    "proxy":  _section_proxy,
    "sched":  _section_sched,
}


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    role = database.get_user_role(update.effective_user.id)
    is_adm = is_admin(update) or role == "admin"
    text = _section_search()
    if is_adm:
        text += _get_admin_help()
    await update.message.reply_text(
        text, parse_mode="MarkdownV2",
        reply_markup=_help_nav_kb("help_search")
    )


async def help_section_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Навігація між розділами довідки."""
    query = update.callback_query
    if not query or not query.data or not update.effective_user:
        return
    from handlers.scraping import safe_answer
    await safe_answer(query)
    if query.data == "noop":
        return
    section_key = _HELP_SECTIONS.get(query.data)
    if not section_key:
        return
    renderer = _SECTION_RENDERERS.get(section_key)
    if not renderer:
        return
    text = renderer()
    if section_key == "cmds":
        role = database.get_user_role(update.effective_user.id)
        if is_admin(update) or role == "admin":
            text += _get_admin_help()
    try:
        await query.edit_message_text(
            text, parse_mode="MarkdownV2",
            reply_markup=_help_nav_kb(query.data)
        )
    except Exception:
        pass


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /users — список користувачів (тільки адмін)."""
    if not update.message or not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ Тільки для адміна.")
        return
    users = database.get_all_users()
    if not users:
        await update.message.reply_text("📋 Список користувачів порожній.")
        return
    from html import escape as _h_escape
    lines = ["👥 <b>Список користувачів:</b>\n"]
    for u in users:
        role_icon = "👑" if u["role"] == "admin" else "👤"
        status = "🟢" if u["is_active"] else "🔴"
        username = f"@{_h_escape(u['username'])}" if u["username"] else "—"
        lines.append(f"{status} {role_icon} <code>{u['chat_id']}</code> {username} — <i>{_h_escape(u['role'])}</i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_adduser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /adduser <chat_id> [role] — додати користувача (тільки адмін)."""
    if not update.message or not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ Тільки для адміна.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Використання: `/adduser <chat_id> [user|admin]`",
            parse_mode="Markdown"
        )
        return
    try:
        target_id = int(args[0])
        role = args[1] if len(args) > 1 and args[1] in ("user", "admin") else "user"
        database.add_user(target_id, "", role=role)
        await update.message.reply_text(
            f"✅ Користувача `{target_id}` додано з роллю **{role}**.",
            parse_mode="Markdown"
        )
    except ValueError:
        await update.message.reply_text("❌ Невірний chat_id. Має бути числом.")


async def cmd_removeuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /removeuser <chat_id> — заблокувати користувача (тільки адмін)."""
    if not update.message or not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ Тільки для адміна.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Використання: `/removeuser <chat_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(args[0])
        # Захист від самоблокування адміна
        admin_user = update.effective_user
        if admin_user and target_id == admin_user.id:
            await update.message.reply_text("⚠️ Не можна заблокувати сам себе.")
            return
        database.set_user_active(target_id, False)
        await update.message.reply_text(f"🚫 Користувача `{target_id}` заблоковано.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Невірний chat_id.")


async def cmd_unblockuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /unblockuser <chat_id> — розблокувати раніше заблокованого юзера.

    Сематично відрізняється від /adduser: не створює нових юзерів, тільки
    реактивує існуючих у whitelist. Якщо юзера взагалі нема в БД — повідомляє.
    """
    if not update.message or not is_admin(update):
        if update.message:
            await update.message.reply_text("⛔️ Тільки для адміна.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Використання: `/unblockuser <chat_id>`", parse_mode="Markdown"
        )
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Невірний chat_id.")
        return

    user = database.get_user(target_id)
    if user is None:
        await update.message.reply_text(
            f"⚠️ Користувача `{target_id}` немає в БД. "
            "Використайте `/adduser` щоб додати нового.",
            parse_mode="Markdown"
        )
        return
    if bool(user["is_active"]):
        await update.message.reply_text(
            f"ℹ️ Користувач `{target_id}` вже активний.", parse_mode="Markdown"
        )
        return
    database.set_user_active(target_id, True)
    await update.message.reply_text(
        f"✅ Користувача `{target_id}` розблоковано.", parse_mode="Markdown"
    )


# ─────────────────────────────────────────────
#  ІСТОРІЯ ПОШУКІВ
# ─────────────────────────────────────────────

@require_auth
async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /history — останні 10 пошуків користувача."""
    if not update.message or not update.effective_user:
        return
    chat_id = update.effective_user.id
    history = database.get_search_history(chat_id, limit=10)
    if not history:
        await update.message.reply_text("📋 Історія пошуків порожня.")
        return

    lines = ["📋 **Останні пошуки:**\n"]
    buttons = []
    from html import escape as _h_escape
    for i, h in enumerate(history, 1):
        flag = COUNTRY_FLAGS.get(h["site"], "📍")
        # started_at може бути datetime (Python 3.12 SQLite adapters) або str (legacy).
        # Нормалізуємо до "YYYY-MM-DD HH:MM" безвідносно вхідного типу.
        _raw = h["started_at"]
        if _raw is None:
            dt = "—"
        elif hasattr(_raw, "strftime"):
            dt = _raw.strftime("%Y-%m-%d %H:%M")
        else:
            dt = str(_raw)[:16]
        kw_safe = _h_escape(str(h['keyword']))
        site_safe = _h_escape(str(h['site']))
        lines.append(f"<code>{i}.</code> {flag} <b>{site_safe}</b> — <code>{kw_safe}</code> x{h['count']} [{dt}]")
        # Кнопка: обрізаємо keyword до 30 символів щоб не вийти за ліміт callback (64 байти запас для "repeat_<id>")
        btn_label = f"🔄 #{i}: {h['site']} / {str(h['keyword'])[:30]}"
        buttons.append([InlineKeyboardButton(
            btn_label,
            callback_data=f"repeat_{h['id']}"
        )])

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@require_auth
async def repeat_from_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback: повторити пошук з історії."""
    import asyncio
    import threading
    from scrapers.main import run_scraping
    from state import scraping_status, _status_lock, MAX_PARALLEL_TASKS
    from keyboards import get_stop_kb
    from handlers.scraping import status_updater

    query = update.callback_query
    if not query or not query.data or not update.effective_user:
        return
    from handlers.scraping import safe_answer
    await safe_answer(query)

    try:
        history_id = int(query.data.replace("repeat_", ""))
    except (ValueError, TypeError):
        await safe_answer(query, "❌ Невірний ID", show_alert=True)
        return
    chat_id = update.effective_user.id
    history = database.get_search_history(chat_id, limit=50)
    item = next((h for h in history if h["id"] == history_id), None)

    if not item:
        await query.answer("❌ Запис не знайдено", show_alert=True)
        return

    # Заповнюємо user_data і запускаємо
    if context.user_data is None:
        return
    # Для UK: режим PDF/links не зберігається в історії, тому спершу питаємо.
    # Раніше хардкодно було False — юзер що хотів PDF їх не отримував.
    if item["site"] == "UnitedKingdom":
        import uuid as _uuid
        token = _uuid.uuid4().hex[:10]
        pending = context.bot_data.setdefault("pending_repeat", {})
        pending[token] = {"history_id": history_id, "chat_id": chat_id}
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📥 Завантажити PDF", callback_data=f"rpt_uk_pdf_{token}"),
            InlineKeyboardButton("🔗 Тільки посилання", callback_data=f"rpt_uk_lnk_{token}"),
        ]])
        try:
            await query.edit_message_text(
                f"🇬🇧 Повторюємо пошук UK: <code>{item['keyword']}</code>\n\n"
                f"Обери режим завантаження:",
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception:
            pass
        return
    context.user_data.update({
        "site": item["site"],
        "kw": item["keyword"],
        "count": str(item["count"]),
        "target_year": item["year"],
        "uk_download_pdf": False,
    })

    async with _status_lock:
        active_tasks = sum(1 for s in scraping_status.values() if s.get("is_running"))
        if active_tasks >= MAX_PARALLEL_TASKS:
            await query.answer("⏳ Черга заповнена, спробуй пізніше", show_alert=True)
            return
        scraping_status[chat_id] = {
            "current": 0, "max": item["count"],
            "last_name": "Повтор пошуку...", "is_running": True,
            "file_path": None, "target_year": item["year"],
            "uk_download_pdf": False, "site": item["site"],
        }

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔄 Повторюю пошук: **{item['site']}** / `{item['keyword']}`",
        parse_mode="Markdown",
        reply_markup=get_stop_kb()
    )

    threading.Thread(
        target=run_scraping,
        args=(chat_id, item["keyword"], item["count"],
              item["site"], item["file_format"], scraping_status[chat_id]),
        daemon=True
    ).start()

    asyncio.create_task(status_updater(context, chat_id, msg.message_id))


async def repeat_uk_mode_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє вибір режиму PDF/links при повторі UK пошуку з історії."""
    import asyncio
    import threading
    from scrapers.main import run_scraping
    from state import scraping_status, _status_lock, MAX_PARALLEL_TASKS
    from keyboards import get_stop_kb
    from handlers.scraping import status_updater, safe_answer
    import time as _time

    query = update.callback_query
    if not query or not query.data or not update.effective_user:
        return
    await safe_answer(query)

    data = query.data  # rpt_uk_pdf_<token> або rpt_uk_lnk_<token>
    download_pdf = data.startswith("rpt_uk_pdf_")
    token = data.rsplit("_", 1)[-1]
    pending = context.bot_data.get("pending_repeat", {})
    info = pending.pop(token, None)
    if not info:
        try:
            await query.edit_message_text("⌛ Сесія вибору прострочена. Натисни «Повторити» знову.")
        except Exception:
            pass
        return

    history_id = info["history_id"]
    chat_id = info["chat_id"]
    history = database.get_search_history(chat_id, limit=50)
    item = next((h for h in history if h["id"] == history_id), None)
    if not item:
        try:
            await query.edit_message_text("❌ Запис з історії зник.")
        except Exception:
            pass
        return

    if context.user_data is None:
        return
    context.user_data.update({
        "site": item["site"],
        "kw": item["keyword"],
        "count": str(item["count"]),
        "target_year": item["year"],
        "uk_download_pdf": download_pdf,
    })

    async with _status_lock:
        active_tasks = sum(1 for s in scraping_status.values() if s.get("is_running"))
        if active_tasks >= MAX_PARALLEL_TASKS:
            try:
                await query.edit_message_text("⏳ Черга заповнена, спробуй пізніше.")
            except Exception:
                pass
            return
        scraping_status[chat_id] = {
            "current": 0, "max": item["count"],
            "last_name": "Повтор пошуку...", "is_running": True,
            "file_path": None, "target_year": item["year"],
            "uk_download_pdf": download_pdf, "site": item["site"],
            "started_at": _time.time(),
        }

    mode_label = "📥 PDF" if download_pdf else "🔗 Посилання"
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔄 Повторюю UK пошук ({mode_label}): <code>{item['keyword']}</code>",
        parse_mode="HTML",
        reply_markup=get_stop_kb(),
    )
    try:
        await query.delete_message()
    except Exception:
        pass

    threading.Thread(
        target=run_scraping,
        args=(chat_id, item["keyword"], item["count"],
              item["site"], item["file_format"], scraping_status[chat_id]),
        daemon=True,
    ).start()

    asyncio.create_task(status_updater(context, chat_id, msg.message_id))
