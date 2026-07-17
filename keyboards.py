from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

import proxy.manager as proxy_manager
from config import SCRAPER_CONFIG

# Опційний маппінг: emoji + display-назва для груп. Якщо групи нема в мапі —
# беремо емоджі першого члена групи і назву = ключ групи.
GROUP_LABELS: dict[str, tuple[str, str]] = {
    "USA": ("🇺🇸", "USA"),
}


def get_sites_kb() -> InlineKeyboardMarkup:
    """Головне меню вибору сайту.

    Логіка групування: якщо у запису SCRAPER_CONFIG є поле `group` — він
    показується не окремою кнопкою, а під одним батьківським пунктом
    (callback_data=group_<NAME>). Записи без `group` рендеряться як раніше.
    Порядок: групи рендеряться у тому місці, де зустрічається перший їх член.
    """
    buttons: list[list[InlineKeyboardButton]] = []
    rendered_groups: set[str] = set()

    for k, v in SCRAPER_CONFIG.items():
        group = v.get("group")
        if group:
            if group in rendered_groups:
                continue
            rendered_groups.add(group)
            flag, label = GROUP_LABELS.get(group, (v.get("flag", "🌍"), group))
            buttons.append([InlineKeyboardButton(
                f"{flag} {label}", callback_data=f"group_{group}"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                f"{v['flag']} {k}", callback_data=f"site_{k}"
            )])

    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")])
    return InlineKeyboardMarkup(buttons)


def get_group_kb(group_name: str) -> InlineKeyboardMarkup:
    """Submenu членів групи (наприклад USA → California, Washington)."""
    buttons: list[list[InlineKeyboardButton]] = []
    for k, v in SCRAPER_CONFIG.items():
        if v.get("group") != group_name:
            continue
        buttons.append([InlineKeyboardButton(
            f"{v['flag']} {k}", callback_data=f"site_{k}"
        )])
    buttons.append([
        InlineKeyboardButton("🔙 Назад", callback_data="back_sites"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search"),
    ])
    return InlineKeyboardMarkup(buttons)


def get_validate_choice_kb() -> InlineKeyboardMarkup:
    """Клавіатура: 'Перевіряти адреси компаній?' — перший крок пошуку юр.доків."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так — з перевіркою адрес",
                              callback_data="validate_yes")],
        [InlineKeyboardButton("⚡ Ні — швидко, без перевірки",
                              callback_data="validate_no")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")],
    ])


def get_unsupported_country_kb() -> InlineKeyboardMarkup:
    """Попередження що Google не валідує адреси у вибраній країні."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶ Продовжити без перевірки",
                              callback_data="validate_skip")],
        [InlineKeyboardButton("⬅ Обрати іншу країну",
                              callback_data="back_validate")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")],
    ])


def get_back_kb(target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Назад", callback_data=f"back_{target}"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")
    ]])


# ── Quick-pick кнопки для кількості та року ──────────────────────────────
# Замінюють текстовий ввід найчастіших значень — 5с замість 15с на крок.
# Останньою кнопкою завжди "✏ Інше" → юзер залишається в state і вводить
# своє число, як раніше.

# Найчастіші значення count. Якщо потрібно частіше міняти — це єдине місце.
QUICK_COUNT_VALUES: tuple[int, ...] = (10, 50, 100, 250, 500, 1000)


def get_count_quick_kb() -> InlineKeyboardMarkup:
    """Швидкі кнопки для вибору кількості компаній + 'Інше' для custom-вводу."""
    rows: list[list[InlineKeyboardButton]] = []
    # По 3 в ряд — щоб 6 значень рівно заповнили 2 ряди
    for i in range(0, len(QUICK_COUNT_VALUES), 3):
        chunk = QUICK_COUNT_VALUES[i:i + 3]
        rows.append([
            InlineKeyboardButton(str(v), callback_data=f"count_{v}")
            for v in chunk
        ])
    rows.append([InlineKeyboardButton("✏ Інше число", callback_data="count_custom")])
    rows.append([
        InlineKeyboardButton("🔙 Назад", callback_data="back_kw"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search"),
    ])
    return InlineKeyboardMarkup(rows)


def get_year_quick_kb() -> InlineKeyboardMarkup:
    """Швидкі кнопки для року реєстрації + 'Усі' (0) + 'Інший' для custom.

    Поточний і 3 попередні роки — найчастіший випадок. Розраховуємо
    динамічно щоб не оновлювати щороку.
    """
    from datetime import datetime
    now_year = datetime.now().year
    rows: list[list[InlineKeyboardButton]] = []
    # [Поточний] [-1] [-2]  у першому ряду
    rows.append([
        InlineKeyboardButton(f"📅 {now_year}",     callback_data=f"year_{now_year}"),
        InlineKeyboardButton(f"📅 {now_year - 1}", callback_data=f"year_{now_year - 1}"),
        InlineKeyboardButton(f"📅 {now_year - 2}", callback_data=f"year_{now_year - 2}"),
    ])
    # [-3] [-4] [Усі] у другому
    rows.append([
        InlineKeyboardButton(f"📅 {now_year - 3}", callback_data=f"year_{now_year - 3}"),
        InlineKeyboardButton(f"📅 {now_year - 4}", callback_data=f"year_{now_year - 4}"),
        InlineKeyboardButton("⏰ Усі",              callback_data="year_0"),
    ])
    rows.append([InlineKeyboardButton("✏ Інший рік", callback_data="year_custom")])
    rows.append([
        InlineKeyboardButton("🔙 Назад", callback_data="back_count"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search"),
    ])
    return InlineKeyboardMarkup(rows)


def get_india_state_kb() -> InlineKeyboardMarkup:
    """Вибір Company State Code для India (37 штатів, як на data.gov.in).

    callback_data=istate_<індекс> — індекс у INDIA_STATE_CODES (уникаємо
    пробілів/`&` у callback_data). По 2 в ряд щоб влізло у вікно Telegram.
    """
    from scrapers.india import INDIA_STATE_CODES

    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(INDIA_STATE_CODES), 2):
        rows.append([
            InlineKeyboardButton(INDIA_STATE_CODES[j].title(), callback_data=f"istate_{j}")
            for j in range(i, min(i + 2, len(INDIA_STATE_CODES)))
        ])
    rows.append([
        InlineKeyboardButton("🔙 Назад", callback_data="back_sites"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search"),
    ])
    return InlineKeyboardMarkup(rows)


def get_formats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 TXT", callback_data="fmt_TXT"),
         InlineKeyboardButton("📊 Excel", callback_data="fmt_EXCEL")],
        [InlineKeyboardButton("🧩 JSON", callback_data="fmt_JSON")],
        [InlineKeyboardButton("📅 Запланувати (Excel)", callback_data="fmt_SCHEDULE")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_count"),
         InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")]
    ])


def get_uk_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Завантажити PDF на диск", callback_data="ukmode_download")],
        [InlineKeyboardButton("🔗 Тільки посилання в таблицю", callback_data="ukmode_links")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_year"),
         InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")]
    ])


def get_main_panel() -> ReplyKeyboardMarkup:
    # Розкладка оптимізована під частоту використання:
    #  1-й ряд — найчастіші дії (пошук компаній + AI перевірка)
    #  2-й ряд — історія + генерація документів
    #  3-й ряд — інфо (об'єднаний статус замість двох 📊 кнопок)
    #  4-й ряд — налаштування + допомога
    # Кнопку "🔄 Перезапустити бота" прибрано з reply keyboard — випадковий тап
    # вбивав активний скрапінг. Тепер тільки через команду /restart (адмін).
    keyboard = [
        ['🔍 Пошук юр. доків',  '📁 Перевірка фіз. доків'],
        ['📋 Історія',           '🪪 Документи'],
        ['📊 Статус бота',       '❓ Допомога'],
        ['🌐 Налаштування проксі', '⚙️ Налаштування'],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_settings_kb() -> InlineKeyboardMarkup:
    """Меню адмін-налаштувань: очистка кешу та інші службові дії."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Очистити кеш OCR",
                              callback_data="settings_clear_ocr")],
        [InlineKeyboardButton("📊 Статистика кешу",
                              callback_data="settings_cache_stats")],
        [InlineKeyboardButton("🔙 Закрити", callback_data="settings_close")],
    ])


def get_settings_confirm_clear_kb() -> InlineKeyboardMarkup:
    """Підтвердження очистки кешу OCR (дія незворотна)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, очистити",
                              callback_data="settings_clear_ocr_confirm")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="settings_back")],
    ])


def get_stop_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Зупинити збір", callback_data="stop_scraping")]])


def get_proxy_kb() -> InlineKeyboardMarkup:
    is_used = proxy_manager.get_use_proxy()
    status_emoji = "✅ Увімкнено" if is_used else "❌ Вимкнено"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📂 Завантажити з файлу (.txt)", callback_data="proxy_upload_info")],
        [InlineKeyboardButton("🔄 Перевірити робочі проксі", callback_data="proxy_check")],
        [InlineKeyboardButton("🗑 Очистити весь список", callback_data="proxy_clear")],
        [InlineKeyboardButton(f"Статус: {status_emoji}", callback_data="toggle_proxy")],
        [InlineKeyboardButton("🔙 Закрити меню", callback_data="close_proxy")]
    ])


def get_check_geo_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇫🇷 Франція", callback_data="checkgeo_France"),
         InlineKeyboardButton("🇫🇮 Фінляндія", callback_data="checkgeo_Finland")],
        [InlineKeyboardButton("🌍 Загальні", callback_data="checkgeo_General")],
        [InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]
    ])


def get_schedule_kb() -> InlineKeyboardMarkup:
    from handlers.schedule import CRON_OPTIONS
    buttons = [[InlineKeyboardButton(label, callback_data=f"sched_{cron}")]
               for label, cron in CRON_OPTIONS.items()]
    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")])
    return InlineKeyboardMarkup(buttons)
