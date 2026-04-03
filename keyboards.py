import importlib

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

import proxy_settings
from config import SCRAPER_CONFIG


def get_sites_kb() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(f"{v['flag']} {k}", callback_data=f"site_{k}")]
        for k, v in SCRAPER_CONFIG.items()
    ]
    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")])
    return InlineKeyboardMarkup(buttons)


def get_back_kb(target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Назад", callback_data=f"back_{target}"),
        InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")
    ]])


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
    keyboard = [
        ['🔍 Пошук юр. доків', '📁 Перевірка фіз. доків'],
        ['🪪 Документи',        '🌐 Налаштування проксі'],
        ['📊 Статистика',       '📋 Історія'],
        ['📊 Статус бота',      '❓ Допомога'],
        ['🔄 Перезапустити бота']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_stop_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Зупинити збір", callback_data="stop_scraping")]])


def get_proxy_kb() -> InlineKeyboardMarkup:
    importlib.reload(proxy_settings)
    is_used = getattr(proxy_settings, 'USE_PROXY', False)
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
    from handlers_schedule import CRON_OPTIONS
    buttons = [[InlineKeyboardButton(label, callback_data=f"sched_{cron}")]
               for label, cron in CRON_OPTIONS.items()]
    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="cancel_search")])
    return InlineKeyboardMarkup(buttons)
