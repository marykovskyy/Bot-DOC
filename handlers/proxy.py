import asyncio
import logging
import os
import re
import tempfile

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

import proxy.manager as proxy_manager
from keyboards import get_proxy_kb, get_check_geo_kb

logger = logging.getLogger(__name__)


def _load_proxy_data() -> tuple[bool, dict]:
    """Читає поточний стан проксі з proxy_settings.json."""
    data = proxy_manager.load()
    proxies = data.get("proxies", {})
    if not isinstance(proxies, dict):
        proxies = {"France": [], "Finland": [], "General": []}
    return bool(data.get("use_proxy", False)), proxies


def _save_proxy_data(use_proxy: bool, proxies: dict) -> None:
    """Зберігає налаштування проксі у proxy_settings.json."""
    proxy_manager.save(use_proxy, proxies)


def test_proxy(p: dict) -> bool:
    """Перевіряє проксі через requests — точно відповідає реальному використанню."""
    import requests as req
    port = int(p['port'])
    proxy_url = f"http://{p['user']}:{p['pass']}@{p['host']}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = req.get("http://httpbin.org/ip", proxies=proxies, timeout=7)
        if resp.status_code == 200:
            logger.info("✅ Проксі %s:%s — робочий", p['host'], port)
            return True
        logger.warning("❌ Проксі %s:%s — HTTP %d", p['host'], port, resp.status_code)
        return False
    except Exception as e:
        logger.warning("❌ Проксі %s:%s — мертвий (%s)", p['host'], port, e)
        return False


async def handle_proxy_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document or context.user_data is None:
        return
    doc = update.message.document
    if not (doc.file_name or "").endswith('.txt'):
        await update.message.reply_text("⚠️ Будь ласка, надішліть файл у форматі .txt")
        return

    msg = await update.message.reply_text("⏳ Завантажую файл...")
    try:
        file = await context.bot.get_file(doc.file_id)
        chat_id_str = str(update.effective_chat.id) if update.effective_chat else "unknown"
        tmp_path = os.path.join(tempfile.gettempdir(), f"proxies_{chat_id_str}.txt")
        await file.download_to_drive(tmp_path)
        context.user_data['pending_proxy_file'] = tmp_path
        await msg.edit_text("🌍 **Для якої країни ці проксі?**",
                            reply_markup=get_check_geo_kb(), parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Помилка: {e}")


async def auto_update_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message or not update.effective_message.text or context.user_data is None:
        return
    text = update.effective_message.text.strip()
    if re.match(r"^([a-zA-Z0-9\.-]+):(\d+):([a-zA-Z0-9_-]+):([a-zA-Z0-9_-]+)$", text):
        context.user_data['pending_proxy_text'] = text
        await update.effective_message.reply_text(
            "🌍 **Для якої країни цей проксі?**",
            reply_markup=get_check_geo_kb(), parse_mode="Markdown"
        )
    else:
        await update.effective_message.reply_text(
            "⚠️ Невірний формат. Має бути: `ip:port:user:pass`", parse_mode="Markdown"
        )


async def proxy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    use, proxies = _load_proxy_data()
    fr = len(proxies.get("France", []))
    fi = len(proxies.get("Finland", []))
    gn = len(proxies.get("General", []))
    text = (
        f"🌐 **Панель управління проксі**\n\n"
        f"🇫🇷 Франція: `{fr}` шт.\n🇫🇮 Фінляндія: `{fi}` шт.\n🌍 Загальні: `{gn}` шт.\n\n"
        f"📦 Всього: **{fr + fi + gn}** шт.\n"
        f"Статус: {'🟢 Активні' if use else '🔴 Вимкнені'}"
    )
    await update.message.reply_text(text, reply_markup=get_proxy_kb(), parse_mode="Markdown")


async def proxy_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or context.user_data is None:
        return
    await query.answer()

    if not query.data:
        return

    if query.data.startswith("checkgeo_"):
        geo = query.data.split("_")[1]

        if 'pending_proxy_file' in context.user_data or 'pending_proxy_text' in context.user_data:
            use, proxies = _load_proxy_data()
            if geo not in proxies:
                proxies[geo] = []

            added_count = 0
            pattern = r"([a-zA-Z0-9\.-]+):(\d+):([a-zA-Z0-9_-]+):([a-zA-Z0-9_-]+)"

            if 'pending_proxy_file' in context.user_data:
                tmp_path = context.user_data.pop('pending_proxy_file')
                try:
                    with open(tmp_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    for line in lines:
                        m = re.search(pattern, line.strip())
                        if m:
                            new_p = {'protocol': 'http', 'host': m.group(1), 'port': m.group(2),
                                     'user': m.group(3), 'pass': m.group(4)}
                            if new_p not in proxies[geo]:
                                proxies[geo].append(new_p)
                                added_count += 1
                    os.remove(tmp_path)
                except Exception as e:
                    logger.error("Помилка читання проксі-файлу: %s", e)

            elif 'pending_proxy_text' in context.user_data:
                m = re.search(pattern, context.user_data.pop('pending_proxy_text'))
                if m:
                    new_p = {'protocol': 'http', 'host': m.group(1), 'port': m.group(2),
                             'user': m.group(3), 'pass': m.group(4)}
                    if new_p not in proxies[geo]:
                        proxies[geo].append(new_p)
                        added_count = 1

            _save_proxy_data(True, proxies)
            await query.edit_message_text(
                f"✅ Додано **{added_count}** унікальних проксі до **{geo}**!",
                parse_mode="Markdown"
            )
            return

        # Перевірка проксі
        await query.edit_message_text(f"⏳ **Перевіряю проксі для {geo}...**", parse_mode="Markdown")
        _, proxies = _load_proxy_data()
        p_list = proxies.get(geo, [])

        if not p_list:
            await query.edit_message_text(
                f"⚠️ Список проксі для **{geo}** порожній.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]),
                parse_mode="Markdown"
            )
            return

        # Паралельна перевірка — замість послідовної (20 проксі × 7 сек = 140 сек → ~7 сек)
        check_results = await asyncio.gather(
            *[asyncio.to_thread(test_proxy, p) for p in p_list]
        )
        results = list(zip(p_list, check_results))
        working_list = [p for p, ok in results if ok]
        working = len(working_list)
        broken = len(p_list) - working

        context.user_data['checked_geo'] = geo
        context.user_data['working_proxies_list'] = working_list

        text = (f"📊 **Результати перевірки ({geo}):**\n\n"
                f"Всього: `{len(p_list)}`\n✅ Робочих: `{working}`\n❌ Неробочих: `{broken}`\n\n")

        if broken > 0:
            text += "Бажаєте видалити мертві проксі?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Видалити неробочі", callback_data="proxy_remove_broken")],
                [InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]
            ])
        else:
            text += "Усі проксі працюють! 🚀"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]])

        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)

    elif query.data == "proxy_check":
        await query.edit_message_text("🌍 **Яке ГЕО перевірити?**",
                                      reply_markup=get_check_geo_kb(), parse_mode="Markdown")

    elif query.data == "proxy_remove_broken":
        geo = context.user_data.get('checked_geo')
        working_list = context.user_data.get('working_proxies_list')
        if geo and working_list is not None:
            use, proxies = _load_proxy_data()
            proxies[geo] = working_list
            _save_proxy_data(use, proxies)
            await query.edit_message_text(
                "✅ Мертві проксі видалено!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]),
                parse_mode="Markdown"
            )

    elif query.data == "toggle_proxy":
        use, proxies = _load_proxy_data()
        _save_proxy_data(not use, proxies)
        await query.edit_message_reply_markup(reply_markup=get_proxy_kb())

    elif query.data == "proxy_upload_info":
        await query.edit_message_text(
            "📂 **Завантаження проксі з файлу**\n\nФормат: `ip:port:user:pass` (кожен з нового рядка)\n\n"
            "👇 Просто надішліть `.txt` файл сюди в чат.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]))

    elif query.data == "proxy_clear":
        use, _ = _load_proxy_data()
        _save_proxy_data(use, {"France": [], "Finland": [], "General": []})
        await query.edit_message_text(
            "🗑 **Список проксі очищено!**",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]))

    elif query.data == "proxy_back":
        use, proxies = _load_proxy_data()
        fr = len(proxies.get("France", []))
        fi = len(proxies.get("Finland", []))
        gn = len(proxies.get("General", []))
        text = (f"🌐 **Панель управління проксі**\n\n"
                f"🇫🇷 Франція: `{fr}` шт.\n🇫🇮 Фінляндія: `{fi}` шт.\n🌍 Загальні: `{gn}` шт.\n\n"
                f"Статус: {'🟢 Активні' if use else '🔴 Вимкнені'}")
        await query.edit_message_text(text, reply_markup=get_proxy_kb(), parse_mode="Markdown")

    elif query.data == "close_proxy":
        await query.delete_message()


async def prompt_for_zip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📁 **Перевірка фізичних документів (ШІ)**\n\n"
        "Цей модуль перевірить термін дії документів і розсортує їх.\n\n"
        "📦 **Структура архіву:**\n"
        "• Кожна папка = один клієнт\n"
        "• Всередині — фото документа (JPG/PNG)\n"
        "• Назва папки = ім'я клієнта\n\n"
        "📤 **Варіанти відправки:**\n\n"
        "**До 50 МБ** — надішли ZIP прямо в чат\n\n"
        "**Більше 50 МБ (100-500 документів):**\n"
        "1. Завантаж ZIP на **Google Drive**\n"
        "2. ПКМ → _Поділитись_ → _Усі хто має посилання_\n"
        "3. Скопіюй посилання і надішли сюди\n\n"
        "👉 _Чекаю на ZIP-архів або Google Drive посилання..._"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
