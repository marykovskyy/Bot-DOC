import asyncio
import logging
import os
import re
import tempfile

import requests

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

import proxy.manager as proxy_manager
from keyboards import get_proxy_kb, get_check_geo_kb
from handlers.admin import require_auth

logger = logging.getLogger(__name__)

# Строгий regex: host (IPv4 / domain), port (1-5 digit), user/pass без пробілів і двокрапок
_PROXY_PATTERN = r"^([a-zA-Z0-9\.\-]+):(\d{1,5}):([^\s:]+):([^\s:]+)$"

# Максимум паралельних перевірок проксі (щоб не DDoS-ити endpoint)
_MAX_CONCURRENT_CHECKS = 20


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


def _valid_port(port_str: str) -> bool:
    """Перевірка що port у межах [1, 65535]."""
    try:
        p = int(port_str)
        return 1 <= p <= 65535
    except (ValueError, TypeError):
        return False


def test_proxy(p: dict) -> bool:
    """Перевіряє проксі через HTTPS-endpoint.

    HTTPS замість HTTP: раніше використовували http://httpbin.org/ip → пароль
    до upstream проксі летів у plaintext через той самий проксі. Тепер
    api.ipify.org по HTTPS → MITM бачить тільки CONNECT-tunnel.
    """
    if not _valid_port(str(p.get('port', ''))):
        return False
    port = int(p['port'])
    proxy_url = f"http://{p.get('user', '')}:{p.get('pass', '')}@{p['host']}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.get("https://api.ipify.org?format=json",
                            proxies=proxies, timeout=7)
        if resp.status_code == 200:
            logger.info("✅ Proxy %s:%s — OK", p['host'], port)
            return True
        logger.warning("❌ Proxy %s:%s — HTTP %d", p['host'], port, resp.status_code)
        return False
    except Exception as e:
        logger.warning("❌ Proxy %s:%s — dead (%s)", p['host'], port, e)
        return False


async def _test_proxies_limited(p_list: list[dict]) -> list[bool]:
    """Перевіряє усі проксі з обмеженням паралельності."""
    sem = asyncio.Semaphore(_MAX_CONCURRENT_CHECKS)

    async def _one(p: dict) -> bool:
        async with sem:
            return await asyncio.to_thread(test_proxy, p)

    return await asyncio.gather(*[_one(p) for p in p_list])


@require_auth
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
        # NamedTemporaryFile: унікальне ім'я → немає гонки між одночасними запитами
        # від одного юзера (раніше — tmp_path = proxies_{chat_id}.txt перезаписувався)
        tf = tempfile.NamedTemporaryFile(
            delete=False, suffix=".txt", prefix="proxies_", mode="wb"
        )
        tmp_path = tf.name
        tf.close()
        await file.download_to_drive(tmp_path)
        context.user_data['pending_proxy_file'] = tmp_path
        await msg.edit_text("🌍 **Для якої країни ці проксі?**",
                            reply_markup=get_check_geo_kb(), parse_mode="Markdown")
    except Exception as e:
        logger.warning("handle_proxy_file error: %s", e)
        await msg.edit_text(f"❌ Помилка: {e}")


@require_auth
async def auto_update_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message or not update.effective_message.text or context.user_data is None:
        return
    text = update.effective_message.text.strip()
    m = re.match(_PROXY_PATTERN, text)
    if m and _valid_port(m.group(2)):
        context.user_data['pending_proxy_text'] = text
        await update.effective_message.reply_text(
            "🌍 **Для якої країни цей проксі?**",
            reply_markup=get_check_geo_kb(), parse_mode="Markdown"
        )
    else:
        await update.effective_message.reply_text(
            "⚠️ Невірний формат. Має бути: `ip:port:user:pass` "
            "(port у діапазоні 1–65535)", parse_mode="Markdown"
        )


@require_auth
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


@require_auth
async def proxy_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or context.user_data is None:
        return
    await query.answer()

    if not query.data:
        return

    if query.data.startswith("checkgeo_"):
        geo = query.data.split("_")[1]
        # Санітизація geo: тільки відомі країни
        if geo not in ("France", "Finland", "General"):
            await query.edit_message_text(f"⚠️ Невідома країна: {geo}")
            return

        if 'pending_proxy_file' in context.user_data or 'pending_proxy_text' in context.user_data:
            added_count = 0
            # ── Збираємо нові проксі ДО мутації ──
            new_proxies: list[dict] = []
            parse_pattern = r"^([a-zA-Z0-9\.\-]+):(\d{1,5}):([^\s:]+):([^\s:]+)$"

            if 'pending_proxy_file' in context.user_data:
                tmp_path = context.user_data.pop('pending_proxy_file')
                try:
                    with open(tmp_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            m = re.match(parse_pattern, line.strip())
                            if m and _valid_port(m.group(2)):
                                new_proxies.append({
                                    'protocol': 'http', 'host': m.group(1), 'port': m.group(2),
                                    'user': m.group(3), 'pass': m.group(4)
                                })
                except Exception as e:
                    logger.error("Помилка читання проксі-файлу: %s", e)
                finally:
                    # Завжди видаляємо tmp-файл (навіть при помилці)
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

            elif 'pending_proxy_text' in context.user_data:
                m = re.match(parse_pattern, context.user_data.pop('pending_proxy_text'))
                if m and _valid_port(m.group(2)):
                    new_proxies.append({
                        'protocol': 'http', 'host': m.group(1), 'port': m.group(2),
                        'user': m.group(3), 'pass': m.group(4)
                    })

            # ── Атомарна read-modify-write мутація (захищено локом) ──
            if new_proxies:
                with proxy_manager.update() as data:
                    data["use_proxy"] = True
                    proxies = data.setdefault("proxies", {})
                    if geo not in proxies:
                        proxies[geo] = []
                    for np in new_proxies:
                        if np not in proxies[geo]:
                            proxies[geo].append(np)
                            added_count += 1

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

        # Паралельна перевірка з обмеженням Semaphore(_MAX_CONCURRENT_CHECKS) —
        # уникаємо DDoS на ipify (раніше 500 проксі = 500 одночасних threads)
        check_results = await _test_proxies_limited(p_list)
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
            # ── Атомарне видалення: зливаємо working_list з поточним станом ──
            # Захист від lost-update: якщо між "Перевірити" і "Видалити" хтось додав
            # нові проксі — не видаляємо їх (залишаємо unknown як working).
            working_keys = {
                (p.get('host'), str(p.get('port')), p.get('user'), p.get('pass'))
                for p in working_list
            }
            with proxy_manager.update() as data:
                proxies = data.setdefault("proxies", {})
                current = proxies.get(geo, [])
                # Зберігаємо все, що знаходиться у working_list АБО було додано після перевірки
                checked_keys = {
                    (p.get('host'), str(p.get('port')), p.get('user'), p.get('pass'))
                    for p in context.user_data.get('working_proxies_list', [])
                } | {
                    (p.get('host'), str(p.get('port')), p.get('user'), p.get('pass'))
                    for p, ok in zip(
                        context.user_data.get('working_proxies_list', []),
                        [True] * len(working_list)
                    )
                }
                preserved = []
                for p in current:
                    key = (p.get('host'), str(p.get('port')), p.get('user'), p.get('pass'))
                    if key in working_keys:
                        preserved.append(p)
                    elif key not in checked_keys:
                        # новий проксі — залишаємо (не перевірявся)
                        preserved.append(p)
                proxies[geo] = preserved
            await query.edit_message_text(
                "✅ Мертві проксі видалено!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]),
                parse_mode="Markdown"
            )

    elif query.data == "toggle_proxy":
        with proxy_manager.update() as data:
            data["use_proxy"] = not bool(data.get("use_proxy", False))
        await query.edit_message_reply_markup(reply_markup=get_proxy_kb())

    elif query.data == "proxy_upload_info":
        await query.edit_message_text(
            "📂 **Завантаження проксі з файлу**\n\nФормат: `ip:port:user:pass` (кожен з нового рядка)\n\n"
            "👇 Просто надішліть `.txt` файл сюди в чат.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="proxy_back")]]))

    elif query.data == "proxy_clear":
        # Перед очищенням зберігаємо backup (на випадок помилкового натискання)
        try:
            import json
            from pathlib import Path
            backup_path = Path(proxy_manager._PROXY_FILE).with_suffix('.json.bak')
            current = proxy_manager.load()
            backup_path.write_text(
                json.dumps(current, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            logger.info("proxy_clear: backup saved to %s", backup_path)
        except Exception as e:
            logger.warning("proxy_clear backup failed: %s", e)
        with proxy_manager.update() as data:
            data["proxies"] = {"France": [], "Finland": [], "General": []}
        await query.edit_message_text(
            "🗑 **Список проксі очищено!**\n\n"
            "_Backup збережено у `proxy_settings.json.bak`._",
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


@require_auth
async def prompt_for_zip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Активуємо «режим очікування документів» на 30 хв.
    # handle_gdrive_link перевіряє цей прапорець перед запуском аналізу,
    # щоб випадково скопійоване drive-посилання не тригерило аналіз.
    import time as _time
    if context.user_data is not None:
        context.user_data["awaiting_docs_until"] = _time.time() + 1800

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
        "👉 _Чекаю на ZIP-архів або Google Drive посилання_ (30 хв)"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
