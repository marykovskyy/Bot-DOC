import json
import logging
import time

import database

logger = logging.getLogger(__name__)

_SOI_KEYWORDS = ("STATEMENT OF INFORMATION", "SI-")


def get_pdf_link_and_address(page, record_num: str) -> dict:
    """Отримує SOI-посилання + principal address одним API-викликом.

    Повертає dict:
      {pdf_link: str, address: str, city: str, state: str, zip: str}
    """
    js_code = f"""
    return (async () => {{
        const empty = {{pdf_link: "Помилка API", address: "", city: "", state: "", zip: ""}};
        try {{
            let token = "";
            try {{
                const storage = JSON.parse(localStorage.getItem('okta-token-storage'));
                token = storage.accessToken.accessToken;
            }} catch(e) {{ token = "undefined"; }}

            const response = await fetch("https://bizfileonline.sos.ca.gov/api/History/business/{record_num}", {{
                "headers": {{
                    "accept": "*/*",
                    "authorization": "Bearer " + token,
                    "sec-fetch-site": "same-origin"
                }},
                "method": "GET",
                "credentials": "include"
            }});

            if (!response.ok) return empty;
            const data = await response.json();

            // ── SOI link ──
            let list = data.AMENDMENT_LIST || [];
            if (typeof list === 'string') {{
                try {{ list = JSON.parse(list); }} catch (e) {{ list = []; }}
            }}
            const soi = list.find(f => {{
                const type = (f.AMENDMENT_TYPE || f.DISPLAY_NAME || "").toUpperCase();
                return type.includes("STATEMENT OF INFORMATION") || type.includes("SI-");
            }});
            const pdf_link = soi && soi.DOWNLOAD_LINK
                ? "https://bizfileonline.sos.ca.gov" + soi.DOWNLOAD_LINK
                : "SOI відсутній";

            // ── Address (різні CA SOS-схеми використовують різні ключі) ──
            // Шукаємо принципову адресу в data або в DRAWER_DETAIL_LIST.
            let addr = "", city = "", state = "", zip = "";

            const pickFrom = (obj) => {{
                if (!obj || typeof obj !== 'object') return false;
                const keys = Object.keys(obj);
                // Повний рядок адреси
                for (const k of keys) {{
                    const lk = k.toLowerCase();
                    if (lk.includes('principal') && lk.includes('addr') && !addr) {{
                        addr = String(obj[k] || '');
                    }}
                    if ((lk === 'street' || lk.endsWith('street1') || lk.endsWith('street_1')) && !addr) {{
                        addr = String(obj[k] || '');
                    }}
                    if (lk.endsWith('city') && !city) city = String(obj[k] || '');
                    if (lk.endsWith('state') && !state && String(obj[k]||'').length <= 3) {{
                        state = String(obj[k] || '');
                    }}
                    if ((lk === 'zip' || lk.endsWith('zip_code') || lk.endsWith('postal_code') || lk.endsWith('zipcode')) && !zip) {{
                        zip = String(obj[k] || '');
                    }}
                }}
                return addr || city || zip;
            }};

            pickFrom(data);
            const drawer = data.DRAWER_DETAIL_LIST || [];
            if (Array.isArray(drawer)) {{
                for (const d of drawer) pickFrom(d);
            }}

            return {{
                pdf_link: pdf_link,
                address: addr.trim(),
                city: city.trim(),
                state: state.trim(),
                zip: zip.trim(),
            }};
        }} catch (err) {{ return empty; }}
    }})();
    """
    res = page.run_js(js_code)
    if isinstance(res, dict):
        return res
    # Backward-compat: якщо API повернуло просто str (старий формат)
    return {"pdf_link": str(res or ""), "address": "", "city": "", "state": "", "zip": ""}


def get_pdf_link(page, record_num: str) -> str:
    """Backward-compat wrapper — повертає тільки pdf_link."""
    return get_pdf_link_and_address(page, record_num).get("pdf_link", "")


def _get_search_input(page, attempts: int = 3):
    """Знаходить ВИДИМЕ поле пошуку, з refresh-ретраями.

    CA SOS — React SPA: input з'являється в DOM раніше, ніж рендериться
    (нульовий розмір). Клік по ньому → "元素没有位置及大小" (DrissionPage 4.1).
    Під час довгих multi-keyword прогонів сайт також може віддавати
    напівпорожню сторінку (rate-limit) — тоді допомагає лише refresh+пауза.
    """
    for attempt in range(1, attempts + 1):
        search_input = page.ele('css:input[aria-label*="Search"]', timeout=10)
        if search_input:
            try:
                search_input.wait.displayed(timeout=8)
            except Exception:
                pass
            try:
                if search_input.states.is_displayed:
                    return search_input
            except Exception:
                pass
        logger.warning("California: поле пошуку не відрендерилось "
                       "(спроба %d/%d, title=%r) — refresh", attempt, attempts, page.title)
        if attempt < attempts:
            page.refresh()
            time.sleep(3 * attempt)   # прогресивна пауза — даємо SPA/rate-limit відпуститись
    return None


def _do_search(page, keyword: str, attempts: int = 3):
    """Виконує пошук за keyword, повертає розпарсений JSON або None.

    Обробляє rate-limit CA SOS: після бурсту запитів API віддає порожнє
    тіло / HTML замість JSON ("Expecting value: line 1 column 1").
    У такому разі — cooldown-пауза з ескалацією (20с → 40с) і повторний пошук.
    """
    for attempt in range(1, attempts + 1):
        page.get('https://bizfileonline.sos.ca.gov/search/business')

        search_input = _get_search_input(page)
        if not search_input:
            logger.error("California: поле пошуку так і не з'явилось — пропускаю '%s'", keyword)
            return None
        try:
            search_input.click()
        except Exception:
            # Елемент видимий, але клік мишею недоступний (перекриття) — JS-клік
            search_input.click(by_js=True)
        search_input.input(keyword, clear=True)
        time.sleep(1)
        page.actions.key_down('ENTER').key_up('ENTER')

        res = page.listen.wait(timeout=12)
        if not res:
            logger.warning("California: '%s' — пошуковий запит не відбувся (спроба %d/%d)",
                           keyword, attempt, attempts)
            continue

        body = res.response.body
        http_status = getattr(res.response, 'status', None)
        if isinstance(body, str):
            body_str = body.strip()
            if not body_str or body_str[0] not in '{[':
                # Порожнє тіло або HTML-заглушка = rate-limit / тимчасовий бан IP
                cooldown = 20 * attempt
                logger.warning(
                    "California: API повернув не-JSON (HTTP %s) на '%s' — схоже на "
                    "rate-limit. Пауза %dс (спроба %d/%d)",
                    http_status, keyword, cooldown, attempt, attempts)
                if attempt < attempts:
                    time.sleep(cooldown)
                continue
            try:
                body = json.loads(body_str)
            except json.JSONDecodeError as e:
                logger.warning("California: битий JSON на '%s' (HTTP %s): %s",
                               keyword, http_status, e)
                continue
        return body

    logger.error("California: '%s' — API не відповів після %d спроб (rate-limit). "
                 "Рекомендація: увімкнути проксі для California або зменшити "
                 "кількість ключових слів за один прогін.", keyword, attempts)
    return None


def scrape_california(page, keyword: str, count: int, status: dict) -> list[dict]:
    results: list[dict] = []
    status['last_name'] = "🇺🇸 Пошук активних компаній із SOI..."

    page.listen.start('businesssearch')

    try:
        data = _do_search(page, keyword)
        if not data:
            return results

        items_dict = data.get('rows', {})
        if not items_dict:
            return results

        sorted_items = sorted(items_dict.values(), key=lambda x: x.get('SORT_INDEX', 0))

        api_fail_streak = 0   # поспіль "Помилка API" = rate-limit History-ендпоінта

        for item in sorted_items:
            if len(results) >= count:
                break
            if not status.get('is_running', True):
                break

            if str(item.get('STATUS', '')).strip().upper() != 'ACTIVE':
                continue

            title_raw = item.get('TITLE', ['Невідомо'])
            full_name = title_raw[0] if isinstance(title_raw, list) else str(title_raw)
            clean_name = full_name.split('(')[0].strip()

            if database.is_company_name_scraped(clean_name):
                logger.debug("⏩ Вже є в базі: %s", clean_name)
                continue

            record_num = item.get('RECORD_NUM')
            status['last_name'] = f"📄 Перевірка SOI: {clean_name}"

            details = get_pdf_link_and_address(page, record_num)
            pdf_link = details.get("pdf_link", "")

            # Легкий тротлінг: не довбимо History API частіше ~2 запитів/сек,
            # інакше сайт банить IP після ~40 компаній (перевірено логами).
            time.sleep(0.4)

            # ── Захист від rate-limit History API ──
            # "Помилка API" ≠ "SOI відсутній": перше — збій запиту (бан/429),
            # друге — компанія реально без SOI. Серію збоїв гасимо паузою,
            # щоб не спалити решту списку і не втратити компанії даремно.
            if pdf_link == "Помилка API":
                api_fail_streak += 1
                if api_fail_streak == 5:
                    logger.warning("California: 5 збоїв History API поспіль — "
                                   "rate-limit, пауза 30с")
                    time.sleep(30)
                elif api_fail_streak >= 10:
                    logger.error("California: History API стабільно не відповідає "
                                 "(rate-limit) — зупиняю це ключове слово")
                    break
                continue
            api_fail_streak = 0

            if "http" not in pdf_link:
                logger.debug("⏩ Немає SOI: %s", clean_name)
                continue

            results.append({
                "Назва": clean_name,
                "Статус": "Active",
                "Address": details.get("address", ""),
                "City":    details.get("city", ""),
                "State":   details.get("state", "") or "CA",
                "Zip":     details.get("zip", ""),
                "Statement of Information (Link)": pdf_link,
                "RECORD_NUM": record_num,
            })

            status['current'] += 1
            logger.info("[%d] %s -> %s", len(results), clean_name, pdf_link)

    except Exception as e:
        logger.error("Помилка California: %s", e)
    finally:
        page.listen.stop()

    return results
