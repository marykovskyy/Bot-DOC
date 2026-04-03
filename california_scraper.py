import time
import json
import logging
import database

logger = logging.getLogger(__name__)

_SOI_KEYWORDS = ("STATEMENT OF INFORMATION", "SI-")


def get_pdf_link(page, record_num: str) -> str:
    """Отримує посилання на Statement of Information через внутрішній API сайту."""
    js_code = f"""
    return (async () => {{
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

            if (!response.ok) return "Помилка API";
            const data = await response.json();
            let list = data.AMENDMENT_LIST || [];
            if (typeof list === 'string') {{
                try {{ list = JSON.parse(list); }} catch (e) {{ list = []; }}
            }}

            const soi = list.find(f => {{
                const type = (f.AMENDMENT_TYPE || f.DISPLAY_NAME || "").toUpperCase();
                return type.includes("STATEMENT OF INFORMATION") || type.includes("SI-");
            }});

            return soi && soi.DOWNLOAD_LINK
                ? "https://bizfileonline.sos.ca.gov" + soi.DOWNLOAD_LINK
                : "SOI відсутній";
        }} catch (err) {{ return "Помилка JS"; }}
    }})();
    """
    return page.run_js(js_code)


def scrape_california(page, keyword: str, count: int, status: dict) -> list[dict]:
    results: list[dict] = []
    status['last_name'] = "🇺🇸 Пошук активних компаній із SOI..."

    page.get('https://bizfileonline.sos.ca.gov/search/business')
    page.listen.start('businesssearch')

    try:
        search_input = page.ele('css:input[aria-label*="Search"]', timeout=10)
        if search_input:
            search_input.click()
            search_input.input(keyword)
            time.sleep(1)
            page.actions.key_down('ENTER').key_up('ENTER')

        res = page.listen.wait(timeout=12)
        if not res:
            return results

        data = res.response.body
        if isinstance(data, str):
            data = json.loads(data)

        items_dict = data.get('rows', {})
        if not items_dict:
            return results

        sorted_items = sorted(items_dict.values(), key=lambda x: x.get('SORT_INDEX', 0))

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

            pdf_link = get_pdf_link(page, record_num)

            if "http" not in pdf_link:
                logger.debug("⏩ Немає SOI: %s", clean_name)
                continue

            results.append({
                "Назва": clean_name,
                "Статус": "Active",
                "Statement of Information (Link)": pdf_link,
                "RECORD_NUM": record_num
            })

            status['current'] += 1
            logger.info("[%d] %s -> %s", len(results), clean_name, pdf_link)

    except Exception as e:
        logger.error("Помилка California: %s", e)
    finally:
        page.listen.stop()

    return results
