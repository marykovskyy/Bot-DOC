"""
scrapers/washington.py — Washington Secretary of State (CCFS).

Сайт: https://ccfs.sos.wa.gov/  (Corporations & Charities Filing System)
Тип: Angular SPA з JSON API. Працюємо як з California:
  - DrissionPage заходить на сторінку (нам потрібні session cookies)
  - page.listen.start() ловить XHR з результатами пошуку
  - для кожної компанії викликаємо filings endpoint через page.run_js
    (fetch у контексті сторінки — підтягує auth cookies автоматично)

Що збираємо для кожної компанії:
  - Annual Report (link)            ← звичайний річний звіт
  - Express Annual Report (link)    ← експрес-варіант (як на скріні юзера)

Фільтр статусу:
  Тільки чистий "Active". Будь-які варіанти Dissolved / Withdrawn /
  Terminated / Inactive / Suspended відкидаємо одразу — це і є вимога
  юзера "не в процесі ліквідації тощо".

Гнучкість до змін API: ловимо ВСІ XHR під /api/, перевіряємо різні ключі
у JSON (DataResult / Data / Result / rows). Так само з полями документа —
підтримуємо кілька можливих імен (FilingTypeCode / DocumentName / ...).
Якщо WA змінить структуру — лог покаже що саме повернулось і ми зможемо
швидко підкрутити селектори.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

import database

logger = logging.getLogger(__name__)

SEARCH_URL = "https://ccfs.sos.wa.gov/#/AdvancedSearch"
BASE_URL   = "https://ccfs.sos.wa.gov"
API_URL    = "https://ccfs-api.prod.sos.wa.gov"   # окремий API-домен!

# Які значення BusinessStatus вважаємо "ACTIVE".
# WA має варіанти: Active / Administratively Dissolved / Voluntarily Dissolved /
# Inactive / Withdrawn / Terminated / Suspended / Expired. Беремо тільки чистий
# Active.
_ACTIVE_VALUES = {"ACTIVE"}

# Підстрічки для пошуку типів документів (case-insensitive).
# Порядок важливий: Express спочатку — щоб "Annual Report" не зловив його як
# звичайний annual.
_EXPRESS_KEYWORDS  = ("EXPRESS ANNUAL REPORT",)
_ANNUAL_KEYWORDS   = ("ANNUAL REPORT",)   # будь-який annual (НЕ-express зловимо нижче явно)


def _extract_items(data: Any) -> list[dict]:
    """З довільної JSON-структури API дістає список бізнесів."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("DataResult", "Data", "Result", "Results", "rows", "items", "List"):
        v = data.get(key)
        if isinstance(v, list) and v:
            return v
    # Іноді буває вкладено: data.data.DataResult
    inner = data.get("data") if isinstance(data.get("data"), dict) else None
    if inner:
        return _extract_items(inner)
    return []


def _doc_type(f: dict) -> str:
    return (
        f.get("FilingTypeCode") or f.get("FilingType") or f.get("DocumentType")
        or f.get("DocumentName") or f.get("FilingTypeDescription") or ""
    ).upper()


def _doc_link(f: dict) -> str:
    link = (
        f.get("DownloadLink") or f.get("DocumentLink") or f.get("ImageDocumentURL")
        or f.get("DocumentURL") or f.get("ImageDocumentURI") or ""
    )
    if link and not link.startswith("http"):
        link = BASE_URL + link
    return link


def _doc_date(f: dict) -> str:
    return (f.get("FiledDate") or f.get("EffectiveDate") or f.get("FilingDate") or "")[:10]


def _pick_latest(filings: list[dict], include_kw: tuple[str, ...],
                 exclude_kw: tuple[str, ...] = ()) -> tuple[str, str]:
    """Знаходить найновіший filing типу що містить include_kw і не містить exclude_kw.

    Повертає (link, filed_date). Якщо нема — ('', '').
    """
    matched: list[dict] = []
    for f in filings:
        t = _doc_type(f)
        if not any(k in t for k in include_kw):
            continue
        if exclude_kw and any(k in t for k in exclude_kw):
            continue
        matched.append(f)
    if not matched:
        return "", ""
    matched.sort(key=_doc_date, reverse=True)
    return _doc_link(matched[0]), _doc_date(matched[0])


def _fetch_filings_via_browser(page, business_id) -> list[dict]:
    """Викликає GetFilingDetails endpoint у контексті сторінки.

    fetch() з credentials=include підхоплює сесійні cookies — так само
    як California-скрапер робить через локальний Okta-token.
    """
    js = f"""
    return (async () => {{
        // CCFS повертає filings через POST. API на окремому домені ccfs-api.prod.sos.wa.gov.
        // Точну назву endpoint поки не знаємо — пробуємо кілька найпоширеніших.
        const endpoints = [
            "/api/BusinessSearch/GetFilingDetails",
            "/api/BusinessSearch/GetBusinessFilings",
            "/api/Common/GetFilingDetails",
            "/api/Common/GetBusinessFilings",
        ];
        for (const ep of endpoints) {{
            try {{
                const r = await fetch("{API_URL}" + ep, {{
                    method: "POST",
                    headers: {{
                        "accept": "application/json",
                        "content-type": "application/json"
                    }},
                    credentials: "include",
                    body: JSON.stringify({{ businessId: {business_id} }})
                }});
                if (!r.ok) continue;
                const d = await r.json();
                if (Array.isArray(d) && d.length) return d;
                if (d && typeof d === 'object') {{
                    for (const k of ["DataResult","Data","Result","Results","rows","items","List","Filings"]) {{
                        if (Array.isArray(d[k]) && d[k].length) return d[k];
                    }}
                }}
            }} catch(e) {{ /* спробуємо наступний endpoint */ }}
        }}
        return [];
    }})();
    """
    try:
        res = page.run_js(js)
        return res if isinstance(res, list) else []
    except Exception as e:
        logger.debug("WA: fetch filings JS error: %s", e)
        return []


def _spoof_us_timezone(page) -> None:
    """Підкручує Chrome timezone під US-East через CDP.

    Без цього pixelscan / Cloudflare bot-detection бачать що IP=NY
    а Chrome шле Europe/Kyiv — це fingerprint mismatch і одразу challenge.
    """
    try:
        page.run_cdp("Emulation.setTimezoneOverride", timezoneId="America/New_York")
        # Locale теж краще вирівняти на en-US
        page.run_cdp("Emulation.setLocaleOverride", locale="en-US")
        logger.debug("WA: timezone/locale override → America/New_York / en-US")
    except Exception as e:
        logger.debug("WA: timezone override skip: %s", e)


def _dump_page_state(page, label: str) -> None:
    """Логує URL + title + body для діагностики (без head/style — нецікаво)."""
    try:
        url = getattr(page, 'url', '?')
    except Exception:
        url = "?"
    try:
        title = getattr(page, 'title', '?')
    except Exception:
        title = "?"
    try:
        html = str(getattr(page, 'html', '') or '')
    except Exception:
        html = ""
    logger.warning("WA: [%s] url=%s title=%s html_len=%d", label, url, title, len(html))
    # Витягуємо тільки body — head з style-блоками засмічує лог
    body_text = ""
    try:
        body_html = page.run_js(
            "return document.body ? document.body.outerHTML.replace(/\\s+/g,' ') : '';"
        )
        if isinstance(body_html, str):
            body_text = body_html[:2000]
    except Exception:
        # Fallback — витягуємо substring між <body...> і </body>
        low = html.lower()
        i = low.find("<body")
        j = low.find("</body>")
        if i != -1 and j != -1:
            body_text = " ".join(html[i:j].split())[:2000]
    if body_text:
        logger.warning("WA: [%s] body[:2000]=%s", label, body_text)
    # Окремо рахуємо input-и в DOM — щоб одразу бачити чи Angular зрендерив форму
    try:
        inputs_count = page.run_js(
            "return document.querySelectorAll('input').length;"
        )
        selects_count = page.run_js(
            "return document.querySelectorAll('select').length;"
        )
        logger.warning("WA: [%s] inputs=%s selects=%s", label, inputs_count, selects_count)
    except Exception:
        pass


def scrape_washington(page, keyword: str, count: int, status: dict) -> list[dict]:
    results: list[dict] = []
    status['last_name'] = "🇺🇸 WA: пошук активних компаній..."

    # ── Anti-fingerprint: timezone/locale під US ────────────────────────
    _spoof_us_timezone(page)

    # ── Warmup: спочатку root URL ──────────────────────────────────────
    # Cloudflare захищає CCFS і блокує "холодного" клієнта на хеш-маршруті
    # одразу. Якщо зайти на головну і дочекатися автопроходження challenge,
    # сервер видає cf_clearance cookie на ~30хв — наступний request на
    # #/BusinessSearch вже йде без challenge.
    logger.info("WA: warmup — заходжу на головну для отримання Cloudflare cookie")
    page.get(BASE_URL + "/")
    # МІНІМАЛЬНО чекаємо 5с (Cloudflare ставить cookie через ~3-7с)
    time.sleep(5)
    # Потім полінг до 30с — поки challenge точно не пройде
    cf_deadline = time.time() + 25
    challenge_detected = False
    while time.time() < cf_deadline:
        try:
            html_lower = (str(getattr(page, 'html', '') or '')).lower()
        except Exception:
            html_lower = ""
        # Ознаки що challenge все ще активний
        if ("just a moment" in html_lower
                or "challenge-platform" in html_lower
                or "cf-mitigated" in html_lower
                or "checking your browser" in html_lower):
            challenge_detected = True
            time.sleep(2)
            continue
        # Очікувана сторінка CCFS вже точно завантажена
        if ("business search" in html_lower or "ccfs" in html_lower
                or "corporations and charities" in html_lower):
            break
        time.sleep(2)
    logger.info("WA: warmup завершено (challenge: %s)", "детектовано" if challenge_detected else "пройдено")
    _dump_page_state(page, "after-warmup")

    # ── Перехід на форму пошуку ЧЕРЕЗ HASH (без повного reload) ─────────
    # page.get(SEARCH_URL) робив би повний HTTP request — який або тригерить
    # новий Cloudflare challenge, або скидає Angular bootstrap (html_len падав
    # з 45k до 18k у попередніх логах). Через location.hash Angular просто
    # перемикає internal router без HTTP — форма рендериться нормально.
    logger.info("WA: переходжу на AdvancedSearch через router (без reload)")
    try:
        page.run_js("window.location.hash = '#/AdvancedSearch';")
    except Exception as e:
        logger.warning("WA: hash navigation failed (%s) — fallback на page.get", e)
        page.get(SEARCH_URL)
    time.sleep(3)
    _dump_page_state(page, "after-route")

    # Слухаємо API-домен. УВАГА: API на ОКРЕМОМУ піддомені ccfs-api.prod.sos.wa.gov
    # (не ccfs.sos.wa.gov!). Endpoint: POST /api/BusinessSearch/GetBusinessSearchList.
    # Запити йдуть з токеном X-reCAPTCHA — тому йдемо через браузер (Angular сам генерує).
    page.listen.start('ccfs-api.prod.sos.wa.gov')

    try:
        # ── Чекаємо input пошуку до 45с ──
        # CCFS захищений Cloudflare — challenge може зайняти 5-15с.
        # Робимо терплячий polling замість фіксованого sleep:
        #   - кожні 2с пробуємо знайти input
        #   - на 12с робимо refresh (раптом застрягло на challenge)
        #   - на 30с — ще один refresh
        # Селектори підбираються до Angular ng-model і generic input.
        selectors = (
            'css:input[placeholder*="Business Name" i]',
            'css:input[placeholder*="Business" i]',
            'css:input[name*="BusinessName" i]',
            'css:input[ng-model*="BusinessName" i]',
            'css:input[type="text"]',
        )

        search_input = None
        deadline = time.time() + 45
        refreshed_at: list[float] = []
        while time.time() < deadline:
            elapsed = 45 - (deadline - time.time())
            for selector in selectors:
                try:
                    el = page.ele(selector, timeout=1)
                except Exception:
                    el = None
                if el:
                    search_input = el
                    break
            if search_input:
                logger.info("WA: input знайдено через %.1fс", elapsed)
                break

            # Cloudflare challenge — даємо йому час
            try:
                html_lower = (str(getattr(page, 'html', '') or '')).lower()
            except Exception:
                html_lower = ""
            if "just a moment" in html_lower or "challenge-platform" in html_lower:
                logger.debug("WA: Cloudflare challenge у процесі, чекаю...")

            # Refresh-точки: ~12с і ~30с
            if elapsed > 12 and len(refreshed_at) == 0:
                logger.info("WA: input не з'явився за 12с — refresh")
                try:
                    page.refresh()
                except Exception:
                    pass
                refreshed_at.append(elapsed)
            elif elapsed > 30 and len(refreshed_at) == 1:
                logger.info("WA: input все ще нема за 30с — повторний refresh")
                try:
                    page.refresh()
                except Exception:
                    pass
                refreshed_at.append(elapsed)

            time.sleep(2)

        if not search_input:
            logger.error("WA: search input не знайдено за 45с на %s", SEARCH_URL)
            _dump_page_state(page, "input-not-found")
            return results

        search_input.click()
        search_input.input(keyword)
        time.sleep(0.5)

        # ── Синхронізуємо Angular ngModel ────────────────────────────────
        # На AdvancedSearch ng-model має інше імʼя ніж на BusinessSearch
        # (типу searchCriteria.BusinessName / advSearchCriteria.Name / ...).
        # Шукаємо input по comprehensive признаках: ng-model fuzzy-match
        # на "businessname" АБО placeholder/label містить "Business Name".
        try:
            # Спочатку дамп — побачимо всі text inputs з ngModel
            inputs_dump = page.run_js("""
                return Array.from(document.querySelectorAll('input[type=text], input:not([type])')).map(el => ({
                    ngModel: el.getAttribute('ng-model') || '',
                    placeholder: el.getAttribute('placeholder') || '',
                    name: el.getAttribute('name') || '',
                    visible: el.offsetParent !== null,
                    value: el.value || ''
                })).filter(x => x.visible);
            """)
            if isinstance(inputs_dump, list):
                logger.info("WA: text inputs на AdvancedSearch (%d):", len(inputs_dump))
                for i, inp in enumerate(inputs_dump[:15]):
                    logger.info("  [%d] ng-model=%r placeholder=%r name=%r value=%r",
                                i, inp.get('ngModel'), inp.get('placeholder'),
                                inp.get('name'), inp.get('value'))

            sync = page.run_js("""
                const kw = arguments[0];
                const norm = s => (s || '').toLowerCase().replace(/[^a-z]/g, '');
                const inputs = Array.from(document.querySelectorAll('input[type=text], input:not([type])'))
                    .filter(el => el.offsetParent !== null);
                let target = null;

                // 1) точний матч: ng-model містить 'businessname'
                for (const el of inputs) {
                    const ng = norm(el.getAttribute('ng-model'));
                    if (ng.includes('businessname')) { target = el; break; }
                }
                // 2) placeholder
                if (!target) for (const el of inputs) {
                    const ph = norm(el.getAttribute('placeholder'));
                    if (ph.includes('businessname')) { target = el; break; }
                }
                // 3) input біля <label>Business Name</label>
                if (!target) {
                    const labels = Array.from(document.querySelectorAll('label, td, th, span'));
                    for (const lab of labels) {
                        if (/business\\s*name/i.test(lab.innerText || '')) {
                            const parent = lab.closest('div, tr, td, .row');
                            if (parent) {
                                const inp = parent.querySelector('input[type=text], input:not([type])');
                                if (inp) { target = inp; break; }
                            }
                        }
                    }
                }
                if (!target) return {ok: false, reason: 'no input found'};

                // Заповнюємо value + диспатчимо events щоб Angular ngModel оновився
                target.focus();
                target.value = kw;
                target.dispatchEvent(new Event('input', {bubbles: true}));
                target.dispatchEvent(new Event('change', {bubbles: true}));
                target.dispatchEvent(new Event('blur', {bubbles: true}));

                return {
                    ok: true,
                    value: target.value,
                    ngModel: target.getAttribute('ng-model'),
                    placeholder: target.getAttribute('placeholder')
                };
            """, keyword)
            logger.info("WA: ngModel sync → %s", sync)
        except Exception as e:
            logger.debug("WA: ngModel sync error: %s", e)

        time.sleep(0.5)

        # ── Status dropdown НЕ чіпаємо ──
        # Раніше тут була логіка вибору "Active" у Business Status. Виявилось,
        # що ng-change на select викликає автопошук → форма submit-ить ДО того
        # як ми клікаємо нашу кнопку. Прибираємо — фільтр статусу робимо у
        # Python вже після видачі результатів (на тому ж місці де було).

        # ── Діагностика: дампимо назви методів на Angular scope ──────────
        # AdvancedSearch може мати searchBusiness/AdvancedSearch/basicSearch/...
        # — побачимо реальну назву.
        try:
            scope_methods = page.run_js("""
                if (typeof angular === 'undefined') return null;
                const root = document.querySelector('[ng-view]') || document.body;
                let s = angular.element(root).scope() || angular.element(root).isolateScope();
                if (!s) return null;
                // Збираємо ВСІ scope-методи з ланцюжка scope + childen
                const found = new Set();
                function walk(sc) {
                    if (!sc) return;
                    for (const k in sc) {
                        if (typeof sc[k] === 'function' && !k.startsWith('$')) {
                            // Цікавлять search-подібні методи
                            if (/search|submit|find|query/i.test(k)) found.add(k);
                        }
                    }
                    if (sc.$$childHead) walk(sc.$$childHead);
                    if (sc.$$nextSibling) walk(sc.$$nextSibling);
                }
                walk(s);
                return Array.from(found);
            """)
            if scope_methods:
                logger.info("WA: Angular scope методи (search-related): %s", scope_methods)
        except Exception as e:
            logger.debug("WA: scope methods dump error: %s", e)

        # ── Submit форми пошуку ──────────────────────────────────────────
        try:
            page.run_js("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        except Exception:
            pass

        # Діагностика: список усіх button/input елементів (БЕЗ td/a/span — це не кнопки)
        try:
            candidates = page.run_js("""
                const els = Array.from(document.querySelectorAll(
                    'button, input[type=submit], input[type=button]'
                ));
                return els.map(el => ({
                    tag: el.tagName,
                    type: el.getAttribute('type') || '',
                    text: ((el.innerText || el.value || el.textContent || '').trim()).slice(0, 50),
                    ngClick: (el.getAttribute('ng-click') || '').slice(0, 100),
                    visible: el.offsetParent !== null,
                    rect: (() => { const r = el.getBoundingClientRect(); return {x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width)}; })()
                }));
            """)
            if isinstance(candidates, list):
                logger.info("WA: на сторінці знайдено %d button/input елементів:", len(candidates))
                for i, c in enumerate(candidates):
                    logger.info("  [%d] %s type=%s text=%r ng-click=%r vis=%s rect=%s",
                                i, c.get('tag'), c.get('type'), c.get('text'),
                                c.get('ngClick'), c.get('visible'), c.get('rect'))
        except Exception as e:
            logger.debug("WA: candidates dump error: %s", e)

        clicked = False

        # 2) Click через JS — ТІЛЬКИ button/input (НЕ td/a — то заголовки сортування)
        try:
            click_result = page.run_js("""
                const cands = Array.from(document.querySelectorAll(
                    'button, input[type=submit], input[type=button]'
                ));
                // Виключаємо ng-click що тригерять сортування (search(page,...))
                const isSortingClick = (ng) => /search\\s*\\(\\s*page/i.test(ng);

                for (const el of cands) {
                    if (el.offsetParent === null) continue;
                    const text = (el.innerText || el.value || el.textContent || '').trim().toUpperCase();
                    const ngclk = (el.getAttribute('ng-click') || '');
                    if (isSortingClick(ngclk)) continue;
                    if (text === 'SEARCH' || text.startsWith('SEARCH')) {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return {ok: true, text: text, ngClick: ngclk, tag: el.tagName};
                    }
                }
                return {ok: false, count: cands.length};
            """)
            if isinstance(click_result, dict) and click_result.get('ok'):
                clicked = True
                logger.info("WA: SEARCH клікнуто через JS (tag=%s text=%r ng-click=%r)",
                            click_result.get('tag'), click_result.get('text'), click_result.get('ngClick'))
        except Exception as e:
            logger.debug("WA: JS click error: %s", e)

        # 2b) Якщо не знайшли по тексту — пробуємо викликати Angular search-функцію напряму
        if not clicked:
            try:
                ng_result = page.run_js("""
                    // На AdvancedSearch контролер має одну з функцій:
                    // searchBusiness(), AdvancedSearch(), basicSearch(), search()
                    // Шукаємо element зі scope і викликаємо першу що існує.
                    if (typeof angular === 'undefined') return {ok: false, reason: 'no angular'};
                    const root = document.querySelector('[ng-view]') || document.body;
                    const scope = angular.element(root).scope() || angular.element(root).isolateScope();
                    if (!scope) return {ok: false, reason: 'no scope'};
                    const cs = scope.$$childHead;
                    // Збираємо всі дочірні скоупи рекурсивно
                    const allScopes = [];
                    function walk(s) {
                        if (!s) return;
                        allScopes.push(s);
                        if (s.$$childHead) walk(s.$$childHead);
                        if (s.$$nextSibling) walk(s.$$nextSibling);
                    }
                    walk(scope);
                    walk(cs);
                    const candidates = ['searchBusiness','AdvancedSearch','BasicSearch','basicSearch','SearchBusiness','doSearch','onSearch'];
                    for (const s of allScopes) {
                        for (const fn of candidates) {
                            if (typeof s[fn] === 'function') {
                                try {
                                    s.$apply(() => s[fn]());
                                    return {ok: true, fn: fn};
                                } catch(e) { return {ok: false, fn: fn, err: String(e)}; }
                            }
                        }
                    }
                    return {ok: false, reason: 'no matching scope fn'};
                """)
                if isinstance(ng_result, dict) and ng_result.get('ok'):
                    clicked = True
                    logger.info("WA: викликано Angular scope.%s()", ng_result.get('fn'))
                else:
                    logger.debug("WA: Angular scope fn не знайдено: %s", ng_result)
            except Exception as e:
                logger.debug("WA: Angular call error: %s", e)

        # 3) Якщо кнопки нема — тригернемо ng-submit на формі
        if not clicked:
            try:
                form_submit = page.run_js("""
                    // Шукаємо першу form з ng-submit що згадує search
                    const forms = Array.from(document.querySelectorAll('form'));
                    for (const f of forms) {
                        const sub = (f.getAttribute('ng-submit') || '').toLowerCase();
                        if (sub.includes('search') || sub.includes('submit')) {
                            // dispatch submit event щоб Angular handler відпрацював
                            const ev = new Event('submit', {bubbles: true, cancelable: true});
                            f.dispatchEvent(ev);
                            return {ok: true, ngSubmit: sub};
                        }
                    }
                    // Fallback: будь-яка форма
                    if (forms.length) {
                        const ev = new Event('submit', {bubbles: true, cancelable: true});
                        forms[0].dispatchEvent(ev);
                        return {ok: true, ngSubmit: '<first form>'};
                    }
                    return {ok: false};
                """)
                if isinstance(form_submit, dict) and form_submit.get('ok'):
                    clicked = True
                    logger.info("WA: submit форми тригернуто (ng-submit=%r)",
                                form_submit.get('ngSubmit'))
            except Exception as e:
                logger.debug("WA: form submit error: %s", e)

        # 4) Last resort — ENTER
        if not clicked:
            logger.warning("WA: ні кнопки, ні submit-форми — fallback на ENTER")
            page.actions.key_down('ENTER').key_up('ENTER')

        # ── Чекаємо XHR-відповідь з результатами пошуку ──
        # Перебираємо до 8 пакетів — можуть бути попередні lookup-XHR
        # (типу GetStates), які треба пропустити. Усі побачені URL
        # логуємо для діагностики.
        items: list[dict] = []
        seen_xhrs: list[tuple[str, str]] = []   # (url, preview)

        for _ in range(8):
            res = page.listen.wait(timeout=15)
            if not res:
                break

            url_raw = getattr(res, 'url', '') or ''
            url = url_raw.lower()

            try:
                body = res.response.body
            except Exception:
                body = None

            preview = ""
            parsed = body
            if isinstance(body, str):
                preview = body[:200]
                try:
                    parsed = json.loads(body)
                except Exception:
                    parsed = None
            elif isinstance(body, (dict, list)):
                preview = json.dumps(body, ensure_ascii=False)[:200]

            seen_xhrs.append((url_raw, preview))

            # Беремо лише XHR що схожі на пошук бізнесів
            # Реальний endpoint: ccfs-api.prod.sos.wa.gov/api/BusinessSearch/GetBusinessSearchList
            if not any(k in url for k in ("getbusinesssearchlist", "businesssearch", "search")):
                continue

            extracted = _extract_items(parsed)
            if extracted:
                items = extracted
                logger.info("WA: search XHR matched (%s) → %d items", url_raw[:120], len(items))
                break

        if not items:
            logger.warning("WA: пошук не повернув результатів для '%s'.", keyword)
            logger.warning("WA: усі побачені XHR (%d) — для діагностики ендпоінта:",
                           len(seen_xhrs))
            for u, prev in seen_xhrs:
                logger.warning("  XHR: %s\n      body: %s", u[:160], prev)
            return results

        for item in items:
            if len(results) >= count:
                break
            if not status.get('is_running', True):
                break

            biz_status = (
                item.get("BusinessStatus") or item.get("BusinessStatusCode")
                or item.get("Status") or ""
            ).strip().upper()
            if biz_status not in _ACTIVE_VALUES:
                logger.debug("⏩ WA: skip non-active '%s'", biz_status)
                continue

            biz_name = (
                item.get("BusinessName") or item.get("Name") or ""
            ).strip()
            if not biz_name:
                continue

            if database.is_company_name_scraped(biz_name):
                logger.debug("⏩ WA: вже в базі: %s", biz_name)
                continue

            biz_id = item.get("BusinessId") or item.get("BusinessID") or item.get("Id")
            ubi = (item.get("UBINumber") or item.get("UBI") or "").strip()
            filed = (
                item.get("BusinessFilingDate") or item.get("FormationDate")
                or item.get("FilingDate") or ""
            )[:10]

            status['last_name'] = f"📄 WA: {biz_name}"

            filings = _fetch_filings_via_browser(page, biz_id) if biz_id else []
            ar_link, ar_date = _pick_latest(
                filings,
                include_kw=_ANNUAL_KEYWORDS,
                exclude_kw=_EXPRESS_KEYWORDS,   # звичайний annual ≠ express
            )
            ex_link, ex_date = _pick_latest(filings, include_kw=_EXPRESS_KEYWORDS)

            # Якщо нема ЖОДНОГО annual-доку — пропускаємо компанію.
            # Юзер просив обидва типи для кожної компанії — без них вона безкорисна.
            if not ar_link and not ex_link:
                logger.debug("⏩ WA: немає annual report: %s (filings=%d)",
                             biz_name, len(filings))
                continue

            # ── Адреса (Principal Office) ──
            # CCFS повертає в полях PrincipalOfficeStreetAddressLine1/City/State/Zip.
            # Назви ключів варіюються; підбираємо case-insensitive.
            def _g(*keys):
                for k in keys:
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                return ""

            street = _g("PrincipalOfficeStreetAddressLine1",
                        "PrincipalOfficeAddressLine1", "StreetAddress1",
                        "Address", "BusinessStreetAddress")
            city = _g("PrincipalOfficeCity", "City", "BusinessCity")
            state_us = _g("PrincipalOfficeState", "State", "BusinessState")
            zip_code = _g("PrincipalOfficeZip", "PrincipalOfficeZipCode",
                          "Zip", "BusinessZip", "PostalCode")

            results.append({
                "Назва":                        biz_name,
                "UBI Number":                   ubi,
                "Статус":                       "Active",
                "Дата подання":                 filed,
                "Address":                      street,
                "City":                         city,
                "State":                        state_us or "WA",
                "Zip":                          zip_code,
                "Annual Report (Link)":         ar_link or "—",
                "Annual Report (Date)":         ar_date or "",
                "Express Annual Report (Link)": ex_link or "—",
                "Express Annual Report (Date)": ex_date or "",
                "BusinessId":                   biz_id,
            })

            status['current'] += 1
            logger.info(
                "[%d] WA %s | UBI=%s | AR=%s | Express=%s",
                len(results), biz_name, ubi or "—",
                "✓" if ar_link else "—",
                "✓" if ex_link else "—",
            )

    except Exception as e:
        logger.error("WA scraper error: %s", e, exc_info=True)
    finally:
        try:
            page.listen.stop()
        except Exception:
            pass

    return results
