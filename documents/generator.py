"""
generator.py — Генератор сертифікатів (Certificate of Incorporation, India) з .docx-шаблону.

Замінює попередній PNG-генератор паспортів. Тепер:
  - Підстановка даних у .docx-шаблон: заміна плейсхолдерів {{...}} у word/document.xml.
    Форматування, шрифти та зображення шаблону зберігаються повністю — використовується
    лише stdlib `zipfile`, жодних зовнішніх залежностей для рендеру.
  - Автогенерація двох дат словами: дата видачі СТРОГО пізніше дати інкорпорації.
  - Пул підписантів (реєстраторів) — випадковий вибір із config.json.
  - Парсинг TXT з кількома компаніями → пакетна генерація.

Структура шаблону:
  documents/templates/india_incorporation/
    template.docx   ← .docx з плейсхолдерами {{COMPANY_NAME}}, {{CIN}}, {{ADDRESS}},
                       {{DATE_INCORP}}, {{DATE_ISSUE}}, {{SIGNATORY}}
    config.json     ← опис шаблону, пул підписантів, діапазон дат

Поля (config.json → fields):
  source = "input"           → значення береться з TXT (company_name / cin / address)
  source = "auto_date"       → дата інкорпорації (генерується)
  source = "auto_date_after" → дата видачі (генерується строго пізніше інкорпорації)
  source = "auto_pool"       → випадковий підписант із signatory_pool
"""
from __future__ import annotations

import io
import json
import logging
import random
import re
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Числівники англійською (дати словами) ────────────────────────────────

_ONES = ["Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight",
         "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen",
         "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
_TENS = {2: "Twenty", 3: "Thirty", 4: "Forty", 5: "Fifty",
         6: "Sixty", 7: "Seventy", 8: "Eighty", 9: "Ninety"}

_ORDINAL = {
    1: "First", 2: "Second", 3: "Third", 4: "Fourth", 5: "Fifth", 6: "Sixth",
    7: "Seventh", 8: "Eighth", 9: "Ninth", 10: "Tenth", 11: "Eleventh",
    12: "Twelfth", 13: "Thirteenth", 14: "Fourteenth", 15: "Fifteenth",
    16: "Sixteenth", 17: "Seventeenth", 18: "Eighteenth", 19: "Nineteenth",
    20: "Twentieth", 30: "Thirtieth",
}

_MONTHS = ["", "January", "February", "March", "April", "May", "June",
           "July", "August", "September", "October", "November", "December"]


def _two_digit_words(n: int) -> str:
    """0..99 → 'Twenty Five' (Title Case)."""
    if n < 20:
        return _ONES[n]
    t, o = divmod(n, 10)
    return _TENS[t] + (" " + _ONES[o] if o else "")


def ordinal_words(n: int) -> str:
    """День місяця 1..31 → 'Thirteenth' / 'Twenty First' (Title Case)."""
    if n in _ORDINAL:
        return _ORDINAL[n]
    t, o = divmod(n, 10)               # 21..29, 31
    return _TENS[t] + " " + _ORDINAL[o]


def year_words(y: int) -> str:
    """Рік 2000..2099 → 'Two Thousand Twenty Five' (Title Case)."""
    if 2000 <= y <= 2099:
        rem = y - 2000
        return "Two Thousand" + (" " + _two_digit_words(rem) if rem else "")
    return str(y)                       # fallback для нетипових років


def _cap_first_only(s: str) -> str:
    """'Two Thousand Twenty One' → 'Two thousand twenty one'."""
    return s[:1].upper() + s[1:].lower()


def format_date_incorp(d: date) -> str:
    """Формат дати інкорпорації: 'Thirteenth Day of June Two Thousand Twenty Five'."""
    return f"{ordinal_words(d.day)} Day of {_MONTHS[d.month]} {year_words(d.year)}"


def format_date_issue(d: date) -> str:
    """Формат дати видачі: 'Eleventh day of June Two thousand twenty one'."""
    return f"{ordinal_words(d.day)} day of {_MONTHS[d.month]} {_cap_first_only(year_words(d.year))}"


def generate_dates(min_year: int = 2016, max_year: int = 2025,
                   issue_offset_days: tuple[int, int] = (1, 60),
                   fixed_year: int | None = None) -> tuple[str, str, date, date]:
    """Генерує пару дат: (str_інкорпорації, str_видачі, date_інкорпорації, date_видачі).

    Гарантії:
      - дата видачі СТРОГО пізніше дати інкорпорації (offset >= 1 день, тому не рівні);
      - різниця у розумних межах issue_offset_days.

    Якщо fixed_year задано (напр. рік із CIN), дата інкорпорації генерується
    строго в межах цього року, а min_year/max_year ігноруються.
    """
    if fixed_year is not None:
        start = date(fixed_year, 1, 1)
        end = date(fixed_year, 12, 31)
    else:
        start = date(min_year, 1, 1)
        end = date(max_year, 12, 31)
    span = (end - start).days
    incorp = start + timedelta(days=random.randint(0, max(span, 0)))

    low, high = issue_offset_days
    low = max(1, int(low))              # мінімум 1 день → строго пізніше й не рівні
    high = max(low, int(high))
    issue = incorp + timedelta(days=random.randint(low, high))

    return format_date_incorp(incorp), format_date_issue(issue), incorp, issue


# ── Парсер TXT з кількома компаніями ─────────────────────────────────────

# CIN Індії: U55101KL2025PTC095056 — літера + 5 цифр + 2 літери + 4 цифри + 3 літери + 6 цифр
_CIN_RE = re.compile(r"^[A-Za-z]\d{5}[A-Za-z]{2}\d{4}[A-Za-z]{3}\d{6}$")


def cin_year(cin: str) -> int | None:
    """Рік реєстрації з CIN (4 цифри в позиціях 9–12, напр. ...2025...).

    Повертає None, якщо CIN не відповідає формату або рік поза 2000–2099
    (у такому разі дати генеруються за діапазоном config.json).
    """
    if not cin:
        return None
    c = cin.replace(" ", "")
    if not _CIN_RE.match(c):
        return None
    year = int(c[8:12])
    return year if 2000 <= year <= 2099 else None

_KEY_ALIASES: dict[str, set[str]] = {
    "company_name": {
        "company", "company name", "name", "назва", "назва компанії",
        "название", "название компании", "компания", "компанія", "наименование",
    },
    "cin": {"cin", "cin number", "cin no", "cin номер", "кін"},
    "address": {
        "address", "addr", "mailing address", "адрес", "адреса",
        "почтовый адрес", "поштова адреса",
    },
}

# Рядок-роздільник у «красивому» експорті скрапера (═══ / ─── / ─ тощо).
_SEP_LINE_RE = re.compile(r"^[═─—–\-=_*·•]{3,}$")
# Службовий рядок-лічильник заголовка ("Зібрано компаній: N").
_COUNTER_RE = re.compile(r"^(зібрано|собрано|collected)\s+компан", re.IGNORECASE)
# Заголовок компанії у форматі експорту: "#1  COMPANY NAME".
_NUM_TITLE_RE = re.compile(r"^#\s*\d+\s+(.+)$")


def _canon_key(raw: str) -> str:
    """Нормалізує ключ рядка 'Company Name' → 'company_name' (або '' якщо невідомий)."""
    s = re.sub(r"\s+", " ", raw.strip().lower().replace("_", " "))
    for field, aliases in _KEY_ALIASES.items():
        if s in aliases:
            return field
    return ""


def parse_companies_txt(text: str) -> list[dict[str, str]]:
    """Розбирає TXT з кількома компаніями → список {'company_name','cin','address'}.

    Приймає ДВА формати:

    1) Простий (ручний) — блоки, розділені порожнім рядком. У блоці:
         - 'ключ: значення' (Company / CIN / Address; двокрапка або '='), або
         - просто 3 рядки поспіль (назва, CIN, адреса) позиційно.

    2) «Красивий» експорт скрапера (кнопка TXT у результатах пошуку):
           ════════════════════════════════════
             Зібрано компаній: 50
           ════════════════════════════════════

           #1  ATMA CONSTRUCTION SYSTEM PRIVATE LIMITED
               CIN              : U45309MP2020PTC053349
               Статус           : ACTIVE
               Адреса           : 56 G SANOUSI ...
               Посилання на PDF : https://www.mca.gov.in/ (verify by CIN)
           ────────────────────────────────────
           #2  ...
       Тут блоки розділені лінією-роздільником (═══/───), заголовок має
       префікс '#N', а зайві поля (Статус, Клас, Штат, RoC, Посилання…)
       ігноруються — беруться лише назва (#N), CIN та Адреса.

    CIN розпізнається автоматично за форматом, навіть без ключа.
    Блоки без назви компанії пропускаються.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Лінії-роздільники (═══/───) трактуємо як межу блоку — так формат
    # експорту скрапера (де компанії розділені лінією, а не порожнім рядком)
    # теж коректно розбивається на блоки.
    text = "\n".join(
        "" if _SEP_LINE_RE.match(ln.strip()) else ln
        for ln in text.split("\n")
    )

    companies: list[dict[str, str]] = []

    for block in re.split(r"\n\s*\n", text.strip()):
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        # службовий рядок-лічильник заголовка ("Зібрано компаній: N") → геть
        lines = [ln for ln in lines if not _COUNTER_RE.match(ln)]
        if not lines:
            continue

        rec = {"company_name": "", "cin": "", "address": ""}
        positional: list[str] = []
        # «Красивий» блок пізнається за заголовком '#N NAME'. У ньому кожен
        # рядок — це поле, тож незнані мітки (Статус, Штат, RoC, Посилання…)
        # відкидаємо, а не зливаємо в адресу.
        pretty = any(_NUM_TITLE_RE.match(ln) for ln in lines)

        # 1) рядки виду '#N NAME' та 'ключ: значення'
        for ln in lines:
            m_title = _NUM_TITLE_RE.match(ln)
            if m_title:
                name = m_title.group(1).strip()
                if name and name != "—" and not rec["company_name"]:
                    rec["company_name"] = name
                continue

            m = re.match(r"^([^:=]{1,40})[:=]\s*(.+)$", ln)
            if m:
                key = _canon_key(m.group(1))
                val = m.group(2).strip()
                if key:
                    if not rec[key]:
                        rec[key] = val
                    continue
                if pretty:
                    continue          # незнане поле експорту → ігноруємо
            elif pretty:
                continue              # у «красивому» блоці голих рядків не буває
            positional.append(ln)

        # 2) автовизначення CIN серед позиційних рядків
        for ln in list(positional):
            if not rec["cin"] and _CIN_RE.match(ln.replace(" ", "")):
                rec["cin"] = ln.replace(" ", "").upper()
                positional.remove(ln)

        # 3) решта позиційних → назва, потім адреса (зайві рядки додаються до адреси)
        for ln in positional:
            if not rec["company_name"]:
                rec["company_name"] = ln
            elif not rec["address"]:
                rec["address"] = ln
            else:
                rec["address"] += " " + ln

        if rec["company_name"]:
            companies.append(rec)

    return companies


# ── Робота з .docx (лише stdlib zipfile) ─────────────────────────────────

def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _sanitize_filename(s: str, maxlen: int = 60) -> str:
    keep = "".join(c for c in s if c.isalnum() or c in " _-").strip()
    keep = re.sub(r"\s+", "_", keep)
    return (keep[:maxlen] or "document").rstrip("_")


# ── Шаблон сертифіката ───────────────────────────────────────────────────

class CertificateTemplate:
    """Один .docx-шаблон із плейсхолдерами {{...}} + правила заповнення з config.json."""

    def __init__(self, template_dir: str | Path):
        self.template_dir = Path(template_dir)
        cfg_path = self.template_dir / "config.json"
        if not cfg_path.exists():
            raise FileNotFoundError(f"config.json не знайдено: {cfg_path}")

        with open(cfg_path, encoding="utf-8") as f:
            self.config: dict = json.load(f)

        tpl_name = self.config.get("template_file", "template.docx")
        self.template_file = self.template_dir / tpl_name
        if not self.template_file.exists():
            raise FileNotFoundError(f"{tpl_name} не знайдено: {self.template_file}")

        self._template_bytes = self.template_file.read_bytes()
        self.signatory_pool: list[str] = list(self.config.get("signatory_pool", []))

        dr = self.config.get("date_range", {})
        self._min_year = int(dr.get("min_year", 2016))
        self._max_year = int(dr.get("max_year", 2025))
        off = dr.get("issue_offset_days", [1, 60])
        self._issue_offset = (int(off[0]), int(off[1]))

        logger.info("CertificateTemplate '%s' завантажено (%d полів, %d підписантів)",
                    self.config.get("name", self.template_dir.name),
                    len(self.config.get("fields", {})), len(self.signatory_pool))

    # ── метадані ──
    @property
    def name(self) -> str:
        return self.config.get("name", self.template_dir.name)

    @property
    def description(self) -> str:
        return self.config.get("description", self.name)

    def input_fields(self) -> list[tuple[str, dict]]:
        """Поля, що заповнюються з TXT (source == 'input')."""
        return [(k, c) for k, c in self.config.get("fields", {}).items()
                if c.get("source") == "input"]

    # ── резолвинг значень → {token: value} ──
    def _resolve(self, provided: dict[str, Any]) -> dict[str, str]:
        fields: dict = self.config.get("fields", {})
        # Якщо у CIN є рік реєстрації — прив'язуємо дату інкорпорації до нього,
        # щоб рік у CIN і згенерована дата не суперечили одне одному.
        year = cin_year(str(provided.get("cin", "") or ""))
        s_incorp, s_issue, _, _ = generate_dates(
            self._min_year, self._max_year, self._issue_offset, fixed_year=year)
        signatory = random.choice(self.signatory_pool) if self.signatory_pool else ""

        tokens: dict[str, str] = {}
        for key, cfg in fields.items():
            placeholder = cfg.get("placeholder")
            if not placeholder:
                continue
            source = cfg.get("source", "input")
            if source == "auto_date":
                val = provided.get(key) or s_incorp
            elif source == "auto_date_after":
                val = provided.get(key) or s_issue
            elif source == "auto_pool":
                val = provided.get(key) or signatory
            else:  # input
                val = provided.get(key, cfg.get("default", ""))
            tokens[placeholder] = str(val)
        return tokens

    # ── рендер одного документа ──
    def render(self, data: dict[str, Any]) -> bytes:
        """Заповнює шаблон і повертає .docx-байти.

        data: {'company_name','cin','address', ...}. Авто-поля (дати, підписант)
        генеруються, якщо не передані явно.
        """
        token_values = self._resolve(data)
        return self._apply(token_values)

    def _apply(self, token_values: dict[str, str]) -> bytes:
        src = io.BytesIO(self._template_bytes)
        out = io.BytesIO()
        with zipfile.ZipFile(src) as zin, \
             zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                content = zin.read(item.filename)
                if item.filename == "word/document.xml":
                    text = content.decode("utf-8")
                    for token, value in token_values.items():
                        text = text.replace(token, _xml_escape(value))
                    content = text.encode("utf-8")
                zout.writestr(item, content)
        return out.getvalue()

    def render_pdf(self, data: dict[str, Any]) -> bytes:
        """Заповнює шаблон і повертає PDF-байти (через LibreOffice).

        Викликає pdf_convert.PdfConversionError, якщо LibreOffice недоступний.
        """
        from documents import pdf_convert
        return pdf_convert.convert_one(self.render(data))

    # ── пакетна генерація ──
    def render_many(self, companies: list[dict[str, Any]]) -> list[tuple[str, bytes]]:
        """Рендерить документ для кожної компанії → [(filename.docx, bytes), ...]."""
        results: list[tuple[str, bytes]] = []
        for i, company in enumerate(companies, 1):
            try:
                doc = self.render(company)
                fname = f"{i:03d}_{_sanitize_filename(company.get('company_name', ''))}.docx"
                results.append((fname, doc))
            except Exception as e:
                logger.warning("render_many: компанія #%d помилка: %s", i, e)
        return results

    def render_zip(self, companies: list[dict[str, Any]], fmt: str = "docx") -> bytes:
        """Пакетна генерація → ZIP-байти.

        fmt='docx' (типово) — .docx-файли; fmt='pdf' — конвертує кожен документ
        у PDF через LibreOffice (pdf_convert). Для 'pdf' потрібен встановлений
        LibreOffice; інакше — pdf_convert.PdfConversionError.
        """
        rendered = self.render_many(companies)
        if fmt.lower() == "pdf":
            from documents import pdf_convert
            rendered = pdf_convert.convert_docx_to_pdf(rendered)

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname, doc in rendered:
                zf.writestr(fname, doc)
        return zip_buf.getvalue()


# ── Реєстр шаблонів (сумісність із bot.py) ───────────────────────────────

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_registry: dict[str, CertificateTemplate] = {}


def load_all_templates(templates_dir: str | Path = _TEMPLATES_DIR) -> None:
    global _registry
    base = Path(templates_dir)
    if not base.exists():
        logger.warning("Папка templates/ не знайдена: %s", base)
        return
    for folder in base.iterdir():
        if not folder.is_dir():
            continue
        cfg_path = folder / "config.json"
        if not cfg_path.exists():
            continue
        # Визначаємо файл шаблону, щоб тихо пропускати не-docx теки
        # (напр. залишки старих PNG-шаблонів без template.docx).
        try:
            with open(cfg_path, encoding="utf-8") as f:
                tpl_file = json.load(f).get("template_file", "template.docx")
        except (OSError, json.JSONDecodeError):
            continue
        if not (folder / tpl_file).exists():
            logger.debug("Пропускаю теку без .docx-шаблону: %s", folder.name)
            continue
        try:
            _registry[folder.name] = CertificateTemplate(folder)
            logger.info("Шаблон завантажено: %s", folder.name)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning("Пропускаю %s: %s", folder.name, e)


def get_template(name: str) -> CertificateTemplate | None:
    return _registry.get(name)


def list_templates() -> list[str]:
    return list(_registry.keys())


# ── Приклад TXT для користувача ──────────────────────────────────────────

SAMPLE_TXT = """\
Company: COMFYVALLEY PRIVATE LIMITED
CIN: U55101KL2025PTC095056
Address: BUILDING NO. AP X/286,VETTUROAD, KANIYAPURAM,Thiruvananthapuram,Kerala,695582-India

Company: GREENLEAF TRADING PRIVATE LIMITED
CIN: U74999MH2022PTC123456
Address: 12 MG ROAD, ANDHERI EAST, Mumbai, Maharashtra, 400069-India

Company: BLUEHARBOR LOGISTICS PRIVATE LIMITED
CIN: U63030DL2021PTC987654
Address: PLOT 45, SECTOR 18, Dwarka, New Delhi, Delhi, 110078-India
"""
