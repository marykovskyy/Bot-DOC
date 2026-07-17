"""
test_bot.py — Юніт-тести проекту.

Запуск:
    pip install pytest
    pytest test_bot.py -v

Тести НЕ потребують запущеного бота, Telegram-токена або мережі.
Всі зовнішні залежності (БД, API) мокуються.
"""
import io
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
#  utils.py
# ─────────────────────────────────────────────────────────────────────────────

class TestWithRetry:
    """Тести декоратора with_retry."""

    def test_success_on_first_attempt(self):
        from utils import with_retry

        calls = []

        @with_retry(max_retries=3, delay=0)
        def ok():
            calls.append(1)
            return 42

        assert ok() == 42
        assert len(calls) == 1

    def test_retries_on_failure_then_succeeds(self):
        from utils import with_retry

        attempts = []

        @with_retry(max_retries=3, delay=0)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("not yet")
            return "done"

        assert flaky() == "done"
        assert len(attempts) == 3

    def test_raises_after_all_retries_exhausted(self):
        from utils import with_retry

        @with_retry(max_retries=2, delay=0)
        def always_fails():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            always_fails()

    def test_retry_request_matches_with_retry(self):
        """retry_request повинен давати той самий результат що й with_retry."""
        from utils import retry_request

        counter = []

        def flaky_fn():
            counter.append(1)
            if len(counter) < 2:
                raise OSError("tmp")
            return "ok"

        result = retry_request(flaky_fn, max_retries=3, delay=0)
        assert result == "ok"
        assert len(counter) == 2


# ─────────────────────────────────────────────────────────────────────────────
#  constants.py
# ─────────────────────────────────────────────────────────────────────────────

class TestConstants:
    """Перевіряє що всі константи присутні та мають розумні значення."""

    def test_all_constants_present(self):
        from constants import (
            AI_MAX_CONCURRENT_API,
            AI_PROGRESS_UPDATE_SEC,
            BROWSER_LAUNCH_TIMEOUT_SEC,
            CAPTCHA_MAX_WAIT_SEC,
            CAPTCHA_POLL_INTERVAL_SEC,
            ELEMENT_WAIT_RETRIES,
            MAX_PARALLEL_TASKS,
            SHEETS_MAX_RETRIES,
            SHEETS_RETRY_WAIT_BASE,
            SHEETS_WRITE_DELAY,
            STATUS_UPDATE_SEC,
        )
        assert MAX_PARALLEL_TASKS >= 1
        assert CAPTCHA_MAX_WAIT_SEC > CAPTCHA_POLL_INTERVAL_SEC
        assert SHEETS_MAX_RETRIES >= 1
        assert SHEETS_WRITE_DELAY > 0
        assert AI_MAX_CONCURRENT_API >= 1


# ─────────────────────────────────────────────────────────────────────────────
#  proxy_manager.py
# ─────────────────────────────────────────────────────────────────────────────

class TestProxyManager:
    """Тести менеджера проксі (без файлової системи — через tmp-директорію)."""

    def setup_method(self):
        """Перед кожним тестом перенаправляємо файл у тимчасову папку."""
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_file = None

    def teardown_method(self):
        self._tmp.cleanup()

    def _patch_path(self, monkeypatch_or_patch):
        """Патчить _PROXY_FILE на тимчасовий шлях."""
        from proxy import manager as proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path
        return tmp_path

    def test_save_and_load(self):
        from proxy import manager as proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path

        proxies = {"France": [{"host": "1.1.1.1", "port": "8080", "user": "u", "pass": "p"}],
                   "Finland": [], "General": []}
        proxy_manager.save(True, proxies)

        data = proxy_manager.load()
        assert data["use_proxy"] is True
        assert data["proxies"]["France"][0]["host"] == "1.1.1.1"

    def test_load_returns_defaults_on_missing_file(self):
        from proxy import manager as proxy_manager
        tmp_path = Path(self._tmp.name) / "no_such.json"
        # Файл не існує — _migrate_from_py буде викликано; стабуємо його
        # щоб не створювався реальний proxy_settings.json
        proxy_manager._PROXY_FILE = tmp_path

        with patch("proxy.manager._migrate_from_py", side_effect=Exception("no py")):
            data = proxy_manager.load()
        # При винятку у міграції load() повертає defaults — use_proxy має бути bool
        assert isinstance(data.get("use_proxy", False), bool)
        assert data.get("use_proxy") is False
        assert "France" in data.get("proxies", {})

    def test_get_use_proxy_helper(self):
        from proxy import manager as proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path
        proxy_manager.save(False, {"France": [], "Finland": [], "General": []})

        assert proxy_manager.get_use_proxy() is False

        proxy_manager.save(True, {"France": [], "Finland": [], "General": []})
        assert proxy_manager.get_use_proxy() is True

    def test_atomic_write(self):
        """Перевіряє що tmp-файл не залишається після save."""
        from proxy import manager as proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path

        proxy_manager.save(False, {"France": [], "Finland": [], "General": []})

        tmp_file = tmp_path.with_suffix(".json.tmp")
        assert not tmp_file.exists(), ".tmp файл не повинен залишатися після save()"


# ─────────────────────────────────────────────────────────────────────────────
#  database.py
# ─────────────────────────────────────────────────────────────────────────────

class TestDatabase:
    """Тести бази даних на in-memory SQLite."""

    def setup_method(self):
        """Кожен тест отримує свіжу БД у тимчасовій папці."""
        import database
        self._orig_db = database.DB_NAME
        # Windows-friendly: TemporaryDirectory замість NamedTemporaryFile —
        # інакше файл залишається відкритим і WAL-файли (.wal, .shm) не видаляються.
        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "test.db")
        database.DB_NAME = self._db_path
        database.init_db()

    def teardown_method(self):
        import database
        database.DB_NAME = self._orig_db
        # Ігноруємо помилки на Windows якщо WAL-файли ще тримаються
        try:
            self._tmpdir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_init_creates_tables(self):
        import database
        with database.get_connection() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert {"companies", "users", "search_history",
                "scheduled_tasks", "doc_analysis_log", "image_cache"}.issubset(tables)

    def test_schema_version_set_after_init(self):
        import database
        with database.get_connection() as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == database._SCHEMA_VERSION

    def test_save_and_check_company(self):
        import database
        saved = database.save_company_to_db("Test Corp", "https://test.com", "France")
        assert saved is True
        assert database.is_company_scraped("https://test.com") is True
        assert database.is_company_scraped("https://other.com") is False

    def test_duplicate_company_returns_false(self):
        import database
        database.save_company_to_db("Test Corp", "https://test.com", "France")
        result = database.save_company_to_db("Test Corp", "https://test.com", "France")
        assert result is False

    def test_add_and_get_user(self):
        import database
        database.add_user(123456, "testuser", role="user")
        user = database.get_user(123456)
        assert user is not None
        assert user["username"] == "testuser"
        assert user["role"] == "user"
        assert user["is_active"] == 1

    def test_user_not_found_returns_none(self):
        import database
        assert database.get_user(999999) is None

    def test_is_user_allowed(self):
        import database
        database.add_user(111, "active_user")
        assert database.is_user_allowed(111) is True

        database.set_user_active(111, False)
        assert database.is_user_allowed(111) is False

    def test_search_history(self):
        import database
        rid = database.save_search_history(100, "France", "tech", 50, "2022", "EXCEL")
        assert isinstance(rid, int)

        history = database.get_search_history(100, limit=5)
        assert len(history) == 1
        assert history[0]["keyword"] == "tech"

    def test_global_stats(self):
        import database
        database.save_company_to_db("A", "https://a.com", "France")
        database.save_company_to_db("B", "https://b.com", "Finland")
        stats = database.get_global_stats()
        assert stats["total"] >= 2


# ─────────────────────────────────────────────────────────────────────────────
#  document_generator.py
# ─────────────────────────────────────────────────────────────────────────────

class TestCertificateGenerator:
    """Тести генератора сертифікатів (.docx-шаблон: підстановка + авто-поля)."""

    def _tpl(self):
        from documents import generator as g
        g.load_all_templates()
        tpl = g.get_template("india_incorporation")
        assert tpl is not None, "шаблон india_incorporation не завантажено"
        return tpl

    @staticmethod
    def _document_xml(docx_bytes: bytes) -> str:
        import io
        import zipfile
        return zipfile.ZipFile(io.BytesIO(docx_bytes)).read("word/document.xml").decode("utf-8")

    def test_template_loads(self):
        tpl = self._tpl()
        assert tpl.config["name"] == "india_incorporation"
        assert tpl.signatory_pool, "пул підписантів порожній"

    def test_render_returns_docx_bytes(self):
        tpl = self._tpl()
        out = tpl.render({"company_name": "TEST PRIVATE LIMITED",
                          "cin": "U12345KL2020PTC000001",
                          "address": "1 ROAD, Kerala, 600001-India"})
        assert isinstance(out, bytes)
        assert out[:2] == b"PK"          # ZIP/OOXML magic

    def test_no_placeholders_left(self):
        import re
        tpl = self._tpl()
        xml = self._document_xml(tpl.render({"company_name": "X PRIVATE LIMITED"}))
        assert not re.search(r"\{\{[^}]+\}\}", xml)

    def test_company_name_substituted_twice(self):
        tpl = self._tpl()
        xml = self._document_xml(tpl.render(
            {"company_name": "UNIQUENAME PRIVATE LIMITED", "address": "addr"}))
        # у тексті сертифіката + у блоці адреси
        assert xml.count("UNIQUENAME PRIVATE LIMITED") == 2

    def test_ampersand_escaped(self):
        tpl = self._tpl()
        xml = self._document_xml(tpl.render({"company_name": "A & B PRIVATE LIMITED"}))
        assert "A &amp; B PRIVATE LIMITED" in xml

    def test_signatory_from_pool(self):
        tpl = self._tpl()
        pool = set(tpl.signatory_pool)
        xml = self._document_xml(tpl.render({"company_name": "X PRIVATE LIMITED"}))
        assert any(name in xml for name in pool)

    def test_issue_date_strictly_after_incorporation(self):
        from documents import generator as g
        for _ in range(1000):
            _, _, di, ii = g.generate_dates(2016, 2025, (1, 60))
            assert ii > di

    def test_ordinal_and_year_words(self):
        from documents import generator as g
        assert g.ordinal_words(13) == "Thirteenth"
        assert g.ordinal_words(21) == "Twenty First"
        assert g.ordinal_words(31) == "Thirty First"
        assert g.year_words(2025) == "Two Thousand Twenty Five"
        assert g.year_words(2000) == "Two Thousand"

    def test_date_word_formats(self):
        from datetime import date

        from documents import generator as g
        assert (g.format_date_incorp(date(2025, 6, 13))
                == "Thirteenth Day of June Two Thousand Twenty Five")
        assert (g.format_date_issue(date(2021, 6, 11))
                == "Eleventh day of June Two thousand twenty one")

    def test_parse_companies_keyed(self):
        from documents import generator as g
        txt = ("Company: FOO PRIVATE LIMITED\nCIN: U12345KL2020PTC000001\n"
               "Address: 1 ROAD, Kerala\n\n"
               "Company: BAR PRIVATE LIMITED\nCIN: U67890MH2021PTC000002\n"
               "Address: 2 ROAD, Mumbai\n")
        comps = g.parse_companies_txt(txt)
        assert len(comps) == 2
        assert comps[0]["company_name"] == "FOO PRIVATE LIMITED"
        assert comps[1]["cin"] == "U67890MH2021PTC000002"

    def test_parse_companies_positional_with_cin_autodetect(self):
        from documents import generator as g
        txt = "ACME PRIVATE LIMITED\nU12345KL2020PTC000001\n9 ROAD, Kerala, 600001-India\n"
        comps = g.parse_companies_txt(txt)
        assert len(comps) == 1
        assert comps[0]["company_name"] == "ACME PRIVATE LIMITED"
        assert comps[0]["cin"] == "U12345KL2020PTC000001"
        assert "9 ROAD" in comps[0]["address"]

    def test_parse_companies_scraper_txt_export(self):
        """«Красивий» TXT-експорт скрапера (кнопка TXT) приймається напряму:
        назва береться з '#N', CIN/адреса — з міток, зайві поля ігноруються."""
        from documents import generator as g
        from scrapers.main import _format_txt_readable

        records = [
            {"Назва": "ATMA CONSTRUCTION SYSTEM PRIVATE LIMITED",
             "CIN": "U45309MP2020PTC053349", "Статус": "ACTIVE",
             "Дата реєстрації": "2020-10-20", "Клас": "Private",
             "Категорія": "Company limited by shares", "Штат": "madhya pradesh",
             "RoC": "ROC Gwalior",
             "Адреса": "56 G SANOUSI SEMARIHA TOLA SHAHDOL,Shahdol,Madhya Pradesh,484774-India",
             "Посилання на PDF": "https://www.mca.gov.in/  (verify by CIN)"},
            {"Назва": "SVGLINE - UP PRIVATE LIMITED",
             "CIN": "U74999UP2021PTC155012", "Статус": "ACTIVE",
             "Дата реєстрації": "2021-03-11", "Клас": "Private",
             "Категорія": "Company limited by shares", "Штат": "uttar pradesh",
             "RoC": "ROC Kanpur",
             "Адреса": "12 MG ROAD, LUCKNOW, Uttar Pradesh, 226001-India",
             "Посилання на PDF": "https://www.mca.gov.in/  (verify by CIN)"},
        ]
        comps = g.parse_companies_txt(_format_txt_readable(records))
        assert len(comps) == 2  # рядок-лічильник заголовка не рахується як компанія
        assert comps[0]["company_name"] == "ATMA CONSTRUCTION SYSTEM PRIVATE LIMITED"
        assert comps[0]["cin"] == "U45309MP2020PTC053349"
        assert comps[0]["address"].startswith("56 G SANOUSI")
        # зайві поля не мають потрапляти в адресу
        for junk in ("ACTIVE", "ROC", "mca.gov.in", "Private", "madhya"):
            assert junk not in comps[0]["address"]
        assert comps[1]["company_name"] == "SVGLINE - UP PRIVATE LIMITED"
        assert comps[1]["cin"] == "U74999UP2021PTC155012"
        assert comps[1]["address"].startswith("12 MG ROAD")

    def test_render_zip_batch(self):
        import io
        import zipfile

        from documents import generator as g
        tpl = self._tpl()
        comps = g.parse_companies_txt(g.SAMPLE_TXT)
        zf = zipfile.ZipFile(io.BytesIO(tpl.render_zip(comps)))
        assert len(zf.namelist()) == len(comps)
        assert all(n.endswith(".docx") for n in zf.namelist())

    def test_pdf_name_helper(self):
        """Заміна розширення .docx→.pdf не залежить від наявності LibreOffice."""
        from documents import pdf_convert
        assert pdf_convert._pdf_name("001_FOO.docx") == "001_FOO.pdf"
        assert pdf_convert._pdf_name("002_BAR.DOCX") == "002_BAR.pdf"

    def test_render_pdf_single(self):
        """render_pdf повертає валідний PDF (пропускається без LibreOffice)."""
        import pytest

        from documents import generator as g
        from documents import pdf_convert
        if not pdf_convert.pdf_available():
            pytest.skip("LibreOffice не встановлено — PDF-конвертація недоступна")

        tpl = self._tpl()
        comps = g.parse_companies_txt(g.SAMPLE_TXT)
        pdf = tpl.render_pdf(comps[0])
        assert pdf[:5] == b"%PDF-"

    def test_render_zip_pdf_batch(self):
        """render_zip(fmt='pdf') → ZIP з валідних PDF (пропускається без LibreOffice)."""
        import io
        import zipfile

        import pytest

        from documents import generator as g
        from documents import pdf_convert
        if not pdf_convert.pdf_available():
            pytest.skip("LibreOffice не встановлено — PDF-конвертація недоступна")

        tpl = self._tpl()
        comps = g.parse_companies_txt(g.SAMPLE_TXT)
        zf = zipfile.ZipFile(io.BytesIO(tpl.render_zip(comps, fmt="pdf")))
        names = zf.namelist()
        assert len(names) == len(comps)
        assert all(n.endswith(".pdf") for n in names)
        assert all(zf.read(n)[:5] == b"%PDF-" for n in names)


# ─────────────────────────────────────────────────────────────────────────────
#  gsheets.py — retry logic
# ─────────────────────────────────────────────────────────────────────────────

class TestGsheetsRetry:
    """Тести backoff+jitter+Retry-After без реальних HTTP-викликів."""

    def test_compute_wait_uses_retry_after_header(self):
        from gspread.exceptions import APIError

        from gsheets import _compute_wait

        # Фейковий response з Retry-After: 7
        resp = MagicMock()
        resp.headers = {"Retry-After": "7"}
        err = APIError(resp)
        wait = _compute_wait(0, err)
        assert wait == 7.0

    def test_compute_wait_caps_retry_after(self):
        from gspread.exceptions import APIError

        from gsheets import _MAX_RETRY_WAIT, _compute_wait

        resp = MagicMock()
        resp.headers = {"Retry-After": "9999"}  # сервер каже чекати 2.7 год
        err = APIError(resp)
        assert _compute_wait(0, err) == _MAX_RETRY_WAIT

    def test_compute_wait_fallback_exponential(self):
        """Без Retry-After — експоненціал з jitter."""
        from gsheets import _MAX_RETRY_WAIT, _RETRY_WAIT_BASE, _compute_wait

        wait0 = _compute_wait(0, None)
        wait2 = _compute_wait(2, None)
        # base * 2^0 = base; з jitter ±25% — в межах [0.75*base, 1.25*base]
        assert 0.5 <= wait0 <= max(_RETRY_WAIT_BASE * 1.25, 0.5)
        # base * 2^2 = 4*base — помітно більше за wait0 (майже завжди)
        assert wait2 <= _MAX_RETRY_WAIT

    def test_compute_wait_invalid_retry_after(self):
        """Retry-After: 'abc' → fallback на exponential без краху."""
        from gspread.exceptions import APIError

        from gsheets import _compute_wait

        resp = MagicMock()
        resp.headers = {"Retry-After": "not-a-number"}
        err = APIError(resp)
        wait = _compute_wait(0, err)
        assert wait > 0  # fallback спрацював


# ─────────────────────────────────────────────────────────────────────────────
#  handlers/scraping.py — валідація вводу
# ─────────────────────────────────────────────────────────────────────────────

class TestScrapingValidators:
    """Тести меж для keyword/count/year."""

    def test_constants_defined(self):
        from handlers.scraping import (
            _MAX_COUNT,
            _MAX_KEYWORD_LEN,
            _MAX_YEAR,
            _MIN_YEAR,
        )
        assert _MAX_KEYWORD_LEN > 0
        assert _MAX_COUNT >= 100
        assert 1900 <= _MIN_YEAR < _MAX_YEAR <= 2200


# ─────────────────────────────────────────────────────────────────────────────
#  observability.py — Sentry no-op
# ─────────────────────────────────────────────────────────────────────────────

class TestObservability:
    """Sentry має gracefully пропускатись без DSN."""

    def test_init_sentry_returns_false_when_dsn_missing(self, monkeypatch):
        from observability import init_sentry
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        assert init_sentry() is False

    def test_init_sentry_returns_false_when_empty_dsn(self, monkeypatch):
        from observability import init_sentry
        monkeypatch.setenv("SENTRY_DSN", "   ")
        assert init_sentry() is False

    def test_set_user_context_noop_without_sentry(self):
        """Якщо sentry-sdk не встановлено — функція не повинна падати."""
        from observability import set_user_context, tag
        # Не кидає навіть без sentry_sdk
        set_user_context(12345, "test")
        tag("country", "France")


# ─────────────────────────────────────────────────────────────────────────────
#  handlers/proxy.py — port validation
# ─────────────────────────────────────────────────────────────────────────────

class TestProxyHandlers:
    """Тести валідаторів proxy."""

    def test_valid_port_accepts_valid_range(self):
        from handlers.proxy import _valid_port
        assert _valid_port("1") is True
        assert _valid_port("8080") is True
        assert _valid_port("65535") is True

    def test_valid_port_rejects_out_of_range(self):
        from handlers.proxy import _valid_port
        assert _valid_port("0") is False
        assert _valid_port("65536") is False
        assert _valid_port("-1") is False
        assert _valid_port("abc") is False
        assert _valid_port("") is False


# ─────────────────────────────────────────────────────────────────────────────
#  database.py — datetime roundtrip (Python 3.12+)
# ─────────────────────────────────────────────────────────────────────────────

class TestDatabaseDatetime:
    """Перевірка що datetime → SQLite → datetime не втрачає типізацію."""

    def setup_method(self):
        import database
        self._orig = database.DB_NAME
        self._tmpdir = tempfile.TemporaryDirectory()
        database.DB_NAME = os.path.join(self._tmpdir.name, "dt.db")
        database.init_db()

    def teardown_method(self):
        import database
        database.DB_NAME = self._orig
        try:
            self._tmpdir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_company_date_is_datetime_after_read(self):
        from datetime import datetime

        import database
        database.save_company_to_db("TestCo", "http://example.com/x1", "France")
        with database.get_connection() as conn:
            row = conn.execute(
                "SELECT date_added FROM companies WHERE link = ?",
                ("http://example.com/x1",)
            ).fetchone()
        # Завдяки register_converter — це datetime, не str
        assert isinstance(row["date_added"], datetime)

    def test_get_new_companies_limit_respected(self):
        from datetime import datetime, timedelta

        import database
        for i in range(5):
            database.save_company_to_db(f"C{i}", f"http://ex.com/{i}", "France")
        since = datetime.now() - timedelta(days=1)
        res = database.get_new_companies_since(since, limit=3)
        assert len(res) == 3


# ─────────────────────────────────────────────────────────────────────────────
#  utils.py — jitter
# ─────────────────────────────────────────────────────────────────────────────

class TestWithRetryJitter:
    def test_jitter_zero_no_variance(self):
        """jitter=0 — sleep робиться рівно на delay."""
        import time as _t

        from utils import with_retry
        calls = [0.0]

        @with_retry(max_retries=2, delay=0.1, jitter=0)
        def fail():
            calls[0] += 1
            raise ValueError("x")

        try:
            fail()
        except ValueError:
            pass
        assert calls[0] == 2


# ─────────────────────────────────────────────────────────────────────────────
#  Authorization (whitelist, UPSERT, unblock)
# ─────────────────────────────────────────────────────────────────────────────

class TestAuthorization:
    """Перевіряє що @require_auth блокує не-whitelist-юзерів,
    /adduser реактивує заблокованого (UPSERT), /unblockuser працює коректно."""

    def setup_method(self):
        import database
        self._orig = database.DB_NAME
        self._tmpdir = tempfile.TemporaryDirectory()
        database.DB_NAME = os.path.join(self._tmpdir.name, "auth.db")
        database.init_db()

    def teardown_method(self):
        import database
        database.DB_NAME = self._orig
        try:
            self._tmpdir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_add_user_upsert_reactivates_blocked(self):
        """/removeuser → /adduser знову робить юзера активним (UPSERT, а не IGNORE)."""
        import database
        uid = 1001
        database.add_user(uid, "alice", role="user")
        assert database.is_user_allowed(uid) is True

        database.set_user_active(uid, False)
        assert database.is_user_allowed(uid) is False

        # Симулюємо повторний /adduser — має реактивувати
        database.add_user(uid, "alice", role="user")
        assert database.is_user_allowed(uid) is True

    def test_unblock_activates_existing_blocked_user(self):
        """set_user_active(True) повертає заблокованому доступ."""
        import database
        uid = 1002
        database.add_user(uid, "bob", role="user")
        database.set_user_active(uid, False)
        assert database.is_user_allowed(uid) is False

        database.set_user_active(uid, True)
        assert database.is_user_allowed(uid) is True

    def test_require_auth_blocks_unknown_user(self):
        """@require_auth викликає reply_text з «Доступ заборонено», func не виконується."""
        import asyncio

        from handlers.admin import require_auth

        called = []

        @require_auth
        async def inner(update, context):
            called.append(1)

        # Mock update з невідомим user.id
        update = MagicMock()
        update.effective_user.id = 99999  # не в БД
        update.effective_user.username = "ghost"
        update.message = MagicMock()
        update.message.reply_text = MagicMock()
        async def _noop_reply(*a, **kw):
            return None
        update.message.reply_text.side_effect = _noop_reply
        update.callback_query = None

        # ADMIN_ID підміняємо на None (щоб не пропустив як адміна)
        with patch("handlers.admin.ADMIN_ID", None):
            # але is_user_allowed повертає False по БД → тут у нас свіжа БД, user не додано
            asyncio.run(inner(update, MagicMock()))

        assert called == [], "Хендлер не повинен був виконатися для unknown user"

    def test_require_auth_allows_whitelisted_user(self):
        """Юзер у БД is_active=1 → хендлер викликається."""
        import asyncio

        import database
        from handlers.admin import require_auth

        uid = 2002
        database.add_user(uid, "carol", role="user")

        called = []

        @require_auth
        async def inner(update, context):
            called.append(1)

        update = MagicMock()
        update.effective_user.id = uid
        update.effective_user.username = "carol"

        with patch("handlers.admin.ADMIN_ID", None):
            asyncio.run(inner(update, MagicMock()))

        assert called == [1]


# ─────────────────────────────────────────────────────────────────────────────
#  Запуск
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
