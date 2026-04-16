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
                raise IOError("tmp")
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
            MAX_PARALLEL_TASKS, STATUS_UPDATE_SEC,
            CAPTCHA_MAX_WAIT_SEC, CAPTCHA_POLL_INTERVAL_SEC,
            ELEMENT_WAIT_RETRIES, BROWSER_LAUNCH_TIMEOUT_SEC,
            SHEETS_MAX_RETRIES, SHEETS_RETRY_WAIT_BASE, SHEETS_WRITE_DELAY,
            AI_MAX_CONCURRENT_API, AI_PROGRESS_UPDATE_SEC,
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
        import proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path
        return tmp_path

    def test_save_and_load(self):
        import proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path

        proxies = {"France": [{"host": "1.1.1.1", "port": "8080", "user": "u", "pass": "p"}],
                   "Finland": [], "General": []}
        proxy_manager.save(True, proxies)

        data = proxy_manager.load()
        assert data["use_proxy"] is True
        assert data["proxies"]["France"][0]["host"] == "1.1.1.1"

    def test_load_returns_defaults_on_missing_file(self):
        import proxy_manager
        tmp_path = Path(self._tmp.name) / "no_such.json"
        proxy_manager._PROXY_FILE = tmp_path

        # Без proxy_settings.py для міграції — перехоплюємо ImportError
        with patch("proxy_manager._migrate_from_py", side_effect=Exception("no py")):
            # _migrate_from_py кине — load поверне defaults
            with patch.object(tmp_path, "exists", return_value=False):
                data = proxy_manager.load()
        # Файл або дефолт — use_proxy має бути bool
        assert isinstance(data.get("use_proxy", False), bool)

    def test_get_use_proxy_helper(self):
        import proxy_manager
        tmp_path = Path(self._tmp.name) / "proxy_settings.json"
        proxy_manager._PROXY_FILE = tmp_path
        proxy_manager.save(False, {"France": [], "Finland": [], "General": []})

        assert proxy_manager.get_use_proxy() is False

        proxy_manager.save(True, {"France": [], "Finland": [], "General": []})
        assert proxy_manager.get_use_proxy() is True

    def test_atomic_write(self):
        """Перевіряє що tmp-файл не залишається після save."""
        import proxy_manager
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
        """Кожен тест отримує свіжу in-memory БД."""
        import database
        self._orig_db = database.DB_NAME
        # Використовуємо тимчасовий файл щоб уникнути конфліктів
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        database.DB_NAME = self._tmp.name
        database.init_db()

    def teardown_method(self):
        import database
        database.DB_NAME = self._orig_db
        os.unlink(self._tmp.name)

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

class TestDocumentGenerator:
    """Тести генератора документів (без реального PNG — мокуємо PIL)."""

    def _make_template_dir(self, tmp_path: Path) -> Path:
        """Створює мінімальний валідний шаблон у tmp_path."""
        tpl_dir = tmp_path / "test_tpl"
        tpl_dir.mkdir()

        # Мінімальний 10×10 RGBA PNG у пам'яті
        from PIL import Image
        img = Image.new("RGBA", (200, 100), (255, 255, 255, 255))
        img.save(str(tpl_dir / "background.png"))

        config = {
            "name": "test_tpl",
            "description": "Тестовий шаблон",
            "fields": {
                "name": {
                    "label": "Ім'я",
                    "default": "Іван",
                    "x": 10, "y": 10,
                    "font_size": 14, "bold": False,
                    "color": [0, 0, 0], "align": "left"
                },
                "date": {
                    "label": "Дата",
                    "default": "01.01.2026",
                    "x": 150, "y": 10,
                    "font_size": 12, "bold": False,
                    "color": [50, 50, 50], "align": "right"
                }
            }
        }
        (tpl_dir / "config.json").write_text(
            json.dumps(config, ensure_ascii=False), encoding="utf-8"
        )
        return tpl_dir

    def test_load_template(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        gen = DocumentGenerator(tpl_dir)
        assert gen.config["name"] == "test_tpl"
        assert "name" in gen.config["fields"]

    def test_render_returns_bytes(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        gen = DocumentGenerator(tpl_dir)
        result = gen.render({"name": "Тест", "date": "06.04.2026"})
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_render_png_magic_bytes(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        gen = DocumentGenerator(tpl_dir)
        result = gen.render({"name": "Test"})
        # PNG завжди починається з \x89PNG
        assert result[:4] == b"\x89PNG"

    def test_preview_uses_field_names_as_placeholders(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        gen = DocumentGenerator(tpl_dir)
        # preview() повинна не кидати виключень
        result = gen.preview()
        assert isinstance(result, bytes)

    def test_font_cache_populated_after_render(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        gen = DocumentGenerator(tpl_dir)
        assert len(gen._font_cache) == 0
        gen.render({"name": "Test", "date": "2026"})
        assert len(gen._font_cache) > 0

    def test_missing_background_raises(self, tmp_path):
        from documents.generator import DocumentGenerator
        tpl_dir = tmp_path / "no_bg"
        tpl_dir.mkdir()
        (tpl_dir / "config.json").write_text('{"fields":{}}')
        with pytest.raises(FileNotFoundError, match="background.png"):
            DocumentGenerator(tpl_dir)

    def test_rgba_color_no_5tuple(self, tmp_path):
        """Поле з color: [R, G, B, A] не повинно падати з 5-tuple."""
        from documents.generator import DocumentGenerator
        tpl_dir = self._make_template_dir(tmp_path)
        cfg = json.loads((tpl_dir / "config.json").read_text())
        cfg["fields"]["name"]["color"] = [0, 0, 0, 255]   # 4 значення
        (tpl_dir / "config.json").write_text(json.dumps(cfg))
        gen = DocumentGenerator(tpl_dir)
        result = gen.render({"name": "Test"})   # не повинно кидати
        assert isinstance(result, bytes)


# ─────────────────────────────────────────────────────────────────────────────
#  Запуск
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
