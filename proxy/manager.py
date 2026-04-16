"""
proxy_manager.py — Централізоване сховище налаштувань проксі (JSON).

Замінює зберігання у proxy_settings.py (Python-файл через importlib.reload).

Файл зберігання: proxy_settings.json
Формат:
  {
    "use_proxy": false,
    "proxies": {
      "France":  [{"protocol":"http","host":"...","port":"...","user":"...","pass":"..."}],
      "Finland": [],
      "General": []
    }
  }

При першому запуску автоматично мігрує дані з proxy_settings.py (якщо існує).
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_PROXY_FILE = Path(__file__).parent.parent / "proxy_settings.json"
_lock = threading.Lock()          # захист від одночасного читання/запису

_DEFAULT: dict = {
    "use_proxy": False,
    "proxies": {"France": [], "Finland": [], "General": []},
}


# ── Публічний API ────────────────────────────────────────────────────────────

def load() -> dict:
    """
    Зчитує налаштування з JSON.
    Повертає dict: {"use_proxy": bool, "proxies": {country: [...]}}
    При відсутності файлу — виконує одноразову міграцію з proxy_settings.py.
    """
    with _lock:
        if not _PROXY_FILE.exists():
            _migrate_from_py()
        try:
            return json.loads(_PROXY_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Не вдалось прочитати %s: %s. Повертаю defaults.", _PROXY_FILE, e)
            return _DEFAULT.copy()


def save(use_proxy: bool, proxies: dict) -> None:
    """
    Зберігає налаштування у JSON.
    Запис атомарний: спочатку в .tmp, потім rename → не буває половинного запису.
    """
    data = {"use_proxy": use_proxy, "proxies": proxies}
    tmp = _PROXY_FILE.with_suffix(".json.tmp")
    with _lock:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_PROXY_FILE)


def get_use_proxy() -> bool:
    """Швидкий хелпер: чи увімкнені проксі."""
    return bool(load().get("use_proxy", False))


def get_proxies() -> dict:
    """Швидкий хелпер: словник проксі по країнах."""
    return load().get("proxies", _DEFAULT["proxies"].copy())


# ── Міграція ─────────────────────────────────────────────────────────────────

def _migrate_from_py() -> None:
    """
    Одноразова міграція даних із proxy_settings.py → proxy_settings.json.
    Викликається автоматично при першому завантаженні якщо JSON відсутній.
    """
    try:
        import proxy.settings as _ps  # noqa: PLC0415
        data = {
            "use_proxy": bool(getattr(_ps, "USE_PROXY", False)),
            "proxies":   dict(getattr(_ps, "PROXIES", _DEFAULT["proxies"])),
        }
        _PROXY_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("proxy_settings.py → proxy_settings.json: міграція успішна.")
    except Exception as e:
        logger.warning("Міграція proxy_settings.py не вдалась (%s). Використовую defaults.", e)
        _PROXY_FILE.write_text(
            json.dumps(_DEFAULT, ensure_ascii=False, indent=2), encoding="utf-8"
        )
