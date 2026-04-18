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
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

_PROXY_FILE = Path(__file__).parent.parent / "proxy_settings.json"
_lock = threading.RLock()         # RLock — дозволяє update() тримати лок і викликати load()

_DEFAULT: dict = {
    "use_proxy": False,
    "proxies": {"France": [], "Finland": [], "General": []},
}


def _validate_schema(data: dict) -> dict:
    """Мінімальна валідація: гарантує наявність ключів і правильні типи.

    При будь-якому незбігу повертає defaults — краще порожні проксі,
    ніж крах скрапера при `random.choice(string)`.
    """
    if not isinstance(data, dict):
        return _DEFAULT.copy()
    use_proxy = bool(data.get("use_proxy", False))
    raw_proxies = data.get("proxies")
    if not isinstance(raw_proxies, dict):
        return {"use_proxy": use_proxy, "proxies": _DEFAULT["proxies"].copy()}
    clean_proxies: dict = {}
    for country, items in raw_proxies.items():
        if isinstance(items, list):
            clean_proxies[country] = [i for i in items if isinstance(i, dict)]
        else:
            logger.warning("proxy_settings.json: %s має невірний тип, замінюю на []", country)
            clean_proxies[country] = []
    return {"use_proxy": use_proxy, "proxies": clean_proxies}


def _atomic_write(data: dict) -> None:
    """Атомарний запис JSON із fsync для durability.

    Послідовність: write tmp → flush → fsync → rename → cleanup старого .tmp.
    """
    tmp = _PROXY_FILE.with_suffix(".json.tmp")
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()
        try:
            os.fsync(f.fileno())
        except (AttributeError, OSError):
            # fsync може не підтримуватись на деяких FS (SMB, мережеві)
            pass
    os.replace(tmp, _PROXY_FILE)


# ── Публічний API ────────────────────────────────────────────────────────────

def load() -> dict:
    """
    Зчитує налаштування з JSON.
    Повертає dict: {"use_proxy": bool, "proxies": {country: [...]}}
    При відсутності файлу — виконує одноразову міграцію з proxy_settings.py.
    При пошкодженому JSON — повертає defaults (не крашимо бот).
    """
    with _lock:
        try:
            if not _PROXY_FILE.exists():
                _migrate_from_py()
            raw = json.loads(_PROXY_FILE.read_text(encoding="utf-8"))
            return _validate_schema(raw)
        except Exception as e:
            # Включає випадки: міграція впала, файл пошкоджений, JSON невалідний.
            # Завжди повертаємо defaults — краще порожні проксі, ніж краш бота.
            logger.warning("Не вдалось прочитати %s: %s. Повертаю defaults.", _PROXY_FILE, e)
            return {
                "use_proxy": False,
                "proxies": {k: list(v) for k, v in _DEFAULT["proxies"].items()},
            }


def save(use_proxy: bool, proxies: dict) -> None:
    """
    Зберігає налаштування у JSON.
    Запис атомарний: write→flush→fsync→rename — немає половинного запису.
    """
    data = {"use_proxy": use_proxy, "proxies": proxies}
    with _lock:
        _atomic_write(data)


@contextmanager
def update():
    """Атомарний read-modify-write під одним локом.

    Використання::

        with update() as data:
            data["proxies"]["France"].append(new_proxy)

    Виправляє lost-update, коли паралельні виклики load()/save() з різних
    потоків перезаписували зміни одне одного (audit finding P4).
    """
    with _lock:
        # Читаємо поточний стан (не можемо використати load() з лока — він теж бере лок).
        # RLock дозволяє re-entry, тому load() працює.
        data = load()
        yield data
        _atomic_write(data)


def mutate(mutator: Callable[[dict], None]) -> dict:
    """Функціональна версія update() — для одноразових операцій.

    Повертає новий стан після мутації.
    """
    with update() as data:
        mutator(data)
    return load()


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
