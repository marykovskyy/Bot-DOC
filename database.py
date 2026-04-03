import sqlite3
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_NAME = "companies.db"
_CONNECT_TIMEOUT = 15.0


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Застосовує PRAGMA налаштування для максимальної надійності та швидкодії.

    WAL (Write-Ahead Log):
      - Дозволяє паралельне читання і запис без блокувань
      - При краші — транзакції не губляться (readers не блокуються writer-ом)
      - Зберігається в файлі БД, тому достатньо встановити один раз

    synchronous=NORMAL:
      - Безпечніше за OFF, швидше за FULL
      - Дані записуються на диск при кожному checkpoint (не при кожній транзакції)

    cache_size=-32000:
      - 32 MB кешу в пам'яті → менше disk I/O для часто читаних рядків

    temp_store=MEMORY:
      - Тимчасові таблиці і індекси — в RAM, не на диску

    foreign_keys=ON:
      - Дотримання цілісності зовнішніх ключів (SQLite вимикає їх за замовчуванням!)
    """
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")


@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_NAME, timeout=_CONNECT_TIMEOUT, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Створює всі таблиці та індекси при першому запуску.
    WAL-режим вмикається тут один раз — зберігається в файлі БД.
    """
    with get_connection() as conn:
        # ── WAL checkpoint: примусово переключаємо режим при ініціалізації ──
        # journal_mode=WAL повертає "wal" якщо успішно, або поточний режим
        mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        logger.info("SQLite journal_mode: %s", mode)

        # Компанії
        conn.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT,
                link        TEXT UNIQUE,
                country     TEXT,
                date_added  TIMESTAMP
            )
        ''')
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_name_nocase
            ON companies (name COLLATE NOCASE)
        ''')
        # Додатковий індекс для швидкого пошуку за датою (для дайджесту)
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_date_added
            ON companies (date_added)
        ''')

        # Користувачі (whitelist + ролі)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                chat_id     INTEGER PRIMARY KEY,
                username    TEXT,
                role        TEXT DEFAULT 'user',
                is_active   INTEGER DEFAULT 1,
                added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Історія пошуків
        conn.execute('''
            CREATE TABLE IF NOT EXISTS search_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER,
                site        TEXT,
                keyword     TEXT,
                count       INTEGER,
                year        TEXT,
                file_format TEXT,
                started_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Заплановані задачі
        conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER,
                site        TEXT,
                keyword     TEXT,
                count       INTEGER,
                year        TEXT,
                file_format TEXT,
                cron        TEXT,
                is_active   INTEGER DEFAULT 1,
                last_run    TIMESTAMP,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Лог аналізу документів
        conn.execute('''
            CREATE TABLE IF NOT EXISTS doc_analysis_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id       INTEGER,
                username      TEXT,
                session_name  TEXT,
                total_docs    INTEGER,
                valid_count   INTEGER,
                invalid_count INTEGER,
                unknown_count INTEGER,
                duration_sec  INTEGER,
                started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at   TIMESTAMP
            )
        ''')

        # Кеш результатів аналізу зображень (по MD5 хешу байтів)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS image_cache (
                img_hash  TEXT PRIMARY KEY,
                exp_date  TEXT,
                doc_type  TEXT,
                country   TEXT,
                source    TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')


# ─────────────────────────────────────────────
#  КОМПАНІЇ
# ─────────────────────────────────────────────

def is_company_scraped(link: str) -> bool:
    with get_connection() as conn:
        row = conn.execute("SELECT 1 FROM companies WHERE link = ?", (link,)).fetchone()
    return row is not None


def is_company_name_scraped(name: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM companies WHERE name = ? COLLATE NOCASE", (name,)
        ).fetchone()
    return row is not None


def save_company_to_db(name: str, link: str, country: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO companies (name, link, country, date_added) VALUES (?, ?, ?, ?)",
                (name, link, country, datetime.now())
            )
        return True
    except sqlite3.IntegrityError:
        return False
    except sqlite3.OperationalError as e:
        logger.warning("SQLite OperationalError: %s", e)
        return False


def get_global_stats() -> dict | None:
    try:
        with get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            by_country = conn.execute(
                "SELECT country, COUNT(*) FROM companies GROUP BY country"
            ).fetchall()
            today_str = datetime.now().strftime('%Y-%m-%d')
            today = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE date_added LIKE ?",
                (f"{today_str}%",)
            ).fetchone()[0]
        return {"total": total, "by_country": by_country, "today": today}
    except Exception as e:
        logger.error("Помилка отримання статистики: %s", e)
        return None


def get_new_companies_since(since: datetime) -> list[dict]:
    """Повертає компанії додані після вказаного часу (для дайджесту)."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT name, link, country FROM companies WHERE date_added >= ? ORDER BY date_added DESC",
            (since,)
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
#  КОРИСТУВАЧІ (whitelist + ролі)
# ─────────────────────────────────────────────

def get_user(chat_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE chat_id = ?", (chat_id,)
        ).fetchone()
    return dict(row) if row else None


def is_user_allowed(chat_id: int) -> bool:
    """Перевіряє чи є користувач у whitelist і активний."""
    user = get_user(chat_id)
    return user is not None and bool(user["is_active"])


def get_user_role(chat_id: int) -> str:
    """Повертає роль: 'admin', 'user', або 'unknown'."""
    user = get_user(chat_id)
    return user["role"] if user else "unknown"


def add_user(chat_id: int, username: str, role: str = "user") -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (chat_id, username, role) VALUES (?, ?, ?)",
                (chat_id, username or "", role)
            )
        return True
    except Exception as e:
        logger.error("Помилка додавання користувача: %s", e)
        return False


def set_user_active(chat_id: int, is_active: bool) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET is_active = ? WHERE chat_id = ?",
            (1 if is_active else 0, chat_id)
        )


def set_user_role(chat_id: int, role: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET role = ? WHERE chat_id = ?", (role, chat_id)
        )


def get_all_users() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM users ORDER BY added_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
#  ІСТОРІЯ ПОШУКІВ
# ─────────────────────────────────────────────

def save_search_history(chat_id: int, site: str, keyword: str,
                        count: int, year: str, file_format: str) -> int:
    """Зберігає запис про пошук, повертає його id."""
    with get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO search_history (chat_id, site, keyword, count, year, file_format) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, site, keyword, count, year, file_format)
        )
    return cur.lastrowid  # type: ignore[return-value]


def get_search_history(chat_id: int, limit: int = 10) -> list[dict]:
    """Повертає останні N пошуків користувача."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM search_history WHERE chat_id = ? "
            "ORDER BY started_at DESC LIMIT ?",
            (chat_id, limit)
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
#  ЗАПЛАНОВАНІ ЗАДАЧІ
# ─────────────────────────────────────────────

def save_scheduled_task(chat_id: int, site: str, keyword: str,
                        count: int, year: str, file_format: str, cron: str) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO scheduled_tasks (chat_id, site, keyword, count, year, file_format, cron) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, site, keyword, count, year, file_format, cron)
        )
    return cur.lastrowid  # type: ignore[return-value]


def get_scheduled_tasks(chat_id: int | None = None) -> list[dict]:
    """Повертає активні задачі. Якщо chat_id=None — всі задачі."""
    with get_connection() as conn:
        if chat_id:
            rows = conn.execute(
                "SELECT * FROM scheduled_tasks WHERE chat_id = ? AND is_active = 1 "
                "ORDER BY created_at DESC",
                (chat_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM scheduled_tasks WHERE is_active = 1"
            ).fetchall()
    return [dict(r) for r in rows]


def delete_scheduled_task(task_id: int, chat_id: int) -> bool:
    with get_connection() as conn:
        conn.execute(
            "UPDATE scheduled_tasks SET is_active = 0 WHERE id = ? AND chat_id = ?",
            (task_id, chat_id)
        )
    return True


def update_task_last_run(task_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE scheduled_tasks SET last_run = ? WHERE id = ?",
            (datetime.now(), task_id)
        )


# ─────────────────────────────────────────────
#  ЛОГ АНАЛІЗУ ДОКУМЕНТІВ
# ─────────────────────────────────────────────

def log_doc_analysis(
    chat_id: int, username: str, session_name: str,
    total: int, valid: int, invalid: int, unknown: int,
    duration_sec: int, started_at: datetime, finished_at: datetime
) -> None:
    """Записує лог завершеного аналізу документів."""
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO doc_analysis_log "
                "(chat_id, username, session_name, total_docs, valid_count, "
                "invalid_count, unknown_count, duration_sec, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (chat_id, username, session_name, total, valid, invalid, unknown,
                 duration_sec, started_at, finished_at)
            )
    except Exception as e:
        logger.error("Помилка запису лога аналізу: %s", e)


def get_doc_analysis_logs(limit: int = 20) -> list[dict]:
    """Повертає останні N записів логу аналізу (для адміна)."""
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM doc_analysis_log ORDER BY started_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Помилка читання логу аналізу: %s", e)
        return []


# ─────────────────────────────────────────────
#  КЕШ РЕЗУЛЬТАТІВ АНАЛІЗУ ЗОБРАЖЕНЬ
# ─────────────────────────────────────────────

def get_cache_entry(img_hash: str) -> dict | None:
    """Повертає кешований результат аналізу зображення або None."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM image_cache WHERE img_hash = ?", (img_hash,)
            ).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error("Помилка читання кешу: %s", e)
        return None


def save_cache_entry(
    img_hash: str,
    exp_date: str | None,
    doc_type: str | None,
    country: str | None,
    source: str
) -> None:
    """Зберігає результат аналізу зображення в кеш."""
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO image_cache "
                "(img_hash, exp_date, doc_type, country, source) VALUES (?, ?, ?, ?, ?)",
                (img_hash, exp_date, doc_type, country, source)
            )
    except Exception as e:
        logger.error("Помилка збереження в кеш: %s", e)