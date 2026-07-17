from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import tempfile
import threading
import time
from typing import Any

import pandas as pd
from DrissionPage import ChromiumOptions, ChromiumPage  # type: ignore[import]

import database
import gsheets
import proxy.manager as proxy_manager
from config import SCRAPER_CONFIG
from constants import (
    CAPTCHA_MAX_WAIT_SEC,
    CAPTCHA_POLL_INTERVAL_SEC,
    SHEETS_WRITE_DELAY,
)
from scrapers import turkey as turkey_scraper
from scrapers.california import scrape_california
from scrapers.czech import scrape_czech
from scrapers.denmark import scrape_denmark
from scrapers.finland import scrape_finland_api  # ← API-скрапер (PRH open data)
from scrapers.france import scrape_france_api  # ← API-скрапер (pappers.ai JSON)
from scrapers.india import scrape_india_api  # ← API-скрапер (data.gov.in / MCA)
from scrapers.latvia import scrape_latvia
from scrapers.new_zealand import scrape_new_zealand
from scrapers.norway import scrape_norway_api  # ← API-скрапер (Brreg open data)
from scrapers.thailand import scrape_thailand
from scrapers.uk_api import scrape_uk_api
from scrapers.washington import scrape_washington

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  ЧЕРГА ЗАПИСУ В GOOGLE SHEETS
#  Один фоновий потік пише послідовно з паузою —
#  уникає 429 і гарантує що всі записи дійдуть
# ─────────────────────────────────────────────
import queue

_sheets_queue: queue.Queue = queue.Queue()
_SHEETS_WRITE_DELAY = SHEETS_WRITE_DELAY  # секунд між записами (ліміт Sheets ~60 req/min)

# Worker-thread створюється лінь (lazy) — при першому виклику _enqueue_sheet_write().
# Уникаємо side-effect при import (тести, import-only сценарії, reload).
_sheets_thread: threading.Thread | None = None
_sheets_thread_lock = threading.Lock()


def _sheets_worker() -> None:
    """Фоновий потік: бере завдання з черги і пише в Sheets по одному."""
    while True:
        task = _sheets_queue.get()
        if task is None:  # сигнал зупинки
            _sheets_queue.task_done()
            break
        name, link, site_key = task
        try:
            gsheets.append_to_sheet(name, link, site_key)
        except Exception as e:
            logger.error("Sheets worker помилка: %s", e)
        finally:
            _sheets_queue.task_done()
            time.sleep(_SHEETS_WRITE_DELAY)


def _ensure_sheets_worker() -> None:
    """Lazy-старт sheets воркера. Thread-safe через DCL-pattern під локом."""
    global _sheets_thread
    if _sheets_thread is not None and _sheets_thread.is_alive():
        return
    with _sheets_thread_lock:
        if _sheets_thread is None or not _sheets_thread.is_alive():
            _sheets_thread = threading.Thread(
                target=_sheets_worker, daemon=True, name="sheets-worker"
            )
            _sheets_thread.start()


def _enqueue_sheet_write(name: str, link: str, site_key: str) -> None:
    """Ставить запис у чергу. Повертається одразу, не блокує скрапер."""
    _ensure_sheets_worker()
    _sheets_queue.put((name, link, site_key))


def flush_sheets_queue() -> None:
    """Чекає поки всі записи в чергу будуть оброблені."""
    _sheets_queue.join()


# --- Таймаути (константи замість магічних чисел) ---
# Таймаути — визначені в constants.py, тут лише підтвердження що імпортовані
# CAPTCHA_MAX_WAIT_SEC, CAPTCHA_POLL_INTERVAL_SEC, ELEMENT_WAIT_RETRIES, BROWSER_LAUNCH_TIMEOUT_SEC



# ─────────────────────────────────────────────
#  LOCAL PROXY RELAY
#  Chrome → localhost:PORT (без пароля)
#  → реальний проксі (з Basic Auth)
#
#  Чому цей підхід:
#  - Chrome 130+ вимкнув Manifest V2 extensions
#  - Chrome не підтримує user:pass@ в --proxy-server
#  - Relay вирішує обидві проблеми: Chrome бачить
#    localhost без авторизації, relay сам додає Auth
# ─────────────────────────────────────────────

class LocalProxyRelay:
    """
    Asyncio HTTP/HTTPS proxy relay з Basic Auth до upstream.

    Chrome → 127.0.0.1:LOCAL_PORT (без пароля)
           → upstream:port (з Proxy-Authorization header)

    Ключові виправлення v3:
    - _pipe НЕ закриває writer — тільки _handle закриває у finally
    - asyncio.wait(FIRST_COMPLETED) замість gather — коли один напрямок
      закривається, другий скасовується через cancel(), а не обривається
    - Правильне читання CONNECT-відповіді від upstream (loop до \r\n\r\n)
    - _start_event синхронізує старт сервера перед поверненням start()
    """

    def __init__(self, upstream_host: str, upstream_port: int,
                 username: str, password: str,
                 upstream_protocol: str = "http"):
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.upstream_protocol = (upstream_protocol or "http").lower()
        self.username = username or ""
        self.password = password or ""
        import base64 as _b64
        creds = _b64.b64encode(f"{username}:{password}".encode()).decode()
        self._auth_header = (
            b"Proxy-Authorization: Basic " + creds.encode() + b"\r\n"
        )
        self.local_port = self._free_port()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server = None
        self._thread: threading.Thread | None = None
        self._stopped = False

    @staticmethod
    def _free_port() -> int:
        import socket as _s
        with _s.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _inject_auth(self, data: bytes) -> bytes:
        """Вставляє Proxy-Authorization після першого \r\n\r\n якщо ще немає."""
        if b"Proxy-Authorization" not in data:
            return data.replace(
                b"\r\n\r\n",
                b"\r\n" + self._auth_header + b"\r\n",
                1
            )
        return data

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter) -> None:
        """
        Одностороннє пересилання reader → writer.
        НЕ закриває writer — це робить _handle у finally.
        """
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    return
                writer.write(data)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError,
                asyncio.CancelledError, OSError):
            pass
        except Exception:
            pass

    @staticmethod
    async def _read_until_blank_line(reader: asyncio.StreamReader,
                                     timeout: float = 15.0) -> bytes:
        """Читає HTTP заголовки до \r\n\r\n з таймаутом."""
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout)
            if not chunk:
                break
            buf += chunk
        return buf

    async def _relay_bidirectional(
        self,
        r1: asyncio.StreamReader, w1: asyncio.StreamWriter,
        r2: asyncio.StreamReader, w2: asyncio.StreamWriter,
    ) -> None:
        """
        Двосторонній relay: r1→w2 і r2→w1 одночасно.
        Коли один напрямок закривається — другий скасовується.
        Самі writer'и НЕ закриваються тут.
        """
        t1 = asyncio.create_task(self._pipe(r1, w2))
        t2 = asyncio.create_task(self._pipe(r2, w1))
        try:
            done, pending = await asyncio.wait(
                [t1, t2], return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        except Exception:
            t1.cancel()
            t2.cancel()

    async def _socks5_connect(self, upstream_r: asyncio.StreamReader,
                              upstream_w: asyncio.StreamWriter,
                              dest_host: str, dest_port: int) -> bool:
        """SOCKS5 handshake → CONNECT до dest_host:dest_port (RFC 1928 + RFC 1929).

        Повертає True якщо upstream підтвердив тунель.
        """
        # 1. Greeting: VER=5, NMETHODS=2, methods=[0=NoAuth, 2=User/Pass]
        upstream_w.write(b"\x05\x02\x00\x02")
        await upstream_w.drain()
        resp = await asyncio.wait_for(upstream_r.readexactly(2), timeout=10.0)
        if resp[0] != 0x05:
            logger.warning("SOCKS5: upstream не SOCKS5 (ver=%d)", resp[0])
            return False
        method = resp[1]
        if method == 0xFF:
            logger.warning("SOCKS5: upstream відхилив методи аутентифікації")
            return False

        # 2. Якщо потрібна user/pass — відправляємо
        if method == 0x02:
            user = self.username.encode("utf-8")
            pwd = self.password.encode("utf-8")
            auth_req = bytes([0x01, len(user)]) + user + bytes([len(pwd)]) + pwd
            upstream_w.write(auth_req)
            await upstream_w.drain()
            auth_resp = await asyncio.wait_for(upstream_r.readexactly(2), timeout=10.0)
            if auth_resp[1] != 0x00:
                logger.warning("SOCKS5: auth failed (status=%d)", auth_resp[1])
                return False
        elif method != 0x00:
            logger.warning("SOCKS5: непідтриманий method %d", method)
            return False

        # 3. CONNECT request: VER=5, CMD=1, RSV=0, ATYP=3 (domain), domain, port
        host_b = dest_host.encode("idna") if dest_host else b""
        if len(host_b) > 255:
            logger.warning("SOCKS5: domain занадто довгий")
            return False
        req = (
            b"\x05\x01\x00\x03"
            + bytes([len(host_b)]) + host_b
            + dest_port.to_bytes(2, "big")
        )
        upstream_w.write(req)
        await upstream_w.drain()

        # 4. Response: VER, REP, RSV, ATYP, BND.ADDR, BND.PORT
        head = await asyncio.wait_for(upstream_r.readexactly(4), timeout=10.0)
        if head[1] != 0x00:
            REP_MSGS = {
                1: "general failure", 2: "not allowed by ruleset",
                3: "network unreachable", 4: "host unreachable",
                5: "connection refused", 6: "TTL expired",
                7: "command not supported", 8: "address type not supported",
            }
            logger.warning("SOCKS5: CONNECT відмовлено: %s",
                           REP_MSGS.get(head[1], f"code={head[1]}"))
            return False
        # Дочитуємо BND.ADDR + BND.PORT (різна довжина за ATYP)
        atyp = head[3]
        if atyp == 0x01:    # IPv4
            await upstream_r.readexactly(4 + 2)
        elif atyp == 0x03:  # domain
            ln = (await upstream_r.readexactly(1))[0]
            await upstream_r.readexactly(ln + 2)
        elif atyp == 0x04:  # IPv6
            await upstream_r.readexactly(16 + 2)
        else:
            logger.warning("SOCKS5: невідомий ATYP=%d", atyp)
            return False
        return True

    async def _handle(self,
                      client_r: asyncio.StreamReader,
                      client_w: asyncio.StreamWriter) -> None:
        upstream_r: asyncio.StreamReader | None = None
        upstream_w: asyncio.StreamWriter | None = None
        try:
            # 1. Читаємо заголовки від Chrome
            head = await self._read_until_blank_line(client_r, timeout=30.0)
            if not head:
                return

            method = head.split(b" ")[0].upper()

            # 2. Підключаємось до upstream проксі
            upstream_r, upstream_w = await asyncio.wait_for(
                asyncio.open_connection(self.upstream_host, self.upstream_port),
                timeout=15.0
            )

            # ── SOCKS5 шлях ──────────────────────────────────────────────
            if self.upstream_protocol in ("socks5", "socks5h"):
                if method != b"CONNECT":
                    # Chrome для HTTP без TLS теж шле GET напряму, не CONNECT.
                    # Парсимо Host: header щоб витягнути dest:port.
                    host_line = b""
                    for line in head.split(b"\r\n"):
                        if line.lower().startswith(b"host:"):
                            host_line = line[5:].strip()
                            break
                    if not host_line:
                        return
                    if b":" in host_line:
                        h, _, p = host_line.partition(b":")
                        dest_host, dest_port = h.decode(), int(p)
                    else:
                        dest_host, dest_port = host_line.decode(), 80
                    if not await self._socks5_connect(upstream_r, upstream_w,
                                                     dest_host, dest_port):
                        return
                    # Пересилаємо оригінальний HTTP-запит (без auth header — SOCKS вже автентифікував)
                    upstream_w.write(head)
                    await upstream_w.drain()
                else:
                    # CONNECT host:port HTTP/1.1
                    target = head.split(b" ")[1].decode()
                    dest_host, _, dest_port_s = target.partition(":")
                    dest_port = int(dest_port_s) if dest_port_s else 443
                    if not await self._socks5_connect(upstream_r, upstream_w,
                                                     dest_host, dest_port):
                        # Сигналізуємо клієнту що тунель не вдалось підняти
                        try:
                            client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                            await client_w.drain()
                        except Exception:
                            pass
                        return
                    # Тунель OK — повідомляємо Chrome
                    client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    await client_w.drain()

                # Двосторонній relay
                await self._relay_bidirectional(client_r, client_w,
                                                upstream_r, upstream_w)
                return

            # ── HTTP/HTTPS upstream шлях (як було) ──────────────────────
            # 3. Пересилаємо запит з авторизацією
            upstream_w.write(self._inject_auth(head))
            await upstream_w.drain()

            if method == b"CONNECT":
                # HTTPS тунель: читаємо відповідь "200 Connection established"
                resp = await self._read_until_blank_line(upstream_r, timeout=15.0)

                # Пересилаємо відповідь Chrome
                client_w.write(resp)
                await client_w.drain()

                # Якщо upstream відмовив — виходимо
                status_line = resp.split(b"\r\n")[0]
                if b"200" not in status_line:
                    logger.warning("Upstream proxy відмовив CONNECT: %s",
                                   status_line.decode(errors="replace"))
                    return

            # 4. Двосторонній relay (HTTP і HTTPS)
            await self._relay_bidirectional(client_r, client_w,
                                            upstream_r, upstream_w)

        except TimeoutError:
            logger.warning("Proxy relay timeout (upstream=%s:%d)",
                           self.upstream_host, self.upstream_port)
        except (ConnectionResetError, BrokenPipeError):
            # Клієнт/upstream різко закрив сокет — звичайний випадок, DEBUG
            logger.debug("Proxy relay: connection reset")
        except Exception as exc:
            logger.warning("Proxy relay error (upstream=%s:%d): %s",
                           self.upstream_host, self.upstream_port, exc)
        finally:
            for w in (upstream_w, client_w):
                if w is not None:
                    try:
                        w.close()
                        await asyncio.wait_for(w.wait_closed(), timeout=2.0)
                    except Exception:
                        pass

    def _run_loop(self, started_event: threading.Event) -> None:
        """Asyncio event loop у фоновому потоці."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def _boot():
            self._server = await asyncio.start_server(
                self._handle, "127.0.0.1", self.local_port
            )
            started_event.set()   # сигналізуємо що сервер готовий
            try:
                await self._server.serve_forever()
            except asyncio.CancelledError:
                pass

        try:
            self._loop.run_until_complete(_boot())
        except (RuntimeError, asyncio.CancelledError):
            # loop.stop() перериває run_until_complete — це очікувано
            pass
        finally:
            # Закриваємо loop щоб звільнити IOCP/epoll handle.
            try:
                self._loop.close()
            except Exception:
                pass

    def start(self) -> int:
        """Запускає relay і чекає поки він буде готовий. Повертає порт."""
        ready = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, args=(ready,), daemon=True)
        self._thread.start()
        # Чекаємо реального старту сервера (не просто sleep)
        if not ready.wait(timeout=5.0):
            raise RuntimeError("LocalProxyRelay не запустився за 5 секунд")
        logger.info("LocalProxyRelay запущено: 127.0.0.1:%d → %s:%d",
                    self.local_port, self.upstream_host, self.upstream_port)
        return self.local_port

    def stop(self) -> None:
        """Зупиняє relay із повним cleanup.

        Послідовність (audit finding P3):
         1. server.close() + wait_closed() — закриває listening socket
         2. Скасовує всі активні _handle-tasks — звільняє відкриті upstream з'єднання
         3. loop.stop() — перериває serve_forever
         4. thread.join() — чекаємо поки потік реально завершиться
         5. loop.close() — виконується у finally _run_loop (звільняє epoll/IOCP)
        """
        if self._stopped:
            return
        self._stopped = True

        if not self._loop or not self._loop.is_running():
            return

        async def _shutdown():
            # 1. Закриваємо listening socket
            if self._server is not None:
                try:
                    self._server.close()
                    await self._server.wait_closed()
                except Exception as e:
                    logger.debug("relay.stop: server.close error: %s", e)
            # 2. Скасовуємо всі активні tasks окрім поточного
            current = asyncio.current_task()
            for task in asyncio.all_tasks(self._loop):
                if task is not current and not task.done():
                    task.cancel()

        try:
            # Даємо shutdown коротко виконатися, потім зупиняємо loop
            fut = asyncio.run_coroutine_threadsafe(_shutdown(), self._loop)
            try:
                fut.result(timeout=3.0)
            except Exception as e:
                logger.debug("relay.stop: shutdown coro: %s", e)
        finally:
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass

        # 4. Чекаємо завершення потоку (коротко)
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            if self._thread.is_alive():
                logger.warning("LocalProxyRelay thread не завершився за 3с (port=%d)",
                               self.local_port)


def get_page(chat_id: int, status_dict: dict, site_key: str = "General") -> tuple[ChromiumPage | None, LocalProxyRelay | None]:
    """
    Запускає Chrome з проксі через LocalProxyRelay.

    Схема: Chrome → localhost:PORT (без auth) → LocalProxyRelay → реальний проксі (з Basic Auth)

    Чому relay а не extension:
      - Chrome 130+ вимкнув Manifest V2 extensions за замовчуванням
      - Chrome не підтримує user:pass@ в --proxy-server (ERR_NO_SUPPORTED_PROXIES)
      - Relay стартує як Python thread ПЕРЕД Chrome — авторизація 100% надійна
    """
    options = ChromiumOptions()
    options.set_argument("--disable-infobars")
    options.set_argument("--no-first-run")
    options.set_argument("--no-default-browser-check")

    # Захист від WebRTC leak (без --disable-webrtc щоб не ламати сайти)
    options.set_pref("webrtc.ip_handling_policy", "disable_non_proxied_udp")
    options.set_pref("webrtc.multiple_routes_enabled", False)
    options.set_pref("webrtc.nonproxied_udp_enabled", False)

    _proxy_data = proxy_manager.load()
    use_proxy   = _proxy_data.get("use_proxy", False)
    proxies_dict = _proxy_data.get("proxies", {})

    available_proxies: list[dict] = []
    if use_proxy:
        if isinstance(proxies_dict, dict):
            available_proxies = proxies_dict.get(site_key, []) or proxies_dict.get("General", [])
        elif isinstance(proxies_dict, list):
            available_proxies = proxies_dict

    relay: LocalProxyRelay | None = None

    if use_proxy and available_proxies:
        p = random.choice(available_proxies)
        host = p["host"]
        port = int(p["port"])
        user = p.get("user", "")
        password = p.get("pass", "")
        protocol = (p.get("protocol") or "http").lower()

        # Запускаємо relay ДО старту Chrome
        relay = LocalProxyRelay(host, port, user, password,
                                upstream_protocol=protocol)
        local_port = relay.start()

        # Chrome підключається до localhost без пароля
        options.set_proxy(f"http://127.0.0.1:{local_port}")

        logger.info("[Proxy %s] Relay 127.0.0.1:%d → %s://%s:%d (user: %s)",
                    site_key, local_port, protocol, host, port, user or "—")
    else:
        logger.info("Проксі для %s відсутні або вимкнені.", site_key)

    # ── Buster captcha extension ────────────────────────────────────────────
    buster_path = os.path.abspath('buster_ext')
    if os.path.exists(buster_path):
        options.add_extension(buster_path)

    try:
        return ChromiumPage(addr_or_opts=options), relay
    except Exception as e:
        logger.error("Помилка запуску браузера: %s", e)
        status_dict['is_running'] = False
        if relay: relay.stop()
        return None, None


def check_and_wait_for_captcha(page: ChromiumPage) -> None:
    wait_time = 0
    while True:
        try:
            html = str(getattr(page, 'html', '') or '').lower()

            is_captcha = (
                "oups..." in html
                or "votre réseau semble beaucoup utiliser" in html
                or (
                    ("just a moment..." in html or "cloudflare" in html)
                    and (page.eles('css:iframe[src*="cloudflare"]') or page.eles('css:iframe[src*="challenge"]'))
                )
            )

            if not is_captcha:
                break

            if wait_time == 0:
                logger.warning("CAPTCHA виявлено. Спроба автопроходження...")

            try:
                recaptcha_frame = page.get_frame('@src^https://www.google.com/recaptcha/api2/anchor')
                if recaptcha_frame:
                    checkboxes = recaptcha_frame.eles('.recaptcha-checkbox-border', timeout=1)
                    if checkboxes:
                        checkboxes[0].click()  # type: ignore[index,union-attr]
                        time.sleep(2)

                bframe = page.get_frame('@src^https://www.google.com/recaptcha/api2/bframe')
                if bframe:
                    buster_btns = bframe.eles('#solver-button', timeout=1)
                    if buster_btns:
                        buster_btns[0].click()  # type: ignore[index,union-attr]
                        time.sleep(5)

                for btn in page.eles('tag:button'):  # type: ignore[union-attr]
                    if "valider" in str(btn.text).lower() or btn.attr('type') == 'submit':
                        btn.click()
                        time.sleep(3)
                        break
            except Exception:
                pass

            if wait_time >= CAPTCHA_MAX_WAIT_SEC:
                raise Exception("NEED_PROXY_CHANGE")

            time.sleep(CAPTCHA_POLL_INTERVAL_SEC)
            wait_time += CAPTCHA_POLL_INTERVAL_SEC

        except Exception as e:
            if "NEED_PROXY_CHANGE" in str(e):
                raise
            time.sleep(2)


# ─────────────────────────────────────────────
#  ДОПОМІЖНІ ФУНКЦІЇ (усунення дублювання)
# ─────────────────────────────────────────────

def _get_link_key(item: dict) -> str:
    """Визначає правильний ключ посилання залежно від скрапера.

    Для скраперів з кількома типами документів (як Washington — Annual + Express)
    повертає перший НЕ-порожній і НЕ-плейсхолдер ключ. Це гарантує що
    Sheets не отримає '—' замість реального посилання.
    """
    for key in (
        "Statement of Information (Link)",
        "Annual Report (Link)",
        "Express Annual Report (Link)",
        "Посилання на PDF",
        "Посилання",
    ):
        v = item.get(key)
        if isinstance(v, str) and v and v != "—":
            return key
    return ""


def _build_full_address(item: dict) -> tuple[str, str]:
    """Збирає повну адресу + поштовий індекс з результату скрапера.

    Різні скрапери складають по-різному:
      France/Finland: "Адреса" (вулиця) + "Місто" + "Поштовий індекс"
      Czech/UK/Latvia/NZ: "Адреса" (одним рядком, вже містить все)
      Washington: "Address" + "City" + "State" + "Zip" (US-стиль)
      California: "Address" (одним рядком після додавання нашим патчем)
      Denmark: "Адреса" + "Місто" + "Поштовий індекс" (після нашого патча)

    Повертає (full_address_str, postal_code).
    """
    parts: list[str] = []
    # Перш за все шукаємо вулицю (різні назви ключів)
    for k in ("Address", "Адреса", "Street", "PrincipalOfficeAddress"):
        v = item.get(k)
        if isinstance(v, str) and v.strip() and v.strip() != "—":
            parts.append(v.strip())
            break
    # Місто
    for k in ("City", "Місто"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
            break
    # State (US)
    state = item.get("State", "") or ""
    if isinstance(state, str) and state.strip():
        parts.append(state.strip())
    # Zip / Поштовий індекс
    postal = ""
    for k in ("Zip", "ZIP", "Поштовий індекс", "PostalCode"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            postal = v.strip()
            parts.append(postal)
            break

    return (", ".join(parts), postal)


def _validate_address_for_item(item: dict, site_key: str, status_dict: dict) -> None:
    """Викликає Google Address Validation API, додає колонки в item.

    Колонки що додаються:
      - "Address Status"  → 🟢 OK / 🟡 Risk / 🔴 Bad / ⚙️ Не налаштовано
      - "Address Reason"  → коротка причина
      - "Place ID"        → Google Maps Place ID (для ручного обліку)
      - "Formatted Address" → канонічна адреса від Google

    Безпечно: будь-який exception ловиться, аналіз продовжує.
    """
    try:
        from analysis.address_validator import (
            SITE_TO_REGION,
            validate_address_sync,
        )
    except Exception as e:
        logger.warning("address_validator import failed: %s", e)
        return

    address, postal = _build_full_address(item)
    if not address:
        # Адреси нема → пишемо явно щоб юзер бачив
        item["Address Status"] = "⏭ Skipped"
        item["Address Reason"] = "no_address"
        return

    region = SITE_TO_REGION.get(site_key, "US")

    try:
        result = validate_address_sync(address, region, postal_code=postal)
    except Exception as e:
        logger.warning("AV validation crashed for %s: %s", item.get("Назва"), e)
        item["Address Status"] = "⚙️ Error"
        item["Address Reason"] = f"crash:{type(e).__name__}"
        return

    item["Address Status"] = result.status_emoji
    item["Address Reason"] = result.reason
    item["Place ID"] = result.place_id or ""
    item["Formatted Address"] = result.formatted

    # Інкремент лічильників у status_dict — для фінального summary
    s = result.status
    if s == "OK":
        status_dict["av_ok"] = status_dict.get("av_ok", 0) + 1
    elif s == "Risk":
        status_dict["av_risk"] = status_dict.get("av_risk", 0) + 1
    elif s == "Bad":
        status_dict["av_bad"] = status_dict.get("av_bad", 0) + 1
    else:
        status_dict["av_skipped"] = status_dict.get("av_skipped", 0) + 1
    if result.cached:
        status_dict["av_cached"] = status_dict.get("av_cached", 0) + 1


def _persist_result(item: dict, site_key: str, collected_data: list[dict],
                    status_dict: dict | None = None) -> None:
    """
    Зберігає один результат у базу + Google Sheets (якщо не дублікат).
    Єдина точка збереження замість 6 однакових блоків у run_scraping.

    Якщо status_dict["validate_address"] == True — додатково перевіряє
    адресу через Google Address Validation API.
    """
    name = item.get("Назва")
    link_key = _get_link_key(item)
    link = item.get(link_key, "")

    if not name or database.is_company_name_scraped(name):
        logger.debug("⏩ Пропущено (дублікат або порожня назва): %s", name)
        return

    database.save_company_to_db(name, link, site_key)

    # Sheets — через чергу: послідовно, з паузою, гарантовано дійде
    _enqueue_sheet_write(name, link, site_key)

    # ── Перевірка адреси (якщо увімкнено) ──
    # Викликається ПІСЛЯ збереження в БД — щоб у разі краху валідатора
    # сама компанія все одно зберіглась.
    if status_dict and status_dict.get("validate_address"):
        _validate_address_for_item(item, site_key, status_dict)

    collected_data.append(item)


def _run_simple_scraper(scraper_fn, args_builder, page, keyword: str,
                        max_count: int, site_key: str, status_dict: dict,
                        chat_id: int, collected_data: list[dict], file_format: str) -> bool:
    """
    Запускає скрапер (підтримує кілька ключових слів через кому).
    Повертає True — сигнал для run_scraping завершити роботу.

    Multi-keyword: "tech, software, startup" → три запити, результати об'єднуються.

    Ліміт для кожного ключового слова = залишок до max_count.
    Якщо перше слово дало лише 42 з 125 — наступне може заповнити решту.
    """
    keywords = [k.strip() for k in str(keyword).split(',') if k.strip()]

    for kw in keywords:
        if not status_dict.get('is_running', True):
            break

        remaining = max_count - len(collected_data)
        if remaining <= 0:
            break

        if len(keywords) > 1:
            logger.info("Multi-keyword [%s]: запит '%s' (ліміт %d, зібрано %d/%d)",
                        site_key, kw, remaining, len(collected_data), max_count)

        args    = args_builder(page, kw, remaining, status_dict)
        results = scraper_fn(*args)

        for item in results:
            if len(collected_data) >= max_count:
                break
            if not status_dict.get('is_running', True):
                break
            _persist_result(item, site_key, collected_data, status_dict=status_dict)

        # ── Оновлюємо загальний лічильник після кожного ключового слова ──
        # (scrapers оновлюють status_dict["current"] своїм per-keyword counter,
        #  тут скидаємо на реальний загальний підсумок)
        status_dict["current"] = len(collected_data)

    if collected_data:
        save_scraping_results(chat_id, collected_data, file_format, status_dict)
    return True


# scrape_france() видалено — France тепер використовує scrape_france_api()
# з france_scraper.py (JSON API pappers.ai, без браузера)


# scrape_finland() (старий Selenium-варіант) видалено — Finland тепер використовує
# scrape_finland_api() з finland.py (PRH YTJ v3 open data API, без браузера)


def _format_excel(tmp_path: str) -> None:
    """Форматує Excel-файл: кольоровий заголовок, авто-ширина, фільтр, зебра."""
    try:
        import openpyxl
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter

        wb = openpyxl.load_workbook(tmp_path)
        ws = wb.active

        # ── Стиль заголовка (темно-синій фон, білий жирний текст) ──
        header_fill = PatternFill("solid", fgColor="1F4E79")
        header_font = Font(color="FFFFFF", bold=True, size=11)
        header_align = Alignment(horizontal="center", vertical="center", wrap_text=False)

        for cell in ws[1]:
            cell.fill   = header_fill
            cell.font   = header_font
            cell.alignment = header_align

        # ── Зебра: кожен 2-й рядок — світло-блакитний ──
        stripe_fill = PatternFill("solid", fgColor="EBF5FB")
        for row_idx in range(2, ws.max_row + 1, 2):
            for cell in ws[row_idx]:
                if cell.fill.patternType == "none" or not cell.fill.fgColor.rgb:
                    cell.fill = stripe_fill

        # ── Авто-ширина колонок (максимум 60 символів) ──
        for col_idx, column_cells in enumerate(ws.columns, 1):
            max_len = 0
            for cell in column_cells:
                try:
                    val_len = len(str(cell.value)) if cell.value is not None else 0
                    max_len = max(max_len, val_len)
                except Exception:
                    pass
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 60)

        # ── Заморожений перший рядок + авто-фільтр ──
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        wb.save(tmp_path)
    except ImportError:
        logger.warning("openpyxl не встановлений — форматування Excel пропущено.")
    except Exception as e:
        logger.warning("Помилка форматування Excel: %s", e)


# Ключі, що використовуються як заголовок блоку (назва компанії) у TXT.
_TXT_TITLE_KEYS = ("Назва", "Name", "Company Name", "Title")


def _format_txt_readable(data: list[dict]) -> str:
    """Людиночитабельний TXT: кожна компанія — окремий блок 'поле : значення'
    з вирівнюванням і роздільником. Значно зручніше за TSV, коли є довгі
    поля (адреса). Формат універсальний — бере ті поля, що є в записі,
    тож працює для будь-якого скрапера.
    """
    if not data:
        return "Даних не знайдено.\n"

    def clean(v) -> str:
        # Прибираємо подвійні пробіли/таби/переноси (артефакти джерела),
        # заразом гарантуємо, що значення не зламає розмітку блоку.
        return re.sub(r"\s+", " ", str(v)).strip()

    # Ключі в порядку першої появи (зберігаємо порядок полів скрапера).
    keys: list[str] = []
    for row in data:
        for k in row:
            if k not in keys:
                keys.append(k)

    title_key = next((k for k in _TXT_TITLE_KEYS if k in keys), keys[0])
    field_keys = [k for k in keys if k != title_key]
    pad = max((len(k) for k in field_keys), default=0)

    sep = "─" * 64
    lines: list[str] = ["═" * 64, f"  Зібрано компаній: {len(data)}", "═" * 64, ""]
    for i, row in enumerate(data, 1):
        lines.append(f"#{i}  {clean(row.get(title_key, '')) or '—'}")
        for k in field_keys:
            if k in row:
                lines.append(f"    {k.ljust(pad)} : {clean(row.get(k, ''))}")
        lines.append(sep)
    lines.append("")
    return "\n".join(lines)


def save_scraping_results(chat_id: int, data: list[dict], file_format: str, status_dict: dict) -> None:
    ext = {"EXCEL": "xlsx", "JSON": "json"}.get(file_format, "txt")
    tmp_path = os.path.join(tempfile.gettempdir(), f"res_{chat_id}.{ext}")

    if file_format == "EXCEL":
        pd.DataFrame(data).to_excel(tmp_path, index=False, engine="openpyxl")
        _format_excel(tmp_path)
    elif file_format == "JSON":
        pd.DataFrame(data).to_json(tmp_path, orient="records", force_ascii=False, indent=4)
    else:
        # TXT — людиночитабельні блоки (utf-8-sig, щоб кирилиця коректно
        # відкривалась у Windows Notepad).
        with open(tmp_path, "w", encoding="utf-8-sig") as f:
            f.write(_format_txt_readable(data))

    status_dict['file_path'] = tmp_path
    logger.info("Файл збережено: %s", tmp_path)


def run_scraping(chat_id: int, keyword: str, max_count: int,
                 site_key: str, file_format: str, status_dict: dict) -> None:
    database.init_db()
    config = SCRAPER_CONFIG.get(site_key, {})
    collected_data: list[dict] = []

    # ── Таблиця маршрутизації скраперів ──
    # Формат: site_key -> (scraper_fn, args_builder)
    # args_builder(page, keyword, max_count, status_dict) → tuple аргументів
    #
    # API-скрапери (France, Finland, Latvia, UK, Turkey) — page=None, браузер не запускається.
    # Браузерні (California, Washington, Denmark, Czech, NZ, Thailand) — page=ChromiumPage.
    SIMPLE_SCRAPERS: dict[str, Any] = {
        # ── API-скрапери (без браузера) ──
        "France":        (scrape_france_api,   lambda p, kw, mc, sd: (kw, mc, sd)),
        "Finland":       (scrape_finland_api,  lambda p, kw, mc, sd: (kw, mc, sd)),
        "Norway":        (scrape_norway_api,   lambda p, kw, mc, sd: (kw, mc, sd)),
        "Latvia":        (scrape_latvia,        lambda p, kw, mc, sd: (kw, mc, sd)),
        "India":         (scrape_india_api,     lambda p, kw, mc, sd: (kw, mc, sd)),
        "UnitedKingdom": (scrape_uk_api,        lambda p, kw, mc, sd: (kw, mc, sd)),
        "Turkey":        (turkey_scraper.scrape_turkey, lambda p, kw, mc, sd: (None, kw, mc, sd)),
        # ── Браузерні скрапери ──
        "California":    (scrape_california,   lambda p, kw, mc, sd: (p, kw, mc, sd)),
        "Washington":    (scrape_washington,   lambda p, kw, mc, sd: (p, kw, mc, sd)),
        "Denmark":       (scrape_denmark,       lambda p, kw, mc, sd: (p, kw, mc, sd)),
        "CzechRepublic": (scrape_czech,         lambda p, kw, mc, sd: (p, kw, mc, sd)),
        "NewZealand":    (scrape_new_zealand,   lambda p, kw, mc, sd: (p, kw, mc, sd)),
        "Thailand":      (scrape_thailand,      lambda p, kw, mc, sd: (p, kw, mc, sd)),
    }

    # Браузер запускаємо ТІЛЬКИ для скраперів, яким він потрібен.
    # Turkey раніше був у BROWSER_BASED — тепер ходить через ITO Internal API (aiohttp).
    BROWSER_BASED: set = {"California", "Washington", "Denmark", "CzechRepublic", "NewZealand", "Thailand"}
    needs_browser = site_key in BROWSER_BASED

    page, relay = (None, None)
    if needs_browser:
        page, relay = get_page(chat_id, status_dict, site_key)
        if page is None:
            return

    try:
        if site_key in SIMPLE_SCRAPERS:
            scraper_fn, args_builder = SIMPLE_SCRAPERS[site_key]
            _run_simple_scraper(
                scraper_fn, args_builder, page, keyword, max_count,
                site_key, status_dict, chat_id, collected_data, file_format
            )
            return

        # Якщо site_key не входить до SIMPLE_SCRAPERS — невідомий скрапер
        logger.error("run_scraping: невідомий site_key '%s'", site_key)

    finally:
        status_dict['is_running'] = False
        if page is not None:
            try:
                page.quit()  # type: ignore[union-attr]
            except Exception:
                pass
        # Зупиняємо локальний проксі-relay
        if relay is not None:
            try:
                relay.stop()
            except Exception:
                pass
        # Чекаємо поки всі записи в Google Sheets дійдуть
        logger.info("[%d] Очікую завершення запису в Sheets...", chat_id)
        flush_sheets_queue()
        logger.info("[%d] Скрапер завершив роботу.", chat_id)
