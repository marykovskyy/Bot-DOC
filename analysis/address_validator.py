"""
analysis/address_validator.py — Google Address Validation API клієнт.

Призначення: перевірка чи адреса компанії реальна, повна і валідна
для use-case'у Google Ads anti-Circumvention (листопадовий 2025 апдейт
Google Ads policy: фейкова адреса = перманентний бан).

Архітектура:
  AddressValidator(api_key)
    ├─ validate(addr_dict, region_code) → ValidationResult
    │  └─ використовує SQLite кеш (TTL 30 днів) щоб не платити за повтори
    └─ classify(api_response) → (status, reason)
       Decision tree описаний у docstring _classify().

Безпечна деградація:
  - Якщо api_key не задано → status="not_configured", аналіз не падає
  - Якщо API timeout/5xx → 3 retry з backoff, далі status="error"
  - Якщо country поза покриттям API → status="unsupported_country"
  - Будь-який неочікуваний exception → status="error" + лог + continue

Покриття країн (станом на 2025-Q1):
  US, GB, FR, DE, IT, ES, NL, BE, AT, CH, PL, SE, NO, DK, FI, IE, PT,
  CZ, SK, HU, GR, RO, BG, HR, SI, EE, LV, LT, LU, AU, NZ, BR, MX, CA,
  JP (preview), IN (preview).
  НЕ підтримуються: Thailand, Turkey, Ukraine.

Free tier (з 1 березня 2025):
  - 5 000 запитів/міс безкоштовно (Pro tier SKU)
  - Понад: $17 / 1 000 запитів

Endpoint:
  POST https://addressvalidation.googleapis.com/v1:validateAddress?key=KEY
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────
#  КОНСТАНТИ
# ─────────────────────────────────────────────────────────────────────────

API_URL = "https://addressvalidation.googleapis.com/v1:validateAddress"
API_TIMEOUT = 15      # секунд на один запит
API_RETRIES = 3
CACHE_TTL_DAYS = 30   # повторно не валідуємо ту саму адресу 30 днів

# ISO-коди країн які підтримуються Google Address Validation API
SUPPORTED_REGIONS = {
    "US", "GB", "FR", "DE", "IT", "ES", "NL", "BE", "AT", "CH",
    "PL", "SE", "NO", "DK", "FI", "IE", "PT", "CZ", "SK", "HU",
    "GR", "RO", "BG", "HR", "SI", "EE", "LV", "LT", "LU",
    "AU", "NZ", "BR", "MX", "CA", "JP", "IN", "MY", "SG", "PR",
    "AR", "CL", "CO",
}

# Маппінг наших site_key → ISO region code
SITE_TO_REGION = {
    "California":    "US",
    "Washington":    "US",
    "France":        "FR",
    "Denmark":       "DK",
    "Finland":       "FI",
    "Norway":        "NO",
    "CzechRepublic": "CZ",
    "UnitedKingdom": "GB",
    "Latvia":        "LV",
    "NewZealand":    "NZ",
    "Thailand":      "TH",   # NOT supported
    "Turkey":        "TR",   # NOT supported
}


# ─────────────────────────────────────────────────────────────────────────
#  ДАНІ
# ─────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    """Результат перевірки однієї адреси.

    Поля що йдуть в Excel:
      status_emoji:   🟢 / 🟡 / 🔴 / ⚙️
      reason:         коротка людська причина
      formatted:      канонічна адреса від Google
      place_id:       Google Maps Place ID (для ручної перевірки)
    """
    status: str = "unknown"          # "OK" | "Risk" | "Bad" | "Error" | "Skipped" | "NotConfigured"
    reason: str = ""
    formatted: str = ""
    place_id: str | None = None
    lat: float | None = None
    lng: float | None = None
    cached: bool = False
    raw: dict = field(default_factory=dict)

    @property
    def status_emoji(self) -> str:
        return {
            "OK":             "🟢 OK",
            "Risk":           "🟡 Risk",
            "Bad":            "🔴 Bad",
            "Error":          "⚙️ Error",
            "Skipped":        "⏭ Skipped",
            "NotConfigured":  "⚙️ Не налаштовано",
        }.get(self.status, "❓")


# ─────────────────────────────────────────────────────────────────────────
#  КЕШ (SQLite)
# ─────────────────────────────────────────────────────────────────────────

def _cache_db_path() -> str:
    """Шлях до того ж companies.db де решта state бота."""
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "companies.db")


def init_address_cache() -> None:
    """Створює таблицю кешу якщо її ще нема. Викликати один раз на старті."""
    with sqlite3.connect(_cache_db_path()) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS address_validation_cache (
                addr_hash       TEXT PRIMARY KEY,
                region          TEXT,
                status          TEXT,
                reason          TEXT,
                formatted       TEXT,
                place_id        TEXT,
                lat             REAL,
                lng             REAL,
                raw_response    TEXT,
                validated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


def _hash_address(address: str, region: str) -> str:
    """Стабільний хеш адреси для ключа кешу.

    Нормалізуємо: lowercase, прибираємо зайві пробіли/коми, додаємо region.
    Так "1 Apple Park Way, Cupertino, CA" і "1 APPLE PARK WAY  Cupertino CA"
    дають один хеш.
    """
    norm = " ".join((address or "").lower().replace(",", " ").split())
    return hashlib.md5(f"{region}|{norm}".encode()).hexdigest()


def _cache_get(addr_hash: str) -> ValidationResult | None:
    """Дістає валідний (не TTL-протермінований) запис з кешу."""
    cutoff = (datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)).isoformat()
    try:
        with sqlite3.connect(_cache_db_path()) as conn:
            row = conn.execute(
                """SELECT status, reason, formatted, place_id, lat, lng, raw_response
                   FROM address_validation_cache
                   WHERE addr_hash = ? AND validated_at > ?""",
                (addr_hash, cutoff),
            ).fetchone()
    except sqlite3.Error as e:
        logger.warning("address cache get error: %s", e)
        return None

    if not row:
        return None

    raw = {}
    try:
        raw = json.loads(row[6] or "{}")
    except json.JSONDecodeError:
        pass

    return ValidationResult(
        status=row[0], reason=row[1] or "", formatted=row[2] or "",
        place_id=row[3], lat=row[4], lng=row[5], cached=True, raw=raw,
    )


def _cache_put(addr_hash: str, region: str, result: ValidationResult) -> None:
    """Зберігає / оновлює запис у кеші."""
    try:
        with sqlite3.connect(_cache_db_path()) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO address_validation_cache
                   (addr_hash, region, status, reason, formatted,
                    place_id, lat, lng, raw_response, validated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (addr_hash, region, result.status, result.reason,
                 result.formatted, result.place_id, result.lat, result.lng,
                 json.dumps(result.raw, ensure_ascii=False)[:50000]),
            )
            conn.commit()
    except sqlite3.Error as e:
        logger.warning("address cache put error: %s", e)


# ─────────────────────────────────────────────────────────────────────────
#  DECISION TREE — класифікація API-відповіді → 🟢/🟡/🔴
# ─────────────────────────────────────────────────────────────────────────

def _classify(api_response: dict) -> tuple[str, str]:
    """Інтерпретує відповідь Google Address Validation API.

    Повертає (status, reason):
      "OK"   — адреса повна, підтверджена до рівня будинку
      "Risk" — адреса в цілому валідна, але є unconfirmed/inferred/replaced
      "Bad"  — PO Box / unconfirmed_suspicious / тільки до рівня міста / DPV=N

    Логіка (по убуванню важливості):
      1. PO Box                                     → Bad (для US)
      2. validationGranularity ∈ {OTHER, COUNTRY,
         ADMINISTRATIVE_AREA, LOCALITY, ROUTE}      → Bad (надто загальна)
      3. Будь-який component UNCONFIRMED_AND_SUSPICIOUS → Bad
      4. uspsData.dpvConfirmation == "N" (US)       → Bad
      5. residential == True (US)                   → Risk
      6. hasUnconfirmedComponents/Inferred/Replaced → Risk
      7. uspsData.dpvConfirmation ∈ {"S", "D"}      → Risk
      8. Інакше + addressComplete                   → OK
    """
    result = api_response.get("result", {})
    verdict = result.get("verdict", {}) or {}
    address = result.get("address", {}) or {}
    metadata = result.get("metadata", {}) or {}
    uspsData = result.get("uspsData", {}) or {}
    components = address.get("addressComponents", []) or []

    # ── 1. PO Box (тільки US повертає це поле) ──
    if metadata.get("poBox") is True:
        return ("Bad", "po_box")

    # ── 2. Геокод впав до низької granularity ──
    bad_granularity = {"OTHER", "COUNTRY", "ADMINISTRATIVE_AREA", "LOCALITY", "ROUTE"}
    val_gran = verdict.get("validationGranularity", "") or ""
    if val_gran in bad_granularity:
        return ("Bad", f"low_granularity:{val_gran}")

    # ── 3. Suspicious component ──
    for c in components:
        cl = c.get("confirmationLevel", "")
        if cl == "UNCONFIRMED_AND_SUSPICIOUS":
            ctype = c.get("componentType", "?")
            return ("Bad", f"suspicious_component:{ctype}")

    # ── 4. USPS DPV (US) ──
    dpv = uspsData.get("dpvConfirmation", "")
    if dpv == "N":
        return ("Bad", "usps_dpv_undeliverable")

    # ── 5. Residential (US) — для бізнесу = підозра ──
    is_residential = metadata.get("residential") is True
    if is_residential:
        return ("Risk", "residential")

    # ── 6. Unconfirmed / Inferred / Replaced ──
    risk_flags = []
    if verdict.get("hasUnconfirmedComponents"):
        risk_flags.append("unconfirmed")
    if verdict.get("hasInferredComponents"):
        risk_flags.append("inferred")
    if verdict.get("hasReplacedComponents"):
        risk_flags.append("replaced")
    if risk_flags:
        return ("Risk", "+".join(risk_flags))

    # ── 7. DPV S (secondary missing) / D (secondary unconfirmed) ──
    if dpv in ("S", "D"):
        return ("Risk", f"usps_dpv_{dpv}")

    # ── 8. addressComplete для остаточного OK ──
    if not verdict.get("addressComplete", False):
        return ("Risk", "not_complete")

    # Все ок
    is_business = metadata.get("business") is True   # буває None для не-US
    reason = "confirmed_premise"
    if is_business:
        reason += "+business"
    return ("OK", reason)


# ─────────────────────────────────────────────────────────────────────────
#  КЛІЄНТ
# ─────────────────────────────────────────────────────────────────────────

class AddressValidator:
    """Async-клієнт до Google Address Validation API.

    Безпечна деградація: якщо api_key порожній — клас залишається робочим,
    але всі validate() повертають status='NotConfigured'. Аналіз продовжує
    працювати, в Excel просто буде колонка ⚙️ замість 🟢/🟡/🔴.
    """

    def __init__(self, api_key: str | None = None):
        self.api_key = (api_key or "").strip()
        self.enabled = bool(self.api_key)
        self._session: aiohttp.ClientSession | None = None
        # Лічильник у межах сесії — для daily summary
        self.stats = {"total": 0, "cached": 0, "api_calls": 0,
                      "ok": 0, "risk": 0, "bad": 0, "error": 0, "skipped": 0}
        if not self.enabled:
            logger.info("AddressValidator: GOOGLE_MAPS_API_KEY не задано — "
                        "перевірка адрес вимкнена (graceful)")

    async def __aenter__(self) -> AddressValidator:
        if self.enabled:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT),
            )
        return self

    async def __aexit__(self, *_exc) -> None:
        if self._session:
            await self._session.close()

    # ─── Основний публічний метод ──────────────────────────────────────
    async def validate(self, address: str, region: str = "US",
                       *, postal_code: str = "", city: str = "") -> ValidationResult:
        """Валідує одну адресу. Повертає ValidationResult.

        Args:
          address:     основна адреса (вулиця + дім). Може містити місто/zip
                       — Google всеодно нормалізує.
          region:      ISO-2 код країни (US/GB/FR/...). Обов'язково.
          postal_code: ZIP/postal — допомагає Google вибрати точний матч.
          city:        опційно — те саме.

        Result.status:
          - NotConfigured: API ключ відсутній
          - Skipped:      адреса порожня або країна не підтримується
          - OK / Risk / Bad: результат валідації
          - Error:        мережа / API лежить / quota
        """
        self.stats["total"] += 1

        # ── Швидкі ранні відмови ──
        if not self.enabled:
            self.stats["skipped"] += 1
            return ValidationResult(status="NotConfigured",
                                    reason="api_key_missing")

        if not address or len(address.strip()) < 5:
            self.stats["skipped"] += 1
            return ValidationResult(status="Skipped", reason="empty_or_too_short")

        region = (region or "").upper()
        if region not in SUPPORTED_REGIONS:
            self.stats["skipped"] += 1
            return ValidationResult(status="Skipped",
                                    reason=f"unsupported_region:{region}")

        # ── Кеш ──
        addr_hash = _hash_address(address, region)
        cached = _cache_get(addr_hash)
        if cached:
            self.stats["cached"] += 1
            self._bump_status_stat(cached.status)
            logger.debug("AV cache HIT %s → %s", addr_hash[:8], cached.status)
            return cached

        # ── Запит до API ──
        result = await self._call_api(address, region,
                                      postal_code=postal_code, city=city)
        self.stats["api_calls"] += 1
        self._bump_status_stat(result.status)

        # Пишемо у кеш тільки результати API (не "NotConfigured" / "Skipped")
        if result.status in ("OK", "Risk", "Bad"):
            _cache_put(addr_hash, region, result)

        return result

    # ─── Внутрішні методи ──────────────────────────────────────────────
    def _bump_status_stat(self, status: str) -> None:
        key = {"OK": "ok", "Risk": "risk", "Bad": "bad",
               "Error": "error", "Skipped": "skipped",
               "NotConfigured": "skipped"}.get(status)
        if key:
            self.stats[key] += 1

    async def _call_api(self, address: str, region: str,
                        *, postal_code: str, city: str) -> ValidationResult:
        """Викликає Address Validation API з retry + backoff."""
        if self._session is None:
            return ValidationResult(status="Error", reason="no_session")

        # Build request body. addressLines — головна вулиця;
        # postalCode/locality/regionCode — окремо для structured-input.
        addr_payload: dict[str, Any] = {
            "regionCode": region,
            "addressLines": [address.strip()],
        }
        if postal_code:
            addr_payload["postalCode"] = postal_code.strip()
        if city:
            addr_payload["locality"] = city.strip()

        body = {
            "address": addr_payload,
            # USPS CASS працює тільки для US/PR і дає DPV — критично для anti-ban
            "enableUspsCass": region in {"US", "PR"},
        }

        url = f"{API_URL}?key={self.api_key}"

        last_err: str = ""
        for attempt in range(API_RETRIES):
            try:
                async with self._session.post(url, json=body) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._build_result(data)

                    text = (await r.text())[:300]
                    last_err = f"http_{r.status}"

                    # Permanent errors — не retry'ємо
                    if r.status in (400, 401, 403):
                        logger.warning("AV API %s for region=%s: %s",
                                       r.status, region, text)
                        return ValidationResult(
                            status="Error",
                            reason=f"http_{r.status}",
                            raw={"error": text},
                        )

                    # 429 / 5xx — backoff + retry
                    await asyncio.sleep(0.5 * (2 ** attempt))
            except TimeoutError:
                last_err = "timeout"
                await asyncio.sleep(0.5 * (2 ** attempt))
            except aiohttp.ClientError as e:
                last_err = f"client_error:{type(e).__name__}"
                await asyncio.sleep(0.5 * (2 ** attempt))
            except Exception as e:
                logger.exception("AV API unexpected error: %s", e)
                last_err = f"unexpected:{type(e).__name__}"
                break

        return ValidationResult(status="Error", reason=last_err)

    @staticmethod
    def _build_result(api_response: dict) -> ValidationResult:
        """З raw-JSON Google API → ValidationResult."""
        status, reason = _classify(api_response)

        result = api_response.get("result", {}) or {}
        addr = result.get("address", {}) or {}
        geo = result.get("geocode", {}) or {}
        loc = geo.get("location", {}) or {}

        return ValidationResult(
            status=status,
            reason=reason,
            formatted=addr.get("formattedAddress", "") or "",
            place_id=geo.get("placeId"),
            lat=loc.get("latitude"),
            lng=loc.get("longitude"),
            cached=False,
            raw=api_response,
        )

    # ─── Зведення для Telegram-summary ─────────────────────────────────
    def summary(self) -> str:
        s = self.stats
        if s["total"] == 0:
            return ""
        cache_rate = (s["cached"] / s["total"] * 100) if s["total"] else 0
        return (
            f"🗺 <b>Перевірка адрес:</b>\n"
            f"  всього: {s['total']}\n"
            f"  🟢 OK: {s['ok']}, 🟡 Risk: {s['risk']}, 🔴 Bad: {s['bad']}\n"
            f"  ⚙️ Skip/Err: {s['skipped'] + s['error']}\n"
            f"  Кеш-хіт: {s['cached']}/{s['total']} ({cache_rate:.0f}%)\n"
            f"  API запитів: {s['api_calls']}"
        )


# ─────────────────────────────────────────────────────────────────────────
#  СИНХРОННІ ОБГОРТКИ для виклику зі скраперів (DrissionPage потоки)
# ─────────────────────────────────────────────────────────────────────────

# Глобальний клієнт + окремий event loop на потоці scraper-worker.
# Скрапери запускаються в threading.Thread (бо DrissionPage блокуючий),
# тому викликати asyncio з них через asyncio.run() — безпечно.
_global_validator: AddressValidator | None = None
_global_validator_lock = None  # ленива ініціалізація


def get_validator(api_key: str | None = None) -> AddressValidator:
    """Singleton-getter. api_key береться з config якщо не переданий."""
    global _global_validator
    if _global_validator is None:
        if api_key is None:
            try:
                import os
                api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
            except Exception:
                api_key = ""
        _global_validator = AddressValidator(api_key)
        init_address_cache()
    return _global_validator


def validate_address_sync(address: str, region: str = "US",
                          *, postal_code: str = "",
                          city: str = "") -> ValidationResult:
    """Синхронний фасад — для виклику з потоку scraper-worker.

    Використовує одноразовий event loop. Приєднує self.session на час
    одного виклику — для batch-валідації (багато адрес підряд) краще
    використовувати async-варіант через AddressValidator у одному loop.
    """
    validator = get_validator()

    if not validator.enabled:
        return ValidationResult(status="NotConfigured", reason="api_key_missing")

    async def _one():
        async with AddressValidator(validator.api_key) as v:
            return await v.validate(address, region,
                                    postal_code=postal_code, city=city)

    try:
        return asyncio.run(_one())
    except RuntimeError:
        # Якщо event loop вже працює (рідко в нашому потоці) —
        # створюємо окремий
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_one())
        finally:
            loop.close()
    except Exception as e:
        logger.exception("validate_address_sync error: %s", e)
        return ValidationResult(status="Error",
                                reason=f"sync_wrapper:{type(e).__name__}")
