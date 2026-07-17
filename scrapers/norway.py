"""
scrapers/norway.py — Скрапер Норвегії через відкритий API Brønnøysundregistrene
(Enhetsregisteret — Central Coordinating Register for Legal Entities).

API:     https://data.brreg.no/enhetsregisteret/api/enheter
Фінанси: https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}
Docs:    https://data.brreg.no/enhetsregisteret/swagger-ui/
Безкоштовно, без токена, без CAPTCHA, без браузера.

Параметри пошуку (підтверджено наживо):
  navn                                — пошук за назвою (часткове співпадіння)
  fraRegistreringsdatoEnhetsregisteret — дата реєстрації ВІД (YYYY-MM-DD)
  konkurs=false                       — НЕ збанкрутілі
  underAvvikling=false                — НЕ в ліквідації
  size, page                          — пагінація (обмеження: (page+1)*size <= 10000)

Структура відповіді:
  page.totalElements        — загальна кількість
  _embedded.enheter[]       — масив компаній, кожна має:
    organisasjonsnummer, navn, organisasjonsform{kode,beskrivelse},
    registreringsdatoEnhetsregisteret, naeringskode1{kode,beskrivelse},
    forretningsadresse{adresse[],postnummer,poststed,kommune,landkode},
    epostadresse, mobil, konkurs, underAvvikling,
    underTvangsavviklingEllerTvangsopplosning, sisteInnsendteAarsregnskap

Фільтр статусу:
  Активна = konkurs=False AND underAvvikling=False
            AND underTvangsavviklingEllerTvangsopplosning=False.
  Решта (банкрут / ліквідація) відкидаємо — як в інших скраперах.
"""
from __future__ import annotations

import logging
import time

import requests

from utils import retry_request

logger = logging.getLogger(__name__)

_BASE_URL  = "https://data.brreg.no/enhetsregisteret/api/enheter"
_LINK_BASE = "https://virksomhet.brreg.no/nb/oppslag/enheter"
_PER_PAGE  = 100        # записів на сторінку
# Brreg обмежує (page+1)*size <= 10000 → не більше 100 сторінок по 100.
_MAX_PAGES = 100
_DELAY_SEC = 0.4
_TIMEOUT   = 30


def _get_address(ent: dict) -> tuple[str, str, str]:
    """Витягує (вулиця, поштовий індекс, місто) з forretningsadresse."""
    addr = ent.get("forretningsadresse") or {}
    street_parts = addr.get("adresse") or []
    street = ", ".join(p for p in street_parts if p) if isinstance(street_parts, list) else str(street_parts or "")
    return street.strip(), str(addr.get("postnummer", "")).strip(), str(addr.get("poststed", "")).strip()


def _is_active(ent: dict) -> bool:
    """Активна = не банкрут, не в ліквідації (звичайній чи примусовій)."""
    if ent.get("konkurs"):
        return False
    if ent.get("underAvvikling"):
        return False
    if ent.get("underTvangsavviklingEllerTvangsopplosning"):
        return False
    return True


def scrape_norway_api(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    """
    Шукає норвезькі компанії через відкритий API Brønnøysundregistrene.

    Повертає список словників для збереження у Excel/CSV.
    Поля: Назва, Орг. номер, Форма власності, Дата реєстрації, Галузь (NACE),
          Місто, Поштовий індекс, Адреса, Email, Телефон, Останній звіт, Посилання

    Фільтри:
      - тільки активні (konkurs=false, underAvvikling=false)
      - дата реєстрації ВІД вказаного року (server-side + client-side)
      - дублікати за назвою пропускаються
    """
    target_year = str(status_dict.get("target_year", "0"))
    results: list[dict] = []
    seen_names: set[str] = set()
    page_num = 0

    year_label = f"від {target_year}" if target_year != "0" else "всі роки"
    status_dict["last_name"] = f"🇳🇴 Пошук: '{keyword}' ({year_label})..."

    while len(results) < max_count and page_num < _MAX_PAGES:
        if not status_dict.get("is_running", True):
            break

        params: dict = {
            "navn":           keyword,
            "size":           _PER_PAGE,
            "page":           page_num,
            "konkurs":        "false",
            "underAvvikling": "false",
        }
        if target_year and target_year != "0":
            params["fraRegistreringsdatoEnhetsregisteret"] = f"{target_year}-01-01"

        try:
            resp = retry_request(
                requests.get,
                _BASE_URL,
                params=params,
                headers={"Accept": "application/json"},
                timeout=_TIMEOUT,
                max_retries=3,
                delay=2.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Норвегія API помилка (page=%d): %s", page_num, e)
            break

        enheter: list = (data.get("_embedded") or {}).get("enheter") or []
        total = (data.get("page") or {}).get("totalElements", 0)

        if not enheter:
            logger.info("Норвегія: результатів немає (page=%d), завершуємо.", page_num)
            break

        logger.info("Норвегія: page=%d, отримано=%d, всього у API=%d",
                    page_num, len(enheter), total)

        for ent in enheter:
            if len(results) >= max_count:
                break
            if not status_dict.get("is_running", True):
                break

            # ── Фільтр активності (клієнтський дубль-захист) ──
            if not _is_active(ent):
                status_dict["filtered_inactive"] = status_dict.get("filtered_inactive", 0) + 1
                continue

            name = (ent.get("navn") or "").strip()
            if not name:
                continue

            # ── Дублікати по назві ──
            name_lower = name.lower()
            if name_lower in seen_names:
                status_dict["filtered_duplicate"] = status_dict.get("filtered_duplicate", 0) + 1
                continue
            seen_names.add(name_lower)

            org_nr = str(ent.get("organisasjonsnummer", "")).strip()
            org_form = ent.get("organisasjonsform") or {}
            naering  = ent.get("naeringskode1") or {}
            naering_str = ""
            if naering.get("kode"):
                naering_str = f"{naering.get('kode')} — {naering.get('beskrivelse', '')}".strip(" —")

            street, postnummer, poststed = _get_address(ent)

            company: dict = {
                "Назва":            name,
                "Орг. номер":       org_nr,
                "Форма власності":  org_form.get("beskrivelse", "") or org_form.get("kode", ""),
                "Дата реєстрації":  ent.get("registreringsdatoEnhetsregisteret", "") or "",
                "Галузь (NACE)":    naering_str,
                "Місто":            poststed,
                "Поштовий індекс":  postnummer,
                "Адреса":           street,
                "Email":            ent.get("epostadresse", "") or "",
                "Телефон":          ent.get("mobil", "") or "",
                "Останній звіт":    str(ent.get("sisteInnsendteAarsregnskap", "") or ""),
                "Посилання":        f"{_LINK_BASE}/{org_nr}",
            }

            results.append(company)
            status_dict["last_name"] = name
            status_dict["current"]   = len(results)
            logger.info(
                "%d. %s | org: %s | %s",
                len(results), name, org_nr,
                ent.get("registreringsdatoEnhetsregisteret", "—"),
            )

        # ── Кінець результатів / ліміт API ──
        if (page_num + 1) * _PER_PAGE >= total:
            logger.info("Норвегія: досягнуто кінця (total=%d, page=%d), всього: %d",
                        total, page_num, len(results))
            break
        # Захист від ліміту Brreg (page+1)*size <= 10000
        if (page_num + 2) * _PER_PAGE > 10000:
            logger.info("Норвегія: ліміт API 10000 записів, зупиняємось.")
            break

        page_num += 1
        time.sleep(_DELAY_SEC)

    return results
