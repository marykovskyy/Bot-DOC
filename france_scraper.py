"""
france_scraper.py — Скрапер Франції через внутрішній JSON API pappers.fr

API: https://api.pappers.ai/v2/recherche
Токен: знятий із HAR-файлу сайту (Frontend JS token).
       Зберігається у token.env як PAPPERS_API_TOKEN.
       Якщо змінять — оновити там.

Переваги перед браузерним підходом:
  - Без ChromiumPage, без CAPTCHA, без проксі
  - Швидкість: ~0.5 сек/сторінка замість ~10 сек/вкладка
  - Повні дані: SIREN, дата реєстрації, форма, адреса, NAF
  - Фільтр за роком — server-side (date_creation_min у запиті)
"""
from __future__ import annotations

import logging
import os
import re
import time

import requests
from dotenv import load_dotenv

from utils import retry_request

load_dotenv("token.env")

logger = logging.getLogger(__name__)

# ── Токен: береться з env, fallback — токен зі знятого HAR ──
_API_TOKEN = os.getenv(
    "PAPPERS_API_TOKEN",
    "97a405f1664a83329a7d89ebf51dc227b90633c4ba4a2575"
)
_BASE_URL = "https://api.pappers.ai/v2/recherche"
_PER_PAGE = 20          # записів на сторінку
_MAX_PAGE  = 20         # жорсткий ліміт API: сторінка 21+ повертає 400
_DELAY_SEC = 0.6        # пауза між сторінками (ввічливий scraping)
_TIMEOUT   = 30         # таймаут HTTP запиту


def _build_link(name: str, siren: str) -> str:
    """Будує URL компанії на pappers.fr за зразком сайту."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return f"https://www.pappers.fr/entreprise/{slug}-{siren}"


def scrape_france_api(keyword: str, max_count: int, status_dict: dict) -> list[dict]:
    """
    Шукає французькі компанії через JSON API pappers.ai.

    Повертає список словників для збереження у Excel/CSV.
    Поля:
      Назва, SIREN, Форма власності, Дата реєстрації,
      NAF код, Місто, Поштовий індекс, Адреса, Посилання

    Фільтри:
      - тільки активні компанії (etat=A + statut_consolide=actif)
      - дата реєстрації ВІД вказаного року (date_creation_min=YYYY-01-01)
      - дублікати за назвою пропускаються
    """
    target_year = str(status_dict.get("target_year", "0"))
    results: list[dict] = []
    seen_names: set[str] = set()
    page = 1

    year_label = f"від {target_year}" if target_year != "0" else "всі роки"
    status_dict["last_name"] = f"🇫🇷 Пошук: '{keyword}' ({year_label})..."

    while len(results) < max_count:
        if not status_dict.get("is_running", True):
            break

        # ── Зупиняємось до досягнення ліміту API (стор. 21+ → 400) ──
        if page > _MAX_PAGE:
            logger.info("Франція: досягнуто ліміт API (%d стор.), всього: %d", _MAX_PAGE, len(results))
            break

        params: dict = {
            "q":              keyword,
            "api_token":      _API_TOKEN,
            "precision":      "standard",
            "bases":          "entreprises",
            # etat=A НЕ використовуємо — цей параметр не фільтрує активність
            # в API pappers.ai (ігнорується або має інше значення).
            # Фільтрація відбувається на стороні клієнта нижче.
            "page":           page,
            "par_page":       _PER_PAGE,
            "case_sensitive": "false",
        }

        # ── Фільтр за датою реєстрації: ВІД вказаного року ──
        if target_year and target_year != "0":
            params["date_creation_min"] = f"{target_year}-01-01"

        _headers = {
            "Accept":     "application/json, text/plain, */*",
            "Origin":     "https://www.pappers.fr",
            "Referer":    "https://www.pappers.fr/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }
        try:
            resp = retry_request(
                requests.get,
                _BASE_URL,
                params=params,
                headers=_headers,
                timeout=_TIMEOUT,
                max_retries=3,
                delay=2.0,
            )

            # 400 на пізніх сторінках = API вичерпав результати
            if resp.status_code == 400:
                logger.info("Франція: API повернув 400 на стор. %d — кінець результатів.", page)
                break

            resp.raise_for_status()
            data = resp.json()

        except requests.exceptions.HTTPError as e:
            logger.error("Франція API HTTP помилка (стор. %d): %s", page, e)
            if resp.status_code in (401, 403):
                status_dict["last_name"] = (
                    "⚠️ PAPPERS_API_TOKEN застарів. "
                    "Оновіть токен у token.env."
                )
            break
        except Exception as e:
            logger.error("Франція API помилка (стор. %d): %s", page, e)
            break

        items: list[dict] = data.get("resultats", [])
        if not items:
            logger.info("Франція: стор. %d — результатів немає, завершуємо.", page)
            break

        for item in items:
            if len(results) >= max_count:
                break
            if not status_dict.get("is_running", True):
                break

            # ── Фільтр активності (три незалежні перевірки) ──
            # 1. statut_consolide: "actif" = активна, "inactif" = ліквідована
            # 2. entreprise_cessee: 0 = діє, 1 = припинила діяльність
            # 3. statut_rcs: "Radié" = викреслена з реєстру
            is_inactive = (
                item.get("statut_consolide") != "actif"
                or item.get("entreprise_cessee", 0) == 1
                or item.get("statut_rcs") == "Radié"
            )
            if is_inactive:
                status_dict["filtered_inactive"] = status_dict.get("filtered_inactive", 0) + 1
                continue

            # ── Резервний клієнтський фільтр по даті ──
            if target_year and target_year != "0":
                creation = item.get("date_creation", "")
                if creation and creation < f"{target_year}-01-01":
                    continue

            name: str = (item.get("nom_entreprise") or "").strip()
            if not name:
                continue

            # ── Дублікати по назві ──
            name_lower = name.lower()
            if name_lower in seen_names:
                status_dict["filtered_duplicate"] = status_dict.get("filtered_duplicate", 0) + 1
                continue
            seen_names.add(name_lower)

            siren: str = item.get("siren", "")
            siege: dict = item.get("siege") or {}

            naf_code  = item.get("code_naf", "")
            naf_label = item.get("libelle_code_naf", "")
            naf = f"{naf_code} — {naf_label}" if naf_code else ""

            company: dict = {
                "Назва":             name,
                "SIREN":             item.get("siren_formate", siren),
                "Форма власності":   item.get("forme_juridique", ""),
                "Дата реєстрації":   item.get("date_creation_formate",
                                              item.get("date_creation", "")),
                "NAF код":           naf,
                "Місто":             siege.get("ville", ""),
                "Поштовий індекс":   siege.get("code_postal", ""),
                "Адреса":            siege.get("adresse_ligne_1", ""),
                "Посилання":         _build_link(name, siren),
            }

            results.append(company)
            status_dict["last_name"] = name
            logger.info(
                "%d. %s | SIREN: %s | %s",
                len(results), name, siren, item.get("date_creation", "—"),
            )

        # Якщо прийшло менше записів ніж par_page — більше сторінок немає
        if len(items) < _PER_PAGE:
            logger.info("Франція: остання сторінка (%d), всього: %d", page, len(results))
            break

        page += 1
        time.sleep(_DELAY_SEC)

    return results
