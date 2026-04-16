"""
document_generator.py — Генерація документів із PSD-шаблонів

Можливості:
  - Текстові поля з вирівнюванням, переносом, кольором
  - Вставка фото у визначену область (type: photo)
  - Кастомний шрифт на поле (font: "OcrB.ttf")
  - Автогенерація MRZ (auto: mrz_line1 / mrz_line2)
  - Валідація полів (validation: {...})
  - Вивід у PNG / JPEG / PDF

Структура папок:
  templates/
    passport_de/
      background.png
      config.json
      fonts/           ← опційна папка з TTF
        OcrB.ttf
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# ── Пошук системних шрифтів (TTF) ──────────────────────────────────────
_FONT_SEARCH_PATHS = [
    "C:/Windows/Fonts/",
    "/usr/share/fonts/truetype/dejavu/",
    "/usr/share/fonts/truetype/liberation/",
    "/usr/share/fonts/truetype/freefont/",
    "/Library/Fonts/",
    "/System/Library/Fonts/",
]
_FONT_NAMES_REGULAR = ["Arial.ttf", "arial.ttf", "DejaVuSans.ttf",
                        "LiberationSans-Regular.ttf", "FreeSans.ttf"]
_FONT_NAMES_BOLD    = ["Arial Bold.ttf", "arialbd.ttf", "DejaVuSans-Bold.ttf",
                        "LiberationSans-Bold.ttf", "FreeSansBold.ttf"]


def _find_system_font(bold: bool = False) -> str | None:
    candidates = _FONT_NAMES_BOLD if bold else _FONT_NAMES_REGULAR
    for folder in _FONT_SEARCH_PATHS:
        for name in candidates:
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                return path
    return None


def _load_system_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    path = _find_system_font(bold)
    if path:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    logger.warning("TTF шрифт не знайдено, використовую стандартний bitmap.")
    return ImageFont.load_default()


# ── Валідація полів ─────────────────────────────────────────────────────

def validate_field(value: str, rules: dict) -> str | None:
    """Перевіряє значення поля за правилами validation.

    Returns:
        Повідомлення про помилку або None якщо валідне.
    """
    if not rules:
        return None

    if "max_length" in rules and len(value) > rules["max_length"]:
        return f"Максимум {rules['max_length']} символів (введено {len(value)})"

    if "min_length" in rules and len(value) < rules["min_length"]:
        return f"Мінімум {rules['min_length']} символів (введено {len(value)})"

    if "length" in rules and len(value) != rules["length"]:
        return f"Потрібно рівно {rules['length']} символів (введено {len(value)})"

    if "choices" in rules:
        allowed = [c.upper() for c in rules["choices"]]
        if value.upper() not in allowed:
            return f"Допустимі значення: {', '.join(rules['choices'])}"

    if "pattern" in rules:
        if not re.match(rules["pattern"], value):
            return rules.get("hint", "Невірний формат")

    return None


# ── Генератор документів ────────────────────────────────────────────────

class DocumentGenerator:
    """
    Генератор документів із PNG-фону + JSON-конфігу.

    Підтримує:
      - Текстові поля (x, y, font_size, bold, color, align, max_width)
      - Поля з фото (type: photo, x, y, width, height)
      - Кастомні шрифти (font: "OcrB.ttf" → шукає в template/fonts/)
      - Автополя MRZ (auto: mrz_line1 / mrz_line2)
      - Вивід у PNG / JPEG / PDF
    """

    def __init__(self, template_dir: str | Path, font_path: str | None = None):
        self.template_dir = Path(template_dir)
        self.font_path_override = font_path

        bg_path = self.template_dir / "background.png"
        cfg_path = self.template_dir / "config.json"

        if not bg_path.exists():
            raise FileNotFoundError(f"background.png не знайдено: {bg_path}")
        if not cfg_path.exists():
            raise FileNotFoundError(f"config.json не знайдено: {cfg_path}")

        with open(cfg_path, encoding="utf-8") as f:
            self.config: dict = json.load(f)

        self._bg_image: Image.Image = Image.open(str(bg_path)).convert("RGBA")
        self._font_cache: dict[tuple, ImageFont.FreeTypeFont] = {}

        logger.info("DocumentGenerator: шаблон '%s' завантажено (%d полів)",
                    self.config.get("name", template_dir),
                    len(self.config.get("fields", {})))

    # ── Шрифти ────────────────────────────────────────────────────────

    def _get_font(self, size: int, bold: bool,
                  custom_font: str | None = None) -> ImageFont.FreeTypeFont:
        """Завантажує шрифт з кешем.

        Пошук:
          1. custom_font → template_dir/fonts/ → системні папки
          2. font_path_override (конструктор)
          3. Системний шрифт (Arial / DejaVu / FreeSans)
        """
        key = (size, bold, custom_font or "")
        if key in self._font_cache:
            return self._font_cache[key]

        font: ImageFont.FreeTypeFont | None = None

        # 1. Кастомний шрифт з конфігу поля
        if custom_font:
            # Шукаємо в папці шаблону
            local_path = self.template_dir / "fonts" / custom_font
            if local_path.is_file():
                try:
                    font = ImageFont.truetype(str(local_path), size)
                except Exception:
                    pass
            # Шукаємо в системних папках
            if not font:
                for folder in _FONT_SEARCH_PATHS:
                    sys_path = os.path.join(folder, custom_font)
                    if os.path.isfile(sys_path):
                        try:
                            font = ImageFont.truetype(sys_path, size)
                            break
                        except Exception:
                            pass

        # 2. Override з конструктора
        if not font and self.font_path_override:
            try:
                font = ImageFont.truetype(self.font_path_override, size)
            except Exception:
                pass

        # 3. Системний шрифт
        if not font:
            font = _load_system_font(size, bold)

        self._font_cache[key] = font
        return font

    # ── Текст ─────────────────────────────────────────────────────────

    @staticmethod
    def _wrap_text(text: str, font: ImageFont.FreeTypeFont,
                   max_width: int, draw: ImageDraw.ImageDraw) -> list[str]:
        if not max_width:
            return [text]
        words = text.split()
        lines: list[str] = []
        current = ""
        for word in words:
            test = (current + " " + word).strip()
            bbox = draw.textbbox((0, 0), test, font=font)
            if bbox[2] - bbox[0] <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        return lines or [text]

    @staticmethod
    def _draw_text_aligned(draw: ImageDraw.ImageDraw, x: int, y: int,
                           text: str, font: ImageFont.FreeTypeFont,
                           color: tuple, align: str) -> None:
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        if align == "right":
            x = x - w
        elif align == "center":
            x = x - w // 2
        draw.text((x, y), text, font=font, fill=color)

    # ── Авто-поля (computed, MRZ, дати) ─────────────────────────────

    @staticmethod
    def _compute_auto_fields(data: dict, fields_cfg: dict) -> dict:
        """Обчислює авто-поля на основі інших полів.

        Підтримувані auto-типи:
          - "mrz_line1" / "mrz_line2" — MRZ ICAO 9303
          - "today"                   — сьогоднішня дата (DD.MM.YYYY)
          - "expiry_10y"              — issue_date + 10 років
          - "nationality_from_country"— мапінг country_code → nationality
          - "doc_number_random"       — випадковий номер у форматі C00X00T00
        """
        from datetime import datetime, timedelta
        import random

        auto_fields = {k: v for k, v in fields_cfg.items() if v.get("auto")}
        if not auto_fields:
            return data

        # ── Прості авто-поля (до MRZ, бо MRZ залежить від них) ────
        _NATIONALITY_MAP = {
            "D": "DEUTSCH", "DEU": "DEUTSCH",
            "F": "FRANCAIS", "FRA": "FRANCAIS",
            "I": "ITALIANO", "ITA": "ITALIANO",
            "E": "ESPANOL", "ESP": "ESPANOL",
            "P": "PORTUGUES", "PRT": "PORTUGUES",
            "NL": "NEDERLANDER", "NLD": "NEDERLANDER",
            "B": "BELGE", "BEL": "BELGE",
            "A": "OSTERREICHISCH", "AUT": "OSTERREICHISCH",
            "CH": "SCHWEIZER", "CHE": "SCHWEIZER",
            "GBR": "BRITISH", "GB": "BRITISH",
            "USA": "AMERICAN", "US": "AMERICAN",
            "POL": "POLSKIE", "PL": "POLSKIE",
            "UKR": "UKRAINETS", "UA": "UKRAINETS",
            "CZE": "CESKE", "CZ": "CESKE",
            "ROU": "ROMAN", "RO": "ROMAN",
        }

        for field_key, cfg in auto_fields.items():
            auto_type = cfg["auto"]

            if auto_type == "today":
                data.setdefault(field_key, datetime.now().strftime("%d.%m.%Y"))

            elif auto_type == "expiry_10y":
                # Береться з issue_date або з сьогодні
                issue_str = data.get("issue_date", "")
                try:
                    if '.' in issue_str:
                        parts = issue_str.split('.')
                        issue_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                    else:
                        issue_dt = datetime.now()
                    expiry_dt = issue_dt.replace(year=issue_dt.year + 10)
                    data.setdefault(field_key, expiry_dt.strftime("%d.%m.%Y"))
                except Exception:
                    data.setdefault(field_key, datetime.now().strftime("%d.%m.%Y"))

            elif auto_type == "nationality_from_country":
                cc = data.get("country_code", "").upper().strip()
                nat = _NATIONALITY_MAP.get(cc, cc)
                data.setdefault(field_key, nat)

            elif auto_type == "doc_number_random":
                # Генеруємо номер у форматі C##X##T## (9 символів, як німецький)
                letters = "CFGHJKLMNPRTVWXYZ"
                num = (
                    random.choice(letters)
                    + f"{random.randint(0,9)}{random.randint(0,9)}"
                    + random.choice(letters)
                    + f"{random.randint(0,9)}{random.randint(0,9)}"
                    + random.choice(letters)
                    + f"{random.randint(0,9)}{random.randint(0,9)}"
                )
                data.setdefault(field_key, num)

            elif auto_type == "fixed":
                # Фіксоване значення з default — юзер не вводить
                data.setdefault(field_key, cfg.get("default", ""))

        # ── MRZ (залежить від інших полів, тому рахуємо останнім) ──
        from analysis.mrz_utils import generate_mrz_td3

        has_mrz = any(v.get("auto", "").startswith("mrz_line") for v in auto_fields.values())
        if has_mrz:
            line1, line2 = generate_mrz_td3(
                doc_type=data.get("doc_type", "P"),
                country=data.get("country_code", data.get("country", "")),
                surname=data.get("surname", ""),
                given_name=data.get("given_name", ""),
                doc_number=data.get("doc_number", ""),
                nationality=data.get("nationality", ""),
                birth_date=data.get("birth_date", ""),
                sex=data.get("sex", ""),
                expiry_date=data.get("expiry_date", ""),
            )

        for field_key, cfg in auto_fields.items():
            auto_type = cfg["auto"]
            if auto_type == "mrz_line1":
                data[field_key] = line1
            elif auto_type == "mrz_line2":
                data[field_key] = line2

        return data

    # ── Фото ──────────────────────────────────────────────────────────

    @staticmethod
    def _paste_photo(img: Image.Image, photo_bytes: bytes,
                     x: int, y: int, width: int, height: int) -> None:
        """Вставляє фото у визначену область, масштабуючи під розмір."""
        try:
            photo = Image.open(io.BytesIO(photo_bytes)).convert("RGBA")
            photo = photo.resize((width, height), Image.Resampling.LANCZOS)
            img.paste(photo, (x, y), photo)
        except Exception as e:
            logger.error("Помилка вставки фото: %s", e)

    # ── Публічний API ─────────────────────────────────────────────────

    def render(self, data: dict[str, Any], output_format: str = "PNG") -> bytes:
        """
        Генерує документ.

        Args:
            data: {field_name: value} — str для тексту, bytes для фото
            output_format: "PNG", "JPEG" або "PDF"

        Returns:
            bytes — готове зображення / PDF
        """
        fields_cfg: dict = self.config.get("fields", {})

        # Обчислюємо авто-поля (MRZ тощо)
        data = self._compute_auto_fields(dict(data), fields_cfg)

        img = self._bg_image.copy()
        draw = ImageDraw.Draw(img)

        for field_name, cfg in fields_cfg.items():
            field_type = cfg.get("type", "text")

            # ── Фото ──
            if field_type == "photo":
                photo_data = data.get(field_name)
                if isinstance(photo_data, (bytes, bytearray)):
                    self._paste_photo(
                        img, bytes(photo_data),
                        int(cfg.get("x", 0)), int(cfg.get("y", 0)),
                        int(cfg.get("width", 200)), int(cfg.get("height", 260)),
                    )
                continue

            # ── Текст ──
            text = str(data.get(field_name, ""))
            if not text:
                continue

            x          = int(cfg.get("x", 0))
            y          = int(cfg.get("y", 0))
            font_size  = int(cfg.get("font_size", 16))
            bold       = bool(cfg.get("bold", False))
            color_cfg  = cfg.get("color", [0, 0, 0])[:3]
            align      = str(cfg.get("align", "left"))
            max_width  = int(cfg.get("max_width", 0))
            line_gap   = int(cfg.get("line_gap", 6))
            custom_font = cfg.get("font")  # ← кастомний шрифт для цього поля

            color = tuple(color_cfg) + (255,)
            font  = self._get_font(font_size, bold, custom_font)

            if max_width:
                lines = self._wrap_text(text, font, max_width, draw)
            else:
                lines = [text]

            cur_y = y
            for line in lines:
                self._draw_text_aligned(draw, x, cur_y, line, font, color, align)
                bbox = draw.textbbox((0, 0), line, font=font)
                line_h = bbox[3] - bbox[1]
                cur_y += line_h + line_gap

        # ── Зберігаємо ──
        buf = io.BytesIO()
        fmt = output_format.upper()
        if fmt == "JPEG":
            img.convert("RGB").save(buf, format="JPEG", quality=95)
        elif fmt == "PDF":
            img.convert("RGB").save(buf, format="PDF", resolution=self.config.get("dpi", 150))
        else:
            img.save(buf, format="PNG", optimize=True)
        buf.seek(0)

        logger.info("Документ '%s' згенеровано (%s, %d байт)",
                    self.config.get("name", "?"), fmt, buf.tell())
        return buf.getvalue()

    def preview(self, output_format: str = "PNG") -> bytes:
        """Превью з placeholder-текстом [назва_поля]."""
        placeholders: dict[str, Any] = {}
        for name, cfg in self.config.get("fields", {}).items():
            if cfg.get("auto"):
                continue  # авто-поля пропускаємо — вони обчисляться
            if cfg.get("type") == "photo":
                continue  # фото не вставляємо в превью
            placeholders[name] = f"[{name}]"
        return self.render(placeholders, output_format)

    def get_input_fields(self) -> list[tuple[str, dict]]:
        """Повертає список полів для введення (без auto-полів).

        Returns:
            [(field_key, field_cfg), ...] — тільки ті поля, які юзер заповнює.
        """
        result = []
        for key, cfg in self.config.get("fields", {}).items():
            if cfg.get("auto"):
                continue  # MRZ та інші авто-поля пропускаємо
            result.append((key, cfg))
        return result


# ── Реєстр шаблонів ────────────────────────────────────────────────────

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_registry: dict[str, DocumentGenerator] = {}


def load_all_templates(templates_dir: str | Path = _TEMPLATES_DIR) -> None:
    global _registry
    base = Path(templates_dir)
    if not base.exists():
        logger.warning("Папка templates/ не знайдена: %s", base)
        return
    for folder in base.iterdir():
        if folder.is_dir():
            try:
                gen = DocumentGenerator(folder)
                _registry[folder.name] = gen
                logger.info("Шаблон завантажено: %s", folder.name)
            except FileNotFoundError as e:
                logger.warning("Пропускаємо %s: %s", folder.name, e)


def get_template(name: str) -> DocumentGenerator | None:
    return _registry.get(name)


def list_templates() -> list[str]:
    return list(_registry.keys())
