"""
document_generator.py — Генерація документів із PSD-шаблонів

Підхід (2 файли на шаблон):
  1. background.png  — PSD без текстових шарів (File → Export As → PNG)
  2. config.json     — координати і стилі текстових полів

Структура папок:
  templates/
    invoice/
      background.png
      config.json
    certificate/
      background.png
      config.json

config.json приклад:
  {
    "name": "Рахунок-фактура",
    "dpi": 150,
    "fields": {
      "company_name": {
        "x": 200, "y": 340,
        "font_size": 28, "bold": true,
        "color": [20, 20, 20],
        "align": "left",
        "max_width": 700
      },
      "date": {
        "x": 980, "y": 255,
        "font_size": 18, "bold": false,
        "color": [100, 100, 100],
        "align": "right"
      }
    }
  }

Використання:
  from document_generator import DocumentGenerator

  gen = DocumentGenerator("templates/invoice")
  img_bytes = gen.render({
      "company_name": "Apple Inc.",
      "date": "03.04.2026",
  })
  # img_bytes → відправляємо через bot.send_photo(...)
"""
from __future__ import annotations

import io
import json
import logging
import os
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# ── Пошук системних шрифтів (TTF) ──────────────────────────────────────
_FONT_SEARCH_PATHS = [
    # Windows
    "C:/Windows/Fonts/",
    # Linux
    "/usr/share/fonts/truetype/dejavu/",
    "/usr/share/fonts/truetype/liberation/",
    "/usr/share/fonts/truetype/freefont/",
    # macOS
    "/Library/Fonts/",
    "/System/Library/Fonts/",
]
_FONT_NAMES_REGULAR = ["Arial.ttf", "arial.ttf", "DejaVuSans.ttf",
                        "LiberationSans-Regular.ttf", "FreeSans.ttf"]
_FONT_NAMES_BOLD    = ["Arial Bold.ttf", "arialbd.ttf", "DejaVuSans-Bold.ttf",
                        "LiberationSans-Bold.ttf", "FreeSerifBold.ttf"]


def _find_font(bold: bool = False) -> str | None:
    """Шукає шрифт у системних папках. Повертає шлях або None."""
    candidates = _FONT_NAMES_BOLD if bold else _FONT_NAMES_REGULAR
    for folder in _FONT_SEARCH_PATHS:
        for name in candidates:
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                return path
    return None


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Завантажує TTF-шрифт або повертає стандартний Pillow (bitmap)."""
    path = _find_font(bold)
    if path:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    # fallback — вбудований bitmap шрифт (маленький, без TTF)
    logger.warning("TTF шрифт не знайдено, використовую стандартний bitmap.")
    return ImageFont.load_default()


class DocumentGenerator:
    """
    Генератор документів із PNG-фону + JSON-конфігу.

    Порядок роботи:
      1. Відкрити фоновий PNG (background.png)
      2. Для кожного поля з config.json:
         - завантажити TTF-шрифт з параметрами поля
         - намалювати текст на потрібних координатах
         - підтримка: вирівнювання (left/right/center), перенос слів, макс. ширина
      3. Повернути bytes (PNG) для Telegram

    Args:
        template_dir: шлях до папки шаблону (де лежать background.png і config.json)
        font_path: (опційно) власний TTF-шрифт замість системного
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

        # Кешуємо фон при ініціалізації — уникаємо повторного I/O на кожен render()
        self._bg_image: Image.Image = Image.open(str(bg_path)).convert("RGBA")
        logger.info("DocumentGenerator: шаблон '%s' завантажено (%d полів)",
                    self.config.get("name", template_dir),
                    len(self.config.get("fields", {})))

    # ── Внутрішні хелпери ──────────────────────────────────────────────

    def _get_font(self, size: int, bold: bool) -> ImageFont.FreeTypeFont:
        if self.font_path_override:
            try:
                return ImageFont.truetype(self.font_path_override, size)
            except Exception:
                pass
        return _load_font(size, bold)

    @staticmethod
    def _wrap_text(text: str, font: ImageFont.FreeTypeFont,
                   max_width: int, draw: ImageDraw.ImageDraw) -> list[str]:
        """Розбиває текст на рядки щоб не виходити за max_width пікселів."""
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
        """Малює текст з вирівнюванням left / center / right."""
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        if align == "right":
            x = x - w
        elif align == "center":
            x = x - w // 2
        draw.text((x, y), text, font=font, fill=color)

    # ── Публічний API ──────────────────────────────────────────────────

    def render(self, data: dict, output_format: str = "PNG") -> bytes:
        """
        Генерує документ.

        Args:
            data: {field_name: value} — значення для полів шаблону
            output_format: "PNG" або "JPEG"

        Returns:
            bytes — готове зображення для Telegram
        """
        img = self._bg_image.copy()
        draw = ImageDraw.Draw(img)

        fields: dict = self.config.get("fields", {})

        for field_name, cfg in fields.items():
            text = str(data.get(field_name, ""))
            if not text:
                continue

            x          = int(cfg.get("x", 0))
            y          = int(cfg.get("y", 0))
            font_size  = int(cfg.get("font_size", 16))
            bold       = bool(cfg.get("bold", False))
            color_cfg  = cfg.get("color", [0, 0, 0])[:3]  # гарантуємо RGB, уникаємо 5-tuple
            align      = str(cfg.get("align", "left"))
            max_width  = int(cfg.get("max_width", 0))
            line_gap   = int(cfg.get("line_gap", 6))  # px між рядками

            color = tuple(color_cfg) + (255,)  # RGBA
            font  = self._get_font(font_size, bold)

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

        buf = io.BytesIO()
        fmt = output_format.upper()
        if fmt == "JPEG":
            img.convert("RGB").save(buf, format="JPEG", quality=95)
        else:
            img.save(buf, format="PNG", optimize=True)
        buf.seek(0)

        logger.info("Документ '%s' згенеровано (%d байт)",
                    self.config.get("name", "?"), buf.tell())
        return buf.getvalue()

    def preview(self, output_format: str = "PNG") -> bytes:
        """
        Генерує превью шаблону з placeholder-текстом (для перевірки координат).
        Кожне поле заповнюється своєю назвою: "company_name", "date", ...
        """
        placeholders = {
            name: f"[{name}]"
            for name in self.config.get("fields", {})
        }
        return self.render(placeholders, output_format)


# ── Реєстр шаблонів (за замовчуванням) ────────────────────────────────

_TEMPLATES_DIR = Path("templates")

_registry: dict[str, DocumentGenerator] = {}


def load_all_templates(templates_dir: str | Path = _TEMPLATES_DIR) -> None:
    """Завантажує всі шаблони з папки templates/ у глобальний реєстр."""
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
    """Повертає генератор за назвою або None."""
    return _registry.get(name)


def list_templates() -> list[str]:
    """Повертає список назв доступних шаблонів."""
    return list(_registry.keys())
