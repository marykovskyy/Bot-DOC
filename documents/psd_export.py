"""
psd_export.py — Утиліта для підготовки PSD шаблонів

Запускати ОДИН РАЗ для кожного нового PSD-шаблону.

Що робить:
  1. Відкриває PSD
  2. Аналізує всі шари і виводить таблицю
  3. Приховує текстові шари → експортує background.png
  4. Генерує config.json з координатами текстових полів

Використання:
  python psd_export.py germany3.psd --out templates/germany_passport
  python psd_export.py myfile.psd --out templates/my_template --dpi 150
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def analyze_and_export(psd_path: str, out_dir: str, dpi: int = 150) -> None:
    try:
        from psd_tools import PSDImage
    except ImportError:
        print("❌ Встановіть psd-tools: pip install psd-tools")
        sys.exit(1)

    psd = PSDImage.open(psd_path)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"\n📄 Файл: {psd_path}")
    print(f"📐 Розмір: {psd.width} × {psd.height} px")
    print(f"📦 Шарів всього: {len(list(psd.descendants()))}\n")

    # ── Збираємо інформацію по всіх шарах ──
    text_layers = []
    pixel_layers = []

    for layer in psd.descendants():
        kind = str(getattr(layer, 'kind', ''))
        bbox = getattr(layer, 'bbox', None)
        name = getattr(layer, 'name', '')
        visible = getattr(layer, 'visible', True)

        is_text = 'TYPE' in kind.upper() or kind == '2'

        if not bbox:
            continue

        # В psd-tools bbox може бути об'єктом або кортежем (left, top, right, bottom)
        if hasattr(bbox, 'left'):
            x, y, right, bottom = bbox.left, bbox.top, bbox.right, bbox.bottom
        else:
            x, y, right, bottom = bbox[0], bbox[1], bbox[2], bbox[3]
        w = right - x
        h = bottom - y

        if is_text:
            # Витягуємо текст і розмір шрифту
            text_content = ""
            font_size_pt = 16
            font_name = ""
            try:
                ed = layer.engine_data
                if ed:
                    txt = ed.get('EngineDict', {}).get('Editor', {}).get('Text', {})
                    text_content = str(txt.get('Txt', '')).strip()
                    runs = ed.get('EngineDict', {}).get('StyleRun', {}).get('RunArray', [])
                    if runs:
                        style = runs[0].get('RunData', {}).get('StyleSheet', {}).get('StyleSheetData', {})
                        raw_size = style.get('FontSize', 0)
                        # PSD зберігає px при 72 ppi → конвертуємо до цільового dpi
                        font_size_pt = max(8, int(round(float(raw_size) * dpi / 72)))
                        font_obj = style.get('Font', '')
                        if isinstance(font_obj, dict):
                            font_name = font_obj.get('Name', '')
                        else:
                            font_name = str(font_obj)
            except Exception:
                pass

            # Вертикальний текст: ширина << висоти
            is_vertical = h > w * 2 if w > 0 else False

            text_layers.append({
                'layer_name': name,
                'text': text_content,
                'x': x, 'y': y,
                'width': w, 'height': h,
                'font_size_est': font_size_pt,
                'font_name': font_name,
                'visible': visible,
                'is_vertical': is_vertical,
            })
        else:
            pixel_layers.append({'name': name, 'visible': visible})

    # ── Виводимо таблицю текстових шарів ──
    print("✍️  Текстові шари:")
    print(f"{'#':<3} {'Шар':<40} {'Текст':<35} {'x':>5} {'y':>5} {'w':>5} {'h':>5}  {'Верт?'}")
    print("-" * 110)
    for i, tl in enumerate(text_layers):
        vert = "↕" if tl['is_vertical'] else ""
        print(f"{i:<3} {tl['layer_name'][:39]:<40} {repr(tl['text'])[:34]:<35} "
              f"{tl['x']:>5} {tl['y']:>5} {tl['width']:>5} {tl['height']:>5}  {vert}")

    # ── Приховуємо текстові шари → рендеримо фон ──
    print("\n🎨 Рендеримо фоновий PNG (без текстових шарів)...")

    def _hide_text_layers(parent):
        for layer in parent:
            k = str(getattr(layer, 'kind', ''))
            if 'TYPE' in k.upper() or k == '2':
                layer.visible = False
            elif hasattr(layer, '__iter__'):
                _hide_text_layers(layer)

    _hide_text_layers(psd)

    bg_path = out / "background.png"
    try:
        bg_img = psd.composite()
    except Exception:
        bg_img = None
    if bg_img:
        bg_img.save(str(bg_path))
        print(f"✅ background.png збережено: {bg_path}")
    else:
        print("⚠️ composite() повернув None — спробую PIL merge...")
        from PIL import Image
        merged = Image.new("RGBA", (psd.width, psd.height), (255, 255, 255, 255))
        for layer in psd:
            k = str(getattr(layer, 'kind', ''))
            if 'TYPE' in k.upper() or k == '2':
                continue
            if not getattr(layer, 'visible', True):
                continue
            try:
                layer_img = layer.composite()
                if layer_img:
                    merged.paste(layer_img, (layer.left, layer.top), layer_img)
            except Exception:
                pass
        merged.save(str(bg_path))
        print(f"✅ background.png (manual merge): {bg_path}")

    # ── Генеруємо config.json ──
    # Фільтруємо: пропускаємо вертикальні шари і службові
    skip_names = {'Layer 1', 'Слой 2', 'Слой 2 копия'}

    fields = {}
    for tl in text_layers:
        if tl['is_vertical']:
            print(f"  ⏩ Пропускаємо вертикальний шар: '{tl['layer_name']}'")
            continue
        if tl['layer_name'] in skip_names:
            continue
        if not tl['text'] and tl['layer_name'] == 'Layer 1':
            continue

        # Використовуємо назву шару як ключ поля (очищена)
        field_key = tl['layer_name'].lower().replace(' ', '_').strip('_')
        field_key = ''.join(c for c in field_key if c.isalnum() or c == '_')

        # Уникаємо дублікатів ключів
        base_key = field_key
        counter = 2
        while field_key in fields:
            field_key = f"{base_key}_{counter}"
            counter += 1

        fields[field_key] = {
            "label":      tl['layer_name'],
            "default":    tl['text'],
            "x":          tl['x'],
            "y":          tl['y'],
            "font_size":  tl['font_size_est'],
            "bold":       False,
            "color":      [0, 0, 0],
            "align":      "left",
            "max_width":  tl['width'],
        }

    config = {
        "name":        Path(psd_path).stem,
        "description": f"Шаблон: {Path(psd_path).stem}",
        "canvas_w":    psd.width,
        "canvas_h":    psd.height,
        "dpi":         dpi,
        "fields":      fields,
    }

    cfg_path = out / "config.json"
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"✅ config.json збережено: {cfg_path}")
    print(f"\n📋 Полів у шаблоні: {len(fields)}")
    for k, v in fields.items():
        print(f"  • {k:30} (default: {repr(v['default'])[:30]})")

    print(f"\n🚀 Шаблон готовий: {out_dir}")
    print("   Відредагуй config.json якщо потрібно змінити font_size, color, align")
    print("   Потім перезапусти бота — шаблон підхопиться автоматично.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Експорт PSD шаблону для document_generator")
    parser.add_argument("psd", help="Шлях до PSD файлу")
    parser.add_argument("--out", default=None, help="Папка виводу (за замовчуванням: templates/<ім'я файлу>)")
    parser.add_argument("--dpi", type=int, default=150, help="DPI для розрахунку розміру шрифту (150)")
    args = parser.parse_args()

    out_dir = args.out or os.path.join("templates", Path(args.psd).stem)
    analyze_and_export(args.psd, out_dir, args.dpi)
