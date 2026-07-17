#!/usr/bin/env python3
"""
sort_client_folders.py — сортує папку-з-папками-клієнтів за терміном
дійсності документа.

Структура входу:
    INPUT_DIR/
        1/                  ← папка клієнта
            front.jpg
            back.jpg
            avis-XXX.pdf    ← PDF ігноруються
        2/
            ...
        ...

Що робить:
  1. Для КОЖНОЇ папки прогоняє ВСІ зображення (jpg/jpeg/png/jfif) через
     intl_doc_analyzer.analyze_image_intl
  2. Бере найбільш свіжий exp_date з-поміж усіх фото в папці
  3. Класифікує: valid (exp >= today) / not_valid (exp < today) / unknown
  4. Копіює всю папку у OUTPUT_DIR/<category>/<folder_name>/
  5. Створює _summary.csv з колонками folder, name, exp_date, country,
     doc_type, status, n_photos, source

Запуск:
    python scripts/sort_client_folders.py \\
        "C:/Users/Влад/Downloads/Telegram Desktop/drive-download-..." \\
        "C:/Users/Влад/Desktop/Клоуд аналіз"

Або без аргументів — використає шляхи за замовчуванням з ENV/CONFIG.
"""
from __future__ import annotations

import csv
import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Додаємо корінь проекту до sys.path щоб імпортувати analysis.intl_doc_analyzer
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

TODAY = datetime.now()
IMG_EXTS = {".jpg", ".jpeg", ".png", ".jfif", ".webp"}

# ─── CLI ────────────────────────────────────────────────────────────────

def parse_args():
    if len(sys.argv) >= 3:
        return Path(sys.argv[1]), Path(sys.argv[2])
    # Defaults — для зручності тестування
    default_in = Path(r"C:\Users\Влад\Downloads\Telegram Desktop\drive-download-20260601T095827Z-3-001")
    default_out = Path(r"C:\Users\Влад\Desktop\Клоуд аналіз")
    return default_in, default_out


# ─── Витяг імені людини (з OCR-тексту) ───────────────────────────────────

# Pre-compiled regex для пошуку імені у MRZ форматі: LASTNAME<<FIRSTNAME
_MRZ_NAME_RE = re.compile(r"\b([A-Z]{3,})<<([A-Z<]+)")

def extract_name(text: str) -> str:
    """Намагається витягти "FIRST LAST" з OCR-тексту."""
    if not text:
        return ""

    # 1. MRZ: SURNAME<<FIRSTNAME<
    m = _MRZ_NAME_RE.search(text)
    if m:
        surname = m.group(1)
        first = m.group(2).replace("<", " ").strip()
        return f"{first} {surname}".strip()

    # 2. Шукаємо рядки в верхньому регістрі типу "1 SURNAME / 2 FIRSTNAME"
    for line in text.splitlines():
        line_clean = line.strip()
        # Driver license рядок типу "MARTINEZ\nSTEVEN LEE" або
        # "1. WALSH\n2. CHRISTOPHER PETER"
        m = re.match(r"^\s*1[\.\s]+([A-Z][A-Z\s\-']{2,30})\b", line_clean)
        if m:
            return m.group(1).strip()

    return ""


# ─── Аналіз однієї папки ────────────────────────────────────────────────

def analyze_folder(folder: Path) -> dict:
    """OCR'ить всі фото в папці, повертає найкращий результат.

    Returns: {
      folder, name, exp_date, country, doc_type, n_photos, source, status
    }
    """
    from analysis.intl_doc_analyzer import analyze_image_intl

    images = sorted(
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in IMG_EXTS
    )
    if not images:
        return {
            "folder": folder.name, "name": "", "exp_date": "",
            "country": "", "doc_type": "no_image", "n_photos": 0,
            "source": "", "status": "unknown",
        }

    best = {"exp_date": None, "country": None, "doc_type": None,
            "source": None, "name": ""}

    for img_path in images:
        try:
            img_bytes = img_path.read_bytes()
            res = analyze_image_intl(img_bytes, client_id=folder.name)
        except Exception as e:
            logger.warning("Error analyzing %s: %s", img_path, e)
            continue

        if res.get("exp_date") and not best["exp_date"]:
            best["exp_date"] = res["exp_date"]
            best["source"] = res.get("source", "")
        if res.get("country") and not best["country"]:
            best["country"] = res["country"]
        if res.get("doc_type") and res["doc_type"] != "other" and not best["doc_type"]:
            best["doc_type"] = res["doc_type"]
        if not best["name"]:
            n = extract_name(res.get("ocr_text", ""))
            if n:
                best["name"] = n

    # Класифікація
    if best["exp_date"]:
        try:
            exp = datetime.strptime(best["exp_date"], "%Y-%m-%d")
            status = "valid" if exp >= TODAY else "not_valid"
        except ValueError:
            status = "unknown"
    else:
        status = "unknown"

    return {
        "folder": folder.name,
        "name": best["name"],
        "exp_date": best["exp_date"] or "",
        "country": best["country"] or "",
        "doc_type": best["doc_type"] or "",
        "n_photos": len(images),
        "source": best["source"] or "",
        "status": status,
    }


# ─── MAIN ───────────────────────────────────────────────────────────────

def main():
    in_dir, out_dir = parse_args()
    if not in_dir.is_dir():
        logger.error("Input directory not found: %s", in_dir)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)
    for cat in ("valid", "not_valid", "unknown"):
        (out_dir / cat).mkdir(exist_ok=True)

    client_folders = sorted([d for d in in_dir.iterdir() if d.is_dir()])

    # ── RESUME ──
    # Якщо _summary.csv вже існує — пропускаємо ті папки що там є.
    # Так можна перезапустити після обриву і продовжити з місця останнього збереження.
    summary_path = out_dir / "_summary.csv"
    rows = []
    done_folders: set = set()
    if summary_path.exists():
        try:
            with summary_path.open(encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    rows.append(r)
                    done_folders.add(r["folder"])
            logger.info("Resume mode: %d folders already in summary", len(done_folders))
        except Exception as e:
            logger.warning("Cannot read existing summary: %s", e)

    logger.info("Found %d client folders. Today: %s",
                len(client_folders), TODAY.strftime("%Y-%m-%d"))

    # Записуємо CSV інкрементально — кожні 5 папок
    def _flush_summary():
        with summary_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "folder", "name", "exp_date", "country",
                "doc_type", "n_photos", "source", "status"
            ])
            w.writeheader()
            for r in sorted(rows, key=lambda x: (x["status"], x["folder"])):
                w.writerow(r)

    for i, folder in enumerate(client_folders, 1):
        if folder.name in done_folders:
            continue
        try:
            r = analyze_folder(folder)
            rows.append(r)
            logger.info("[%d/%d] %s → %s exp=%s country=%s",
                        i, len(client_folders), folder.name,
                        r["status"], r["exp_date"] or "—", r["country"] or "—")

            # Copy entire folder to output category
            dest = out_dir / r["status"] / folder.name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(folder, dest)
        except Exception as e:
            logger.exception("Failed processing %s: %s", folder.name, e)
            rows.append({
                "folder": folder.name, "name": "", "exp_date": "",
                "country": "", "doc_type": "error", "n_photos": 0,
                "source": "", "status": "unknown",
            })

        # Інкрементальний save кожні 5 папок (захист від обриву)
        if i % 5 == 0:
            _flush_summary()

    # Final save
    _flush_summary()

    counts = {"valid": 0, "not_valid": 0, "unknown": 0}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    print()
    print("=" * 60)
    print("РЕЗУЛЬТАТ:")
    print(f"  valid:      {counts['valid']:4d}")
    print(f"  not_valid:  {counts['not_valid']:4d}")
    print(f"  unknown:    {counts['unknown']:4d}")
    print(f"  Total:      {sum(counts.values()):4d}")
    print(f"\nSummary: {summary_path}")
    print(f"Folders: {out_dir}")


if __name__ == "__main__":
    main()
