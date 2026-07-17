"""
pdf_convert.py — Конвертація .docx → .pdf через LibreOffice (headless).

MS Word на цільовій машині немає, тож рендер у PDF робимо через LibreOffice
`soffice` у headless-режимі. Особливості, що визначають реалізацію:

  • Одна команда `soffice --convert-to pdf` конвертує ОДРАЗУ пакет файлів —
    це амортизує ~5 c холодного старту LibreOffice на весь пакет.
  • Кожен виклик отримує ВЛАСНИЙ тимчасовий профіль (-env:UserInstallation),
    інакше паралельні конвертації (різні користувачі бота) конфліктують за
    блокування профілю, і команда зависає. Разом із --norestore/--nolockcheck
    це усуває характерне «підвисання» soffice.
  • Довгі пакети ріжемо на чанки, щоб не впертись у ліміт довжини команди
    Windows (~32 KB). Чанки йдуть послідовно з тим самим профілем (теплий старт).

Публічний API:
  pdf_available()                  -> bool           — чи знайдено soffice
  convert_one(docx_bytes)          -> bytes          — один документ
  convert_docx_to_pdf([(name,b)])  -> [(name.pdf,b)] — пакет (зберігає порядок)

Шлях до soffice можна задати змінною середовища LIBREOFFICE_PATH (або
SOFFICE_PATH); інакше шукаємо в PATH і стандартних місцях встановлення.
"""
from __future__ import annotations

import logging
import os
import platform
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class PdfConversionError(RuntimeError):
    """LibreOffice недоступний або конвертація не вдалася."""


# ── Пошук soffice ────────────────────────────────────────────────────────

_SOFFICE_CMD: str | None = None

_WINDOWS_SOFFICE_PATHS = (
    r"C:\Program Files\LibreOffice\program\soffice.exe",
    r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
)
_POSIX_SOFFICE_PATHS = (
    "/usr/bin/soffice",
    "/usr/local/bin/soffice",
    "/opt/libreoffice/program/soffice",
    "/Applications/LibreOffice.app/Contents/MacOS/soffice",
)


def _find_soffice() -> str | None:
    """Знаходить виконуваний файл LibreOffice. Кешує результат.

    Порядок: LIBREOFFICE_PATH/SOFFICE_PATH → PATH → стандартні шляхи ОС.
    Порожній рядок у кеші = «шукали, не знайшли» (щоб не шукати повторно).
    """
    global _SOFFICE_CMD
    if _SOFFICE_CMD is not None:
        return _SOFFICE_CMD or None

    env = (os.getenv("LIBREOFFICE_PATH") or os.getenv("SOFFICE_PATH") or "").strip()
    if env and os.path.isfile(env):
        _SOFFICE_CMD = env
        return env

    for name in ("soffice", "soffice.exe", "libreoffice"):
        found = shutil.which(name)
        if found:
            _SOFFICE_CMD = found
            return found

    candidates = _WINDOWS_SOFFICE_PATHS if platform.system() == "Windows" else _POSIX_SOFFICE_PATHS
    for cand in candidates:
        if os.path.isfile(cand):
            _SOFFICE_CMD = cand
            return cand

    _SOFFICE_CMD = ""  # не знайдено
    return None


def pdf_available() -> bool:
    """True, якщо LibreOffice знайдено і конвертація можлива."""
    return bool(_find_soffice())


# ── Конвертація ──────────────────────────────────────────────────────────

# Скільки файлів максимум передаємо одній команді soffice (запас від ліміту
# довжини командного рядка Windows ~32 KB; шлях у temp короткий).
_CHUNK_SIZE = 40


def _pdf_name(docx_name: str) -> str:
    """'001_FOO.docx' → '001_FOO.pdf' (розширення .docx замінюється на .pdf)."""
    return re.sub(r"\.docx$", "", docx_name, flags=re.IGNORECASE) + ".pdf"


def _run_soffice(soffice: str, work_dir: str, profile_dir: str,
                 files: list[str], timeout: float) -> None:
    """Один запуск soffice: конвертує перелічені .docx у PDF в той самий каталог."""
    profile_url = "file:///" + os.path.abspath(profile_dir).replace("\\", "/")
    cmd = [
        soffice, "--headless", "--norestore", "--nolockcheck", "--nodefault",
        f"-env:UserInstallation={profile_url}",
        "--convert-to", "pdf", "--outdir", work_dir, *files,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise PdfConversionError(
            f"LibreOffice не встиг конвертувати за {timeout:.0f} c") from e
    except OSError as e:
        raise PdfConversionError(f"Не вдалося запустити LibreOffice: {e}") from e
    if proc.returncode != 0:
        err = (proc.stderr or b"").decode("utf-8", "replace").strip()[:500]
        raise PdfConversionError(f"LibreOffice повернув код {proc.returncode}: {err}")


def convert_docx_to_pdf(items: list[tuple[str, bytes]],
                        timeout_per_chunk: float | None = None) -> list[tuple[str, bytes]]:
    """Конвертує пакет .docx → .pdf. Повертає [(name.pdf, pdf_bytes), ...].

    Порядок зберігається. Файли, які LibreOffice не зміг конвертувати,
    пропускаються з попередженням (решта пакета все одно повертається).

    Викликає PdfConversionError, якщо LibreOffice не знайдено або запуск не вдався.
    """
    soffice = _find_soffice()
    if not soffice:
        raise PdfConversionError(
            "LibreOffice не знайдено. Встановіть LibreOffice або задайте LIBREOFFICE_PATH.")
    if not items:
        return []

    results: list[tuple[str, bytes]] = []
    with tempfile.TemporaryDirectory(prefix="cert_docx_") as work, \
         tempfile.TemporaryDirectory(prefix="lo_profile_") as profile:
        # Пишемо вхідні файли під короткими унікальними іменами (індекс),
        # щоб уникнути колізій (різні компанії → однакова назва) і зберегти
        # відповідність вхід↔вихід.
        stems: list[str] = []
        for i, (_, data) in enumerate(items):
            stem = f"{i:05d}"
            Path(work, stem + ".docx").write_bytes(data)
            stems.append(stem)

        for start in range(0, len(stems), _CHUNK_SIZE):
            chunk = stems[start:start + _CHUNK_SIZE]
            files = [str(Path(work, s + ".docx")) for s in chunk]
            timeout = timeout_per_chunk or max(120.0, 8.0 * len(chunk))
            _run_soffice(soffice, work, profile, files, timeout)

        for stem, (name, _) in zip(stems, items):
            pdf_path = Path(work, stem + ".pdf")
            if not pdf_path.exists():
                logger.warning("PDF не створено для '%s' (пропущено)", name)
                continue
            results.append((_pdf_name(name), pdf_path.read_bytes()))

    if not results:
        raise PdfConversionError("Жоден документ не вдалося конвертувати у PDF.")
    return results


def convert_one(docx_bytes: bytes, timeout: float = 120.0) -> bytes:
    """Конвертує один .docx → .pdf (байти). Зручно для прев'ю одного документа."""
    out = convert_docx_to_pdf([("document.docx", docx_bytes)], timeout_per_chunk=timeout)
    return out[0][1]
