"""
mrz_utils.py — Генерація Machine Readable Zone (MRZ) для паспортів TD3

TD3 формат (паспорт): 2 рядки по 44 символи
  Рядок 1: P<COUNTRY<<SURNAME<<GIVEN<NAMES<<<<<<<<<<<<<<<<<<
  Рядок 2: DOCNUM<CHECK NAT BDATE CHECK SEX EDATE CHECK OPT<<<<<< CHECK COMP

Алгоритм контрольних цифр: ICAO 9303, ваги 7-3-1.
"""
from __future__ import annotations


def _char_value(ch: str) -> int:
    if ch == '<':
        return 0
    if ch.isdigit():
        return int(ch)
    if ch.isalpha():
        return ord(ch.upper()) - ord('A') + 10
    return 0


def _check_digit(data: str) -> str:
    weights = [7, 3, 1]
    total = sum(_char_value(ch) * weights[i % 3] for i, ch in enumerate(data))
    return str(total % 10)


def _pad(text: str, length: int) -> str:
    text = text.upper().replace(' ', '<')
    return (text + '<' * length)[:length]


def _clean(name: str) -> str:
    result = name.upper().replace(' ', '<')
    return ''.join(c for c in result if c.isalpha() or c == '<')


def _date_to_mrz(date_str: str) -> str:
    """DD.MM.YYYY або YYYY-MM-DD → YYMMDD."""
    date_str = date_str.strip()
    if '.' in date_str:
        parts = date_str.split('.')
        if len(parts) == 3:
            dd, mm, yyyy = parts
            return yyyy[-2:] + mm.zfill(2) + dd.zfill(2)
    elif '-' in date_str:
        parts = date_str.split('-')
        if len(parts) == 3:
            yyyy, mm, dd = parts
            return yyyy[-2:] + mm.zfill(2) + dd.zfill(2)
    # Якщо вже YYMMDD
    return date_str[:6] if len(date_str) >= 6 else _pad(date_str, 6)


def generate_mrz_td3(
    doc_type: str = "P",
    country: str = "D",
    surname: str = "",
    given_name: str = "",
    doc_number: str = "",
    nationality: str = "",
    birth_date: str = "",
    sex: str = "",
    expiry_date: str = "",
    optional: str = "",
) -> tuple[str, str]:
    """
    Генерує 2 рядки MRZ для паспорта (TD3, ICAO 9303).

    Returns:
        (line1, line2) — два рядки по 44 символи
    """
    # ── Рядок 1: тип + країна + ім'я ──
    dt = _pad(_clean(doc_type), 2)
    cc = _pad(_clean(country), 3)
    names = _clean(surname) + "<<" + _clean(given_name)
    names = _pad(names, 39)
    line1 = dt + cc + names

    # ── Рядок 2: номер + дати + контрольні цифри ──
    dn = _pad(doc_number.upper().replace(' ', ''), 9)
    dn_check = _check_digit(dn)

    nat = _pad(_clean(nationality), 3)

    bd = _pad(_date_to_mrz(birth_date) if birth_date else "000000", 6)
    bd_check = _check_digit(bd)

    sx = sex.upper() if sex.upper() in ('M', 'F') else '<'

    ed = _pad(_date_to_mrz(expiry_date) if expiry_date else "000000", 6)
    ed_check = _check_digit(ed)

    opt = _pad(optional.upper(), 14)
    opt_check = _check_digit(opt)

    composite_data = dn + dn_check + bd + bd_check + ed + ed_check + opt + opt_check
    composite_check = _check_digit(composite_data)

    line2 = dn + dn_check + nat + bd + bd_check + sx + ed + ed_check + opt + opt_check + composite_check

    # Гарантуємо довжину 44
    line1 = _pad(line1, 44)[:44]
    line2 = _pad(line2, 44)[:44]

    return line1, line2
