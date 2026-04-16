"""
translit.py — ICAO 9303 транслітерація кирилиці → латиниця.

Підтримує: українську, російську, німецьку умлаути.
Результат завжди UPPER CASE — як у паспортних документах.

Використання:
  from documents.translit import to_latin
  to_latin("Мустерманн")  → "MUSTERMANN"
  to_latin("Шевченко")    → "SHEVCHENKO"
  to_latin("Müller")      → "MUELLER"
"""
from __future__ import annotations

# ── ICAO 9303 Doc 9303 — таблиця транслітерації ──────────────────────────
# Українська (ДСТУ 9112:2021 / ICAO)
_UA_MAP: dict[str, str] = {
    'А': 'A',  'Б': 'B',  'В': 'V',  'Г': 'H',  'Ґ': 'G',
    'Д': 'D',  'Е': 'E',  'Є': 'IE', 'Ж': 'ZH', 'З': 'Z',
    'И': 'Y',  'І': 'I',  'Ї': 'I',  'Й': 'I',  'К': 'K',
    'Л': 'L',  'М': 'M',  'Н': 'N',  'О': 'O',  'П': 'P',
    'Р': 'R',  'С': 'S',  'Т': 'T',  'У': 'U',  'Ф': 'F',
    'Х': 'KH', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SHCH',
    'Ь': '',   'Ю': 'IU', 'Я': 'IA',
}

# Ці літери на ПОЧАТКУ слова мають "Y"-prefix замість "I"
_UA_WORD_START: dict[str, str] = {
    'Є': 'YE', 'Ї': 'YI', 'Й': 'Y', 'Ю': 'YU', 'Я': 'YA',
}

# Російські літери (яких немає в українській)
_RU_EXTRA: dict[str, str] = {
    'Ё': 'E',  'Ы': 'Y',  'Э': 'E',  'Ъ': 'IE',
}

# Німецькі умлаути та ß
_DE_MAP: dict[str, str] = {
    'Ä': 'AE', 'Ö': 'OE', 'Ü': 'UE', 'ß': 'SS',
}

# Об'єднана таблиця (upper → upper)
_FULL_MAP: dict[str, str] = {}
_FULL_MAP.update(_UA_MAP)
_FULL_MAP.update(_RU_EXTRA)
_FULL_MAP.update({k.upper(): v for k, v in _DE_MAP.items()})

# Додаємо lower → upper (щоб працювало з будь-яким регістром)
_LOWER_MAP: dict[str, str] = {}
for k, v in list(_FULL_MAP.items()):
    _LOWER_MAP[k.lower()] = v
_FULL_MAP.update(_LOWER_MAP)
# Окремо для ß (не має upper-форми)
_FULL_MAP['ß'] = 'SS'


def is_latin(text: str) -> bool:
    """Перевіряє чи текст вже повністю латиницею."""
    for ch in text:
        if ch.isalpha() and ch.upper() not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            return False
    return True


def to_latin(text: str) -> str:
    """Транслітерує текст → латиниця UPPER CASE.

    Якщо текст вже латиницею — просто переводить в UPPER.
    Змішаний текст обробляється посимвольно.
    Контекстні правила:
      - "Й" на початку слова → "Y", інакше → "I"
      - "Я/Ю/Є/Ї" на початку слова мають "Y" prefix
    """
    if is_latin(text):
        return text.upper()

    result: list[str] = []
    chars = list(text)
    for i, ch in enumerate(chars):
        upper_ch = ch.upper()
        # Контекст: чи це початок слова?
        is_word_start = (i == 0) or not chars[i - 1].isalpha()

        # Контекстні правила: Й, Я, Ю, Є, Ї — на початку слова "Y", інакше "I"
        if upper_ch in _UA_WORD_START:
            if is_word_start:
                result.append(_UA_WORD_START[upper_ch])
            elif ch in _FULL_MAP:
                result.append(_FULL_MAP[ch])
            else:
                result.append(_FULL_MAP.get(upper_ch, upper_ch))
        elif ch in _FULL_MAP:
            result.append(_FULL_MAP[ch])
        elif ch.isalpha():
            result.append(ch.upper())
        else:
            result.append(ch)

    return ''.join(result).upper()


def transliterate_if_needed(text: str) -> tuple[str, bool]:
    """Транслітерує якщо є кирилиця. Повертає (результат, чи_була_транслітерація).

    Зручно для UI — показати юзеру що відбулась заміна.
    """
    if is_latin(text):
        return text.upper(), False
    return to_latin(text), True
