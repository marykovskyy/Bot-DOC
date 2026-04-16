"""
random_person.py — Генерація випадкових персональних даних для документів.

Підтримує: DE (Німеччина), FR (Франція), IT (Італія), PL (Польща), UA (Україна).
Стать визначається з імені. Вік завжди 18+.

Використання:
  from documents.random_person import generate_person
  p = generate_person("DE")
  # {'surname': 'MUELLER', 'given_name': 'HANS', 'birth_date': '15.03.1990',
  #  'sex': 'M', 'birth_place': 'HAMBURG'}
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

# ── База імен по країнах ─────────────────────────────────────────────────
# (ім'я, стать)

_NAMES_DE: dict[str, list[tuple[str, str]]] = {
    "male": [
        ("HANS", "M"), ("KARL", "M"), ("PETER", "M"), ("MICHAEL", "M"),
        ("THOMAS", "M"), ("ANDREAS", "M"), ("STEFAN", "M"), ("MARKUS", "M"),
        ("CHRISTIAN", "M"), ("MARTIN", "M"), ("DANIEL", "M"), ("MATTHIAS", "M"),
        ("FRANK", "M"), ("JUERGEN", "M"), ("WOLFGANG", "M"), ("ALEXANDER", "M"),
        ("TOBIAS", "M"), ("SEBASTIAN", "M"), ("FLORIAN", "M"), ("MAXIMILIAN", "M"),
        ("LUKAS", "M"), ("JONAS", "M"), ("LEON", "M"), ("FELIX", "M"),
        ("NIKLAS", "M"), ("PAUL", "M"), ("TIM", "M"), ("DAVID", "M"),
        ("MORITZ", "M"), ("OLIVER", "M"), ("PHILIPP", "M"), ("BENJAMIN", "M"),
        ("JAN", "M"), ("KLAUS", "M"), ("DIETER", "M"), ("RALF", "M"),
        ("UWE", "M"), ("BERND", "M"), ("HELMUT", "M"), ("GERHARD", "M"),
    ],
    "female": [
        ("ERIKA", "F"), ("ANNA", "F"), ("MARIA", "F"), ("PETRA", "F"),
        ("SABINE", "F"), ("MONIKA", "F"), ("CLAUDIA", "F"), ("SUSANNE", "F"),
        ("ANDREA", "F"), ("NICOLE", "F"), ("KATHARINA", "F"), ("CHRISTINE", "F"),
        ("JULIA", "F"), ("JENNIFER", "F"), ("SARAH", "F"), ("LAURA", "F"),
        ("LISA", "F"), ("LENA", "F"), ("HANNAH", "F"), ("SOPHIE", "F"),
        ("EMMA", "F"), ("MIA", "F"), ("LEONIE", "F"), ("MARIE", "F"),
        ("JOHANNA", "F"), ("CHARLOTTE", "F"), ("FRANZISKA", "F"), ("HEIKE", "F"),
        ("BRIGITTE", "F"), ("RENATE", "F"), ("URSULA", "F"), ("INGRID", "F"),
        ("GISELA", "F"), ("HELGA", "F"), ("ELKE", "F"), ("GABRIELE", "F"),
        ("BIRGIT", "F"), ("KARIN", "F"), ("ANGELIKA", "F"), ("SILKE", "F"),
    ],
}

_SURNAMES_DE = [
    "MUELLER", "SCHMIDT", "SCHNEIDER", "FISCHER", "WEBER", "MEYER", "WAGNER",
    "BECKER", "SCHULZ", "HOFFMANN", "SCHAEFER", "KOCH", "BAUER", "RICHTER",
    "KLEIN", "WOLF", "SCHROEDER", "NEUMANN", "SCHWARZ", "ZIMMERMANN",
    "BRAUN", "KRUEGER", "HOFMANN", "HARTMANN", "LANGE", "SCHMITT", "WERNER",
    "SCHMITZ", "KRAUSE", "MEIER", "LEHMANN", "SCHMID", "SCHULZE", "MAIER",
    "KOEHLER", "HERRMANN", "KOENIG", "WALTER", "MAYER", "HUBER", "KAISER",
    "FUCHS", "PETERS", "LANG", "SCHOLZ", "MOELLER", "WEISS", "JUNG",
    "HAHN", "SCHUBERT", "VOGEL", "FRIEDRICH", "KELLER", "GUENTHER",
    "FRANK", "BERGER", "WINKLER", "ROTH", "BECK", "LORENZ", "BAUMANN",
    "FRANKE", "ALBRECHT", "SCHUSTER", "SIMON", "LUDWIG", "BOEHM", "WINTER",
    "KRAUS", "MARTIN", "SCHUMACHER", "KROEGER", "SCHREIBER", "OTTO",
]

_CITIES_DE = [
    "BERLIN", "HAMBURG", "MUENCHEN", "KOELN", "FRANKFURT AM MAIN",
    "STUTTGART", "DUESSELDORF", "LEIPZIG", "DORTMUND", "ESSEN",
    "BREMEN", "DRESDEN", "HANNOVER", "NUERNBERG", "DUISBURG",
    "BOCHUM", "WUPPERTAL", "BIELEFELD", "BONN", "MUENSTER",
    "MANNHEIM", "KARLSRUHE", "AUGSBURG", "WIESBADEN", "AACHEN",
    "BRAUNSCHWEIG", "KIEL", "CHEMNITZ", "MAGDEBURG", "FREIBURG",
    "LUEBECK", "ERFURT", "ROSTOCK", "MAINZ", "KASSEL",
    "HALLE", "SAARBRUECKEN", "POTSDAM", "LUDWIGSHAFEN", "OLDENBURG",
]

# ── Французькі імена ──
_NAMES_FR: dict[str, list[tuple[str, str]]] = {
    "male": [
        ("JEAN", "M"), ("PIERRE", "M"), ("MICHEL", "M"), ("JACQUES", "M"),
        ("PHILIPPE", "M"), ("NICOLAS", "M"), ("FRANCOIS", "M"), ("LAURENT", "M"),
        ("CHRISTOPHE", "M"), ("THOMAS", "M"), ("ALEXANDRE", "M"), ("ANTOINE", "M"),
        ("LOUIS", "M"), ("GABRIEL", "M"), ("HUGO", "M"), ("LUCAS", "M"),
        ("ARTHUR", "M"), ("RAPHAEL", "M"), ("JULES", "M"), ("ADAM", "M"),
    ],
    "female": [
        ("MARIE", "F"), ("JEANNE", "F"), ("CATHERINE", "F"), ("ISABELLE", "F"),
        ("SOPHIE", "F"), ("NATHALIE", "F"), ("CLAIRE", "F"), ("CHARLOTTE", "F"),
        ("EMMA", "F"), ("JADE", "F"), ("LOUISE", "F"), ("ALICE", "F"),
        ("CHLOE", "F"), ("CAMILLE", "F"), ("MANON", "F"), ("JULIETTE", "F"),
        ("AMELIE", "F"), ("AURELIE", "F"), ("VALERIE", "F"), ("MONIQUE", "F"),
    ],
}

_SURNAMES_FR = [
    "MARTIN", "BERNARD", "THOMAS", "PETIT", "ROBERT", "RICHARD", "DURAND",
    "DUBOIS", "MOREAU", "LAURENT", "SIMON", "MICHEL", "LEFEVRE", "LEROY",
    "ROUX", "DAVID", "BERTRAND", "MOREL", "FOURNIER", "GIRARD",
    "BONNET", "DUPONT", "LAMBERT", "FONTAINE", "ROUSSEAU", "VINCENT",
    "MULLER", "LEFEVRE", "FAURE", "ANDRE", "MERCIER", "BLANC",
]

_CITIES_FR = [
    "PARIS", "MARSEILLE", "LYON", "TOULOUSE", "NICE", "NANTES",
    "STRASBOURG", "MONTPELLIER", "BORDEAUX", "LILLE", "RENNES",
    "REIMS", "SAINT ETIENNE", "TOULON", "GRENOBLE", "DIJON", "ANGERS",
]

# ── Італійські імена ──
_NAMES_IT: dict[str, list[tuple[str, str]]] = {
    "male": [
        ("MARCO", "M"), ("ANDREA", "M"), ("GIUSEPPE", "M"), ("LUCA", "M"),
        ("GIOVANNI", "M"), ("FRANCESCO", "M"), ("ALESSANDRO", "M"), ("MATTEO", "M"),
        ("LORENZO", "M"), ("SIMONE", "M"), ("DAVIDE", "M"), ("FEDERICO", "M"),
        ("ROBERTO", "M"), ("ANTONIO", "M"), ("PAOLO", "M"), ("RICCARDO", "M"),
    ],
    "female": [
        ("MARIA", "F"), ("ANNA", "F"), ("GIULIA", "F"), ("FRANCESCA", "F"),
        ("VALENTINA", "F"), ("CHIARA", "F"), ("SARA", "F"), ("ELENA", "F"),
        ("ALESSIA", "F"), ("LAURA", "F"), ("SOFIA", "F"), ("AURORA", "F"),
        ("GINEVRA", "F"), ("BEATRICE", "F"), ("ALICE", "F"), ("MARTINA", "F"),
    ],
}

_SURNAMES_IT = [
    "ROSSI", "RUSSO", "FERRARI", "ESPOSITO", "BIANCHI", "ROMANO", "COLOMBO",
    "RICCI", "MARINO", "GRECO", "BRUNO", "GALLO", "CONTI", "COSTA",
    "GIORDANO", "MANCINI", "RIZZO", "LOMBARDI", "MORETTI", "BARBIERI",
]

_CITIES_IT = [
    "ROMA", "MILANO", "NAPOLI", "TORINO", "PALERMO", "GENOVA", "BOLOGNA",
    "FIRENZE", "CATANIA", "BARI", "VENEZIA", "VERONA", "MESSINA", "PADOVA",
]

# ── Польські імена ──
_NAMES_PL: dict[str, list[tuple[str, str]]] = {
    "male": [
        ("ADAM", "M"), ("PIOTR", "M"), ("KRZYSZTOF", "M"), ("TOMASZ", "M"),
        ("MARCIN", "M"), ("MICHAL", "M"), ("JAKUB", "M"), ("MATEUSZ", "M"),
        ("LUKASZ", "M"), ("PAWEL", "M"), ("WOJCIECH", "M"), ("ROBERT", "M"),
        ("KAMIL", "M"), ("RAFAL", "M"), ("DAWID", "M"), ("DOMINIK", "M"),
    ],
    "female": [
        ("ANNA", "F"), ("MARIA", "F"), ("KATARZYNA", "F"), ("AGNIESZKA", "F"),
        ("MAGDALENA", "F"), ("MONIKA", "F"), ("NATALIA", "F"), ("JOANNA", "F"),
        ("KAROLINA", "F"), ("ALEKSANDRA", "F"), ("JULIA", "F"), ("ZUZANNA", "F"),
        ("MAJA", "F"), ("ZOFIA", "F"), ("HANNA", "F"), ("WIKTORIA", "F"),
    ],
}

_SURNAMES_PL = [
    "NOWAK", "KOWALSKI", "WISNIEWSKI", "WOJCIK", "KOWALCZYK", "KAMINSKI",
    "LEWANDOWSKI", "ZIELINSKI", "SZYMANSKI", "WOZNIAK", "DABROWSKI",
    "KOZLOWSKI", "JANKOWSKI", "MAZUR", "KWIATKOWSKI", "KRAWCZYK",
    "PIOTROWSKI", "GRABOWSKI", "NOWAKOWSKI", "PAWLOWSKI", "MICHALSKI",
]

_CITIES_PL = [
    "WARSZAWA", "KRAKOW", "LODZ", "WROCLAW", "POZNAN", "GDANSK",
    "SZCZECIN", "BYDGOSZCZ", "LUBLIN", "BIALYSTOK", "KATOWICE", "TORUN",
]

# ── Об'єднаний реєстр ────────────────────────────────────────────────────

_COUNTRY_DATA: dict[str, dict] = {
    "DE": {"names": _NAMES_DE, "surnames": _SURNAMES_DE, "cities": _CITIES_DE},
    "D":  {"names": _NAMES_DE, "surnames": _SURNAMES_DE, "cities": _CITIES_DE},
    "FR": {"names": _NAMES_FR, "surnames": _SURNAMES_FR, "cities": _CITIES_FR},
    "F":  {"names": _NAMES_FR, "surnames": _SURNAMES_FR, "cities": _CITIES_FR},
    "IT": {"names": _NAMES_IT, "surnames": _SURNAMES_IT, "cities": _CITIES_IT},
    "I":  {"names": _NAMES_IT, "surnames": _SURNAMES_IT, "cities": _CITIES_IT},
    "PL": {"names": _NAMES_PL, "surnames": _SURNAMES_PL, "cities": _CITIES_PL},
}

# Fallback — німецькі
_DEFAULT_COUNTRY = "DE"


def _random_birth_date(min_age: int = 18, max_age: int = 65) -> str:
    """Генерує випадкову дату народження у форматі DD.MM.YYYY. Вік: 18-65."""
    today = datetime.now()
    min_date = today - timedelta(days=max_age * 365)
    max_date = today - timedelta(days=min_age * 365 + 1)
    delta = (max_date - min_date).days
    random_date = min_date + timedelta(days=random.randint(0, max(delta, 1)))
    return random_date.strftime("%d.%m.%Y")


def generate_person(country_code: str = "DE") -> dict[str, str]:
    """Генерує випадкову особу для заданої країни.

    Returns:
        {'surname': ..., 'given_name': ..., 'birth_date': ..., 'sex': ..., 'birth_place': ...}
    """
    cc = country_code.upper().strip()
    data = _COUNTRY_DATA.get(cc, _COUNTRY_DATA[_DEFAULT_COUNTRY])

    names_db = data["names"]
    surnames = data["surnames"]
    cities = data["cities"]

    # Випадкова стать
    sex = random.choice(["M", "F"])
    pool = names_db["male"] if sex == "M" else names_db["female"]

    given_name, _ = random.choice(pool)
    surname = random.choice(surnames)
    birth_date = _random_birth_date()
    birth_place = random.choice(cities)

    return {
        "surname": surname,
        "given_name": given_name,
        "birth_date": birth_date,
        "sex": sex,
        "birth_place": birth_place,
    }


def get_supported_countries() -> list[str]:
    """Повертає список підтримуваних кодів країн."""
    return sorted(set(k for k in _COUNTRY_DATA if len(k) == 2))
