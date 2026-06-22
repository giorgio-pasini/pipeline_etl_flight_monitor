"""
Données de référence statiques pour l'enrichissement Silver/Gold.

Évite les placeholders codés en dur :
- COUNTRY_TO_CONTINENT : ISO 3166-1 alpha-2 -> code continent
- AIRCRAFT_PREFIX_TO_MANUFACTURER : préfixe de code ICAO d'avion -> constructeur

Fournit aussi des helpers pour construire des expressions Spark (map lookups)
utilisables directement dans les transformations.
"""

from pyspark.sql import Column
from pyspark.sql.functions import (
    col, create_map, lit, coalesce, upper, lower, substring, regexp_replace,
)
from itertools import chain

# ============================================================================
# Pays (ISO alpha-2) -> continent
# Couverture des principaux pays ; les non-listés tombent sur "UNKNOWN".
# Codes continent : AF, AN, AS, EU, NA, OC, SA
# ============================================================================

COUNTRY_TO_CONTINENT = {
    # Europe
    "AL": "EU", "AD": "EU", "AT": "EU", "BY": "EU", "BE": "EU", "BA": "EU",
    "BG": "EU", "HR": "EU", "CY": "EU", "CZ": "EU", "DK": "EU", "EE": "EU",
    "FI": "EU", "FR": "EU", "DE": "EU", "GR": "EU", "HU": "EU", "IS": "EU",
    "IE": "EU", "IT": "EU", "LV": "EU", "LI": "EU", "LT": "EU", "LU": "EU",
    "MT": "EU", "MD": "EU", "MC": "EU", "ME": "EU", "NL": "EU", "MK": "EU",
    "NO": "EU", "PL": "EU", "PT": "EU", "RO": "EU", "RU": "EU", "SM": "EU",
    "RS": "EU", "SK": "EU", "SI": "EU", "ES": "EU", "SE": "EU", "CH": "EU",
    "UA": "EU", "GB": "EU", "VA": "EU",
    # North America
    "CA": "NA", "US": "NA", "MX": "NA", "GT": "NA", "BZ": "NA", "SV": "NA",
    "HN": "NA", "NI": "NA", "CR": "NA", "PA": "NA", "CU": "NA", "DO": "NA",
    "HT": "NA", "JM": "NA", "BS": "NA", "TT": "NA", "PR": "NA",
    # South America
    "AR": "SA", "BO": "SA", "BR": "SA", "CL": "SA", "CO": "SA", "EC": "SA",
    "GY": "SA", "PY": "SA", "PE": "SA", "SR": "SA", "UY": "SA", "VE": "SA",
    # Asia
    "AF": "AS", "AM": "AS", "AZ": "AS", "BH": "AS", "BD": "AS", "BT": "AS",
    "BN": "AS", "KH": "AS", "CN": "AS", "GE": "AS", "IN": "AS", "ID": "AS",
    "IR": "AS", "IQ": "AS", "IL": "AS", "JP": "AS", "JO": "AS", "KZ": "AS",
    "KW": "AS", "KG": "AS", "LA": "AS", "LB": "AS", "MY": "AS", "MV": "AS",
    "MN": "AS", "MM": "AS", "NP": "AS", "KP": "AS", "OM": "AS", "PK": "AS",
    "PH": "AS", "QA": "AS", "SA": "AS", "SG": "AS", "KR": "AS", "LK": "AS",
    "SY": "AS", "TW": "AS", "TJ": "AS", "TH": "AS", "TR": "AS", "TM": "AS",
    "AE": "AS", "UZ": "AS", "VN": "AS", "YE": "AS", "HK": "AS", "MO": "AS",
    # Africa
    "DZ": "AF", "AO": "AF", "BJ": "AF", "BW": "AF", "BF": "AF", "BI": "AF",
    "CM": "AF", "CV": "AF", "CF": "AF", "TD": "AF", "KM": "AF", "CG": "AF",
    "CD": "AF", "CI": "AF", "DJ": "AF", "EG": "AF", "GQ": "AF", "ER": "AF",
    "ET": "AF", "GA": "AF", "GM": "AF", "GH": "AF", "GN": "AF", "KE": "AF",
    "LS": "AF", "LR": "AF", "LY": "AF", "MG": "AF", "MW": "AF", "ML": "AF",
    "MR": "AF", "MU": "AF", "MA": "AF", "MZ": "AF", "NA": "AF", "NE": "AF",
    "NG": "AF", "RW": "AF", "SN": "AF", "SC": "AF", "SL": "AF", "SO": "AF",
    "ZA": "AF", "SS": "AF", "SD": "AF", "TZ": "AF", "TG": "AF", "TN": "AF",
    "UG": "AF", "ZM": "AF", "ZW": "AF",
    # Oceania
    "AU": "OC", "FJ": "OC", "KI": "OC", "NZ": "OC", "PG": "OC", "WS": "OC",
    "SB": "OC", "TO": "OC", "VU": "OC", "NC": "OC", "PF": "OC",
}

CONTINENT_NAMES = {
    "AF": "Africa", "AN": "Antarctica", "AS": "Asia", "EU": "Europe",
    "NA": "North America", "OC": "Oceania", "SA": "South America",
    "UNKNOWN": "Unknown",
}

# ============================================================================
# Préfixe de code ICAO d'avion -> constructeur
# On teste les préfixes du plus long au plus court pour lever les ambiguïtés
# (ex: "BCS" = Airbus A220 vs "B" = Boeing).
# ============================================================================

AIRCRAFT_PREFIX_TO_MANUFACTURER = {
    "BCS": "Airbus",     # A220 (ex-Bombardier CSeries)
    "CRJ": "Bombardier",
    "CL": "Bombardier",
    "DH": "De Havilland Canada",
    "AT": "ATR",
    "E1": "Embraer",     # E170/E175/E190/E195
    "E2": "Embraer",     # E190-E2/E195-E2
    "EMB": "Embraer",
    "A": "Airbus",
    "B": "Boeing",
    "MD": "McDonnell Douglas",
    "F": "Fokker",
    "SU": "Sukhoi",
    "AN": "Antonov",
    "IL": "Ilyushin",
    "TU": "Tupolev",
}


# ============================================================================
# Nom de pays (FR24) -> code ISO alpha-2
# `get_airports()` renvoie le NOM du pays (pas le code). On mappe ici les noms
# (display ou URL-friendly) vers le code ISO, via une clé normalisée
# (minuscules, lettres uniquement) pour tolérer "United States" / "united-states".
# ============================================================================

def _norm(name: str) -> str:
    return "".join(c for c in name.lower() if c.isalpha())


_COUNTRY_NAME_TO_CODE_RAW = {
    # Europe
    "Albania": "AL", "Andorra": "AD", "Austria": "AT", "Belarus": "BY",
    "Belgium": "BE", "Bosnia and Herzegovina": "BA", "Bulgaria": "BG",
    "Croatia": "HR", "Cyprus": "CY", "Czech Republic": "CZ", "Czechia": "CZ",
    "Denmark": "DK", "Estonia": "EE", "Finland": "FI", "France": "FR",
    "Germany": "DE", "Greece": "GR", "Hungary": "HU", "Iceland": "IS",
    "Ireland": "IE", "Italy": "IT", "Latvia": "LV", "Liechtenstein": "LI",
    "Lithuania": "LT", "Luxembourg": "LU", "Malta": "MT", "Moldova": "MD",
    "Monaco": "MC", "Montenegro": "ME", "Netherlands": "NL",
    "North Macedonia": "MK", "Macedonia": "MK", "Norway": "NO", "Poland": "PL",
    "Portugal": "PT", "Romania": "RO", "Russia": "RU",
    "Russian Federation": "RU", "San Marino": "SM", "Serbia": "RS",
    "Slovakia": "SK", "Slovenia": "SI", "Spain": "ES", "Sweden": "SE",
    "Switzerland": "CH", "Ukraine": "UA", "United Kingdom": "GB",
    # North America
    "Canada": "CA", "United States": "US",
    "United States of America": "US", "Mexico": "MX", "Guatemala": "GT",
    "Belize": "BZ", "El Salvador": "SV", "Honduras": "HN", "Nicaragua": "NI",
    "Costa Rica": "CR", "Panama": "PA", "Cuba": "CU",
    "Dominican Republic": "DO", "Haiti": "HT", "Jamaica": "JM",
    "Bahamas": "BS", "Trinidad and Tobago": "TT", "Puerto Rico": "PR",
    # South America
    "Argentina": "AR", "Bolivia": "BO", "Brazil": "BR", "Chile": "CL",
    "Colombia": "CO", "Ecuador": "EC", "Guyana": "GY", "Paraguay": "PY",
    "Peru": "PE", "Suriname": "SR", "Uruguay": "UY", "Venezuela": "VE",
    # Asia
    "Afghanistan": "AF", "Armenia": "AM", "Azerbaijan": "AZ", "Bahrain": "BH",
    "Bangladesh": "BD", "Bhutan": "BT", "Brunei": "BN", "Cambodia": "KH",
    "China": "CN", "Georgia": "GE", "India": "IN", "Indonesia": "ID",
    "Iran": "IR", "Iraq": "IQ", "Israel": "IL", "Japan": "JP", "Jordan": "JO",
    "Kazakhstan": "KZ", "Kuwait": "KW", "Kyrgyzstan": "KG", "Laos": "LA",
    "Lebanon": "LB", "Malaysia": "MY", "Maldives": "MV", "Mongolia": "MN",
    "Myanmar": "MM", "Nepal": "NP", "North Korea": "KP", "Oman": "OM",
    "Pakistan": "PK", "Philippines": "PH", "Qatar": "QA", "Saudi Arabia": "SA",
    "Singapore": "SG", "South Korea": "KR", "Sri Lanka": "LK", "Syria": "SY",
    "Taiwan": "TW", "Tajikistan": "TJ", "Thailand": "TH", "Turkey": "TR",
    "Turkmenistan": "TM", "United Arab Emirates": "AE", "Uzbekistan": "UZ",
    "Vietnam": "VN", "Yemen": "YE", "Hong Kong": "HK", "Macau": "MO",
    # Africa
    "Algeria": "DZ", "Angola": "AO", "Benin": "BJ", "Botswana": "BW",
    "Burkina Faso": "BF", "Burundi": "BI", "Cameroon": "CM", "Cape Verde": "CV",
    "Chad": "TD", "Congo": "CG", "Democratic Republic of the Congo": "CD",
    "Ivory Coast": "CI", "Djibouti": "DJ", "Egypt": "EG", "Eritrea": "ER",
    "Ethiopia": "ET", "Gabon": "GA", "Gambia": "GM", "Ghana": "GH",
    "Guinea": "GN", "Kenya": "KE", "Lesotho": "LS", "Liberia": "LR",
    "Libya": "LY", "Madagascar": "MG", "Malawi": "MW", "Mali": "ML",
    "Mauritania": "MR", "Mauritius": "MU", "Morocco": "MA", "Mozambique": "MZ",
    "Namibia": "NA", "Niger": "NE", "Nigeria": "NG", "Rwanda": "RW",
    "Senegal": "SN", "Seychelles": "SC", "Sierra Leone": "SL", "Somalia": "SO",
    "South Africa": "ZA", "South Sudan": "SS", "Sudan": "SD", "Tanzania": "TZ",
    "Togo": "TG", "Tunisia": "TN", "Uganda": "UG", "Zambia": "ZM",
    "Zimbabwe": "ZW",
    # Oceania
    "Australia": "AU", "Fiji": "FJ", "Kiribati": "KI", "New Zealand": "NZ",
    "Papua New Guinea": "PG", "Samoa": "WS", "Solomon Islands": "SB",
    "Tonga": "TO", "Vanuatu": "VU", "New Caledonia": "NC",
    "French Polynesia": "PF",
}

# Clé normalisée -> code ISO (tolère display name et URL-friendly)
COUNTRY_NAME_TO_CODE = {_norm(name): code for name, code in _COUNTRY_NAME_TO_CODE_RAW.items()}


def country_code_from_name_expr(country_name_col: str) -> Column:
    """
    Expression Spark : nom de pays (FR24) -> code ISO alpha-2.

    Normalise la colonne (minuscules, lettres uniquement) avant lookup.
    Retourne NULL si inconnu.
    """
    norm = regexp_replace(lower(col(country_name_col)), "[^a-z]", "")
    mapping = create_map([lit(x) for x in chain(*COUNTRY_NAME_TO_CODE.items())])
    return mapping[norm]


def continent_code_expr(country_code_col: str) -> Column:
    """
    Construire une expression Spark : code pays -> code continent.

    Args:
        country_code_col: nom de la colonne contenant le code pays (ISO alpha-2)

    Returns:
        Column avec le code continent (fallback "UNKNOWN")
    """
    mapping = create_map([lit(x) for x in chain(*COUNTRY_TO_CONTINENT.items())])
    return coalesce(mapping[upper(col(country_code_col))], lit("UNKNOWN"))


def manufacturer_expr(aircraft_code_col: str) -> Column:
    """
    Construire une expression Spark : code avion ICAO -> constructeur.

    On évalue les préfixes du plus spécifique au plus générique.

    Args:
        aircraft_code_col: nom de la colonne contenant le code avion

    Returns:
        Column avec le constructeur (fallback "Other")
    """
    from pyspark.sql.functions import when

    code = upper(col(aircraft_code_col))
    # Trier par longueur de préfixe décroissante pour la priorité
    prefixes = sorted(AIRCRAFT_PREFIX_TO_MANUFACTURER.keys(), key=len, reverse=True)

    expr = None
    for prefix in prefixes:
        manufacturer = AIRCRAFT_PREFIX_TO_MANUFACTURER[prefix]
        condition = substring(code, 1, len(prefix)) == lit(prefix)
        expr = when(condition, lit(manufacturer)) if expr is None else expr.when(condition, lit(manufacturer))

    return expr.otherwise(lit("Other")) if expr is not None else lit("Other")
