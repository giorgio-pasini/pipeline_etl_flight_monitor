"""Tests unitaires pour les données de référence (mappings purs, sans Spark)."""

from src.reference_data import (
    COUNTRY_TO_CONTINENT,
    AIRCRAFT_PREFIX_TO_MANUFACTURER,
    CONTINENT_NAMES,
)


class TestCountryToContinent:
    def test_known_countries(self):
        assert COUNTRY_TO_CONTINENT["FR"] == "EU"
        assert COUNTRY_TO_CONTINENT["US"] == "NA"
        assert COUNTRY_TO_CONTINENT["BR"] == "SA"
        assert COUNTRY_TO_CONTINENT["JP"] == "AS"
        assert COUNTRY_TO_CONTINENT["ZA"] == "AF"
        assert COUNTRY_TO_CONTINENT["AU"] == "OC"

    def test_all_continents_valid(self):
        """Tous les codes continent doivent être référencés dans CONTINENT_NAMES."""
        for code in set(COUNTRY_TO_CONTINENT.values()):
            assert code in CONTINENT_NAMES


class TestAircraftManufacturer:
    def test_prefixes_present(self):
        assert AIRCRAFT_PREFIX_TO_MANUFACTURER["A"] == "Airbus"
        assert AIRCRAFT_PREFIX_TO_MANUFACTURER["B"] == "Boeing"
        assert AIRCRAFT_PREFIX_TO_MANUFACTURER["BCS"] == "Airbus"  # A220, plus spécifique que "B"
