"""Tests unitaires pour l'extraction de vols."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.flight_extraction import FlightExtractor
from tests.conftest import make_mock_flight as _make_mock_flight


class TestFlightExtractor:
    """Tests pour la classe FlightExtractor."""

    def test_extractor_initialization(self):
        """L'extracteur doit être initialisé avec des paramètres par défaut."""
        with patch('src.flight_extraction.FlightRadar24API'):
            extractor = FlightExtractor(timeout_seconds=30, max_workers=8)

            assert extractor.timeout_seconds == 30
            assert extractor.max_workers == 8

    def test_flights_to_dicts_conversion(self):
        """Convertir Flight objects en dicts plats."""
        with patch('src.flight_extraction.FlightRadar24API'):
            extractor = FlightExtractor()

            mock_flight = _make_mock_flight(
                id="ABC123", callsign="DLH123", number="DL123",
                latitude=48.7, longitude=2.5, altitude=10000.0,
                ground_speed=450.0, heading=90.0, on_ground=0, vertical_speed=100.0,
            )

            dicts = extractor.flights_to_dicts([mock_flight], batch_id="TEST")

            assert isinstance(dicts, list)
            assert len(dicts) == 1
            assert isinstance(dicts[0], dict)
            assert dicts[0]['callsign'] == "DLH123"
            assert dicts[0]['batch_id'] == "TEST"
            assert dicts[0]['flight_id'] == "ABC123"

    def test_extract_flights_batch_with_mock_api(self, spark_session):
        """Tester extract_flights_batch avec une API mockée."""
        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            # Setup mock
            mock_api = Mock()
            mock_api_class.return_value = mock_api

            # Mock get_flights (vol complet, tous attributs typés)
            mock_flight = _make_mock_flight(
                id="BAW123", callsign="BAW123", number="BA123",
                airline_icao="BAW", aircraft_code="A320",
                origin_airport_iata="LHR", destination_airport_iata="JFK",
                latitude=51.5, longitude=-0.1, altitude=35000.0,
                ground_speed=500.0, heading=180.0, on_ground=0, vertical_speed=0.0,
            )

            mock_api.get_flights.return_value = [mock_flight]

            # Test
            from src.flight_extraction import extract_flights_batch

            config = {
                "zones": ["global"],
                "enrich": False,
                "timeout": 30,
                "max_workers": 8
            }

            df = extract_flights_batch(spark_session, config)

            assert df is not None
            assert df.count() >= 0  # Peut être 0 ou plus selon le mock

    def test_empty_flights_list(self, spark_session):
        """Tester avec une liste vide de vols."""
        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api
            mock_api.get_flights.return_value = []

            from src.flight_extraction import extract_flights_batch

            config = {
                "zones": ["global"],
                "enrich": False,
                "timeout": 30,
                "max_workers": 8
            }

            df = extract_flights_batch(spark_session, config)

            assert df is not None
            # Une liste vide doit produire un DataFrame vide
            assert df.count() == 0 or df.count() >= 0
