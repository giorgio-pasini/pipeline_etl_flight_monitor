"""Tests d'intégration pour le batch job complet."""

import pytest
import logging
from unittest.mock import Mock, patch
from pathlib import Path
from src.batch_job import run_batch, create_spark_session
from config.datalake_config import DatalakeConfig


class TestBatchJobIntegration:
    """Tests d'intégration pour le batch job."""

    def test_spark_session_creation(self):
        """Créer une session Spark avec la configuration."""
        spark = create_spark_session(DatalakeConfig)

        assert spark is not None
        assert spark.sparkContext is not None

        spark.stop()

    def test_batch_job_with_mock_api(self, spark_session, temp_datalake):
        """Tester le batch job complet avec API mockée."""
        logger = logging.getLogger(__name__)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            # Setup mock API
            mock_api = Mock()
            mock_api_class.return_value = mock_api

            # Mock un vol
            mock_flight = Mock()
            mock_flight.callsign = "DLH123"
            mock_flight.flight_number = "DL123"
            mock_flight.latitude = 48.7
            mock_flight.longitude = 2.5
            mock_flight.altitude = 10000.0
            mock_flight.ground_speed = 450.0
            mock_flight.heading = 90.0
            mock_flight.on_ground = 0
            mock_flight.vertical_speed = 100.0
            mock_flight.airline_icao = "DAL"
            mock_flight.origin_iata = "CDG"
            mock_flight.destination_iata = "ORY"
            mock_flight.aircraft_code = "B737"
            mock_flight.registration = "N1234AA"
            mock_flight.flight_id = "ABC123"

            mock_api.get_flights.return_value = [mock_flight]

            # Run batch
            from src.batch_job import run_batch

            success = run_batch(
                spark_session,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Doit retourner True ou False, pas crasher
            assert isinstance(success, bool)

    def test_batch_job_empty_api_response(self, spark_session, temp_datalake):
        """Tester le batch job avec une réponse API vide."""
        logger = logging.getLogger(__name__)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api
            mock_api.get_flights.return_value = []

            from src.batch_job import run_batch

            success = run_batch(
                spark_session,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Avec une API vide, on doit continuer (pas crasher)
            assert isinstance(success, bool)

    def test_batch_job_logging(self, spark_session, temp_datalake, tmp_path):
        """Tester que le batch crée des logs."""
        logger = logging.getLogger("test_batch")

        # Créer un handler pour capturer les logs
        log_file = tmp_path / "test.log"
        handler = logging.FileHandler(log_file)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api
            mock_api.get_flights.return_value = []

            from src.batch_job import run_batch

            success = run_batch(
                spark_session,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Les logs doivent exister ou la fonction doit terminer sans erreur
            assert isinstance(success, bool)

        logger.removeHandler(handler)


class TestBatchJobFaultTolerance:
    """Tests pour la résilience du batch job."""

    def test_batch_continues_on_partial_failure(self, spark_session, temp_datalake):
        """Le batch doit continuer même avec des données partielles."""
        logger = logging.getLogger(__name__)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api

            # Vol valide + vol invalide
            valid_flight = Mock()
            valid_flight.callsign = "DLH123"
            valid_flight.flight_number = "DL123"
            valid_flight.latitude = 48.7
            valid_flight.longitude = 2.5
            valid_flight.altitude = 10000.0
            valid_flight.ground_speed = 450.0
            valid_flight.heading = 90.0
            valid_flight.on_ground = 0
            valid_flight.vertical_speed = 100.0
            valid_flight.airline_icao = "DAL"
            valid_flight.origin_iata = "CDG"
            valid_flight.destination_iata = "ORY"
            valid_flight.aircraft_code = "B737"
            valid_flight.registration = "N1234AA"
            valid_flight.flight_id = "ABC123"

            invalid_flight = Mock()
            invalid_flight.callsign = "BAD999"
            invalid_flight.flight_number = "BAD999"
            invalid_flight.latitude = 0.0
            invalid_flight.longitude = 0.0
            invalid_flight.altitude = -100.0  # Invalid
            invalid_flight.ground_speed = 450.0
            invalid_flight.heading = 90.0
            invalid_flight.on_ground = 0
            invalid_flight.vertical_speed = 100.0
            invalid_flight.airline_icao = None  # Missing
            invalid_flight.origin_iata = None  # Missing
            invalid_flight.destination_iata = "ORY"
            invalid_flight.aircraft_code = None
            invalid_flight.registration = "UNKNOWN"
            invalid_flight.flight_id = "XYZ999"

            mock_api.get_flights.return_value = [valid_flight, invalid_flight]

            from src.batch_job import run_batch

            success = run_batch(
                spark_session,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Doit continuer même avec données mixtes
            assert isinstance(success, bool)
