"""Tests d'intégration pour le batch job complet."""

import pytest
import logging
from unittest.mock import Mock, patch
from pathlib import Path
from src.batch_job import run_batch, create_spark_session
from config.pipeline_config import PipelineConfig
from tests.conftest import make_mock_flight


class TestBatchJobIntegration:
    """Tests d'intégration pour le batch job."""

    def test_batch_job_with_mock_api(self, spark_session, temp_datalake, parquet_write_supported):
        """Tester le batch job complet avec API mockée."""
        if not parquet_write_supported:
            pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

        logger = logging.getLogger(__name__)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            # Setup mock API
            mock_api = Mock()
            mock_api_class.return_value = mock_api

            # Mock un vol valide (tous attributs typés)
            mock_flight = make_mock_flight(
                id="ABC123", callsign="DLH123", number="DL123",
                airline_icao="DAL", aircraft_code="B738", registration="N1234AA",
                origin_airport_iata="CDG", destination_airport_iata="JFK",
                latitude=48.7, longitude=2.5, altitude=10000.0,
                ground_speed=450.0, heading=90.0, on_ground=0, vertical_speed=100.0,
                origin_airport_country_code="FR", destination_airport_country_code="US",
                origin_airport_latitude=49.0, origin_airport_longitude=2.55,
                destination_airport_latitude=40.6, destination_airport_longitude=-73.8,
            )

            mock_api.get_flights.return_value = [mock_flight]

            success = run_batch(
                spark_session,
                PipelineConfig,
                logger,
                zones=["global"]
            )

            # Le batch doit réussir (chemin nominal complet)
            assert success is True

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
                PipelineConfig,
                logger,
                zones=["global"]
            )

            # Extraction vide -> le batch échoue proprement (« Aucun vol collecté »), sans crasher
            assert success is False


class TestBatchJobFaultTolerance:
    """Tests pour la résilience du batch job."""

    def test_batch_continues_on_partial_failure(self, spark_session, temp_datalake, parquet_write_supported):
        """Le batch doit continuer même avec des données partielles."""
        if not parquet_write_supported:
            pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

        logger = logging.getLogger(__name__)

        with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api

            # Vol valide + vol invalide (données manquantes / altitude négative)
            valid_flight = make_mock_flight(
                id="ABC123", callsign="DLH123", number="DL123",
                airline_icao="DAL", aircraft_code="B738", registration="N1234AA",
                origin_airport_iata="CDG", destination_airport_iata="JFK",
                latitude=48.7, longitude=2.5, altitude=10000.0,
                ground_speed=450.0, heading=90.0, on_ground=0, vertical_speed=100.0,
            )

            invalid_flight = make_mock_flight(
                id="XYZ999", callsign="BAD999", number="BAD999",
                airline_icao=None, origin_airport_iata=None,
                destination_airport_iata="ORY",
                latitude=0.0, longitude=0.0, altitude=-100.0,  # altitude invalide
                ground_speed=450.0, heading=90.0, on_ground=0, vertical_speed=100.0,
            )

            mock_api.get_flights.return_value = [valid_flight, invalid_flight]

            success = run_batch(
                spark_session,
                PipelineConfig,
                logger,
                zones=["global"]
            )

            # Doit continuer (fault-tolerant) et réussir malgré le vol invalide
            assert success is True
