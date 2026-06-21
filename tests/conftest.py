"""
Fixtures partagées pour tous les tests.
"""

import pytest
from pathlib import Path
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime

# Import local
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig


@pytest.fixture(scope="session")
def spark_session():
    """Créer une session Spark pour les tests (une par session de test)."""
    spark = SparkSession.builder \
        .appName("flight-radar-tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()


@pytest.fixture(scope="function")
def temp_datalake(tmp_path):
    """Créer un datalake temporaire pour les tests."""
    datalake_root = tmp_path / "datalake"
    datalake_root.mkdir(exist_ok=True)

    # Créer les couches
    for layer in ["bronze", "silver", "gold"]:
        (datalake_root / layer).mkdir(exist_ok=True)

    # Sauvegarder l'ancienne config
    old_root = DatalakeConfig.DATALAKE_ROOT

    # Override avec le chemin temporaire
    DatalakeConfig.DATALAKE_ROOT = str(datalake_root)

    yield datalake_root

    # Restaurer l'ancienne config
    DatalakeConfig.DATALAKE_ROOT = old_root


@pytest.fixture
def sample_flight_dict():
    """Exemple de vol (Flight object converti en dict)."""
    from datetime import datetime as dt
    return {
        'extraction_timestamp': dt.now(),
        'batch_id': 'BATCH_001',
        'source_zone': 'global',
        'flight_id': 'ABC123',
        'callsign': 'DLH123',
        'flight_number': 'DL123',
        'airline_icao': 'DAL',
        'airline_iata': 'DL',
        'aircraft_code': 'B737',
        'registration': 'N1234AA',
        'origin_iata': 'CDG',
        'destination_iata': 'ORY',
        'latitude': 48.7,
        'longitude': 2.5,
        'altitude': 10000.0,
        'ground_speed': 450.0,
        'heading': 90.0,
        'on_ground': 0,
        'vertical_speed': 100.0,
        'icao_24bit': 'ABC123',
        'aircraft_model': 'B737-800',
        'airline_name': 'Lufthansa',
        'origin_airport_name': 'Charles de Gaulle',
        'origin_airport_country_code': 'FR',
        'origin_airport_country_name': 'France',
        'origin_airport_latitude': 49.0,
        'origin_airport_longitude': 2.55,
        'destination_airport_name': 'Orly',
        'destination_airport_country_code': 'FR',
        'destination_airport_country_name': 'France',
        'destination_airport_latitude': 48.72,
        'destination_airport_longitude': 2.39,
        'status_text': 'In Air',
    }


@pytest.fixture
def sample_flight_dict_invalid():
    """Exemple de vol invalide (données manquantes)."""
    from datetime import datetime as dt
    return {
        'extraction_timestamp': dt.now(),
        'batch_id': 'BATCH_001',
        'source_zone': 'global',
        'flight_id': 'XYZ999',
        'callsign': 'BAD999',
        'flight_number': 'BAD999',
        'airline_icao': None,  # Manquant
        'airline_iata': None,
        'aircraft_code': None,  # Manquant
        'registration': 'UNKNOWN',
        'origin_iata': None,  # Manquant
        'destination_iata': 'ORY',
        'latitude': 48.7,
        'longitude': 2.5,
        'altitude': -100.0,  # Invalid
        'ground_speed': 450.0,
        'heading': 90.0,
        'on_ground': 0,
        'vertical_speed': 100.0,
        'icao_24bit': 'XYZ999',
        'aircraft_model': None,
        'airline_name': None,
        'origin_airport_name': None,
        'origin_airport_country_code': None,
        'origin_airport_country_name': None,
        'origin_airport_latitude': None,
        'origin_airport_longitude': None,
        'destination_airport_name': 'Orly',
        'destination_airport_country_code': 'FR',
        'destination_airport_country_name': 'France',
        'destination_airport_latitude': 48.72,
        'destination_airport_longitude': 2.39,
        'status_text': 'Unknown',
    }


@pytest.fixture
def sample_flights_dataframe(spark_session, sample_flight_dict):
    """DataFrame Spark avec quelques vols de test."""
    from src.schemas import schema_flights_raw

    data = [sample_flight_dict.values()]
    df = spark_session.createDataFrame(
        data,
        schema=schema_flights_raw
    )

    return df
