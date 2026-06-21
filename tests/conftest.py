"""
Fixtures partagées pour tous les tests.
"""

import os
import sys
import pytest
from pathlib import Path
from datetime import datetime

# IMPORTANT : sur Windows, les workers PySpark doivent pouvoir retrouver
# l'exécutable Python. Sans cela, Spark échoue avec "Python worker failed to
# connect back" (l'alias d'exécution Windows intercepte `python`).
# On force PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON sur l'interpréteur courant.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

# Import local
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig


# Tous les attributs lus par FlightExtractor.flights_to_dicts (vrais noms FlightRadarAPI)
FLIGHT_ATTRS = [
    "id", "callsign", "number", "airline_icao", "airline_iata", "aircraft_code",
    "registration", "origin_airport_iata", "destination_airport_iata",
    "latitude", "longitude", "altitude", "ground_speed", "heading",
    "on_ground", "vertical_speed", "icao_24bit",
    "aircraft_model", "airline_name", "origin_airport_name",
    "origin_airport_country_code", "origin_airport_country_name",
    "origin_airport_latitude", "origin_airport_longitude",
    "destination_airport_name", "destination_airport_country_code",
    "destination_airport_country_name", "destination_airport_latitude",
    "destination_airport_longitude", "status_text",
]


def make_mock_flight(**overrides):
    """Créer un Mock de vol avec TOUS les attributs lus (None par défaut).

    Évite les fuites d'objets Mock qui font échouer le typage Spark.
    """
    from unittest.mock import Mock
    flight = Mock()
    for attr in FLIGHT_ATTRS:
        setattr(flight, attr, None)
    flight.id = "FID000"  # flight_id non-nullable dans le schéma
    for k, v in overrides.items():
        setattr(flight, k, v)
    return flight


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


@pytest.fixture(scope="session")
def parquet_write_supported(spark_session, tmp_path_factory):
    """True si Spark peut écrire du Parquet localement.

    Sous Windows sans HADOOP_HOME/winutils.exe, l'écriture Parquet échoue
    (limitation connue de Spark-sur-Windows, pas un bug du pipeline). On le
    détecte une fois pour permettre aux tests d'écriture de se *skip* proprement.
    """
    check_path = tmp_path_factory.mktemp("pq_check") / "probe"
    try:
        spark_session.range(1).write.mode("overwrite").parquet(str(check_path))
        return True
    except Exception:
        return False


@pytest.fixture(scope="function")
def temp_datalake(tmp_path):
    """Créer un datalake temporaire pour les tests.

    Redirige DATALAKE_ROOT *et* les chemins dérivés (BRONZE/SILVER/GOLD/LOG),
    qui sont calculés à l'import et ne suivent pas l'override sinon — garantit
    l'isolation : aucun test n'écrit dans le vrai datalake du dépôt.
    """
    datalake_root = tmp_path / "datalake"
    datalake_root.mkdir(exist_ok=True)

    for layer in ["bronze", "silver", "gold"]:
        (datalake_root / layer).mkdir(exist_ok=True)

    # Sauvegarder l'ancienne config
    saved = {
        attr: getattr(DatalakeConfig, attr)
        for attr in ["DATALAKE_ROOT", "BRONZE_PATH", "SILVER_PATH", "GOLD_PATH", "LOG_PATH"]
    }

    # Override avec les chemins temporaires
    root = str(datalake_root)
    DatalakeConfig.DATALAKE_ROOT = root
    DatalakeConfig.BRONZE_PATH = f"{root}/bronze"
    DatalakeConfig.SILVER_PATH = f"{root}/silver"
    DatalakeConfig.GOLD_PATH = f"{root}/gold"
    DatalakeConfig.LOG_PATH = f"{root}/_logs"

    yield datalake_root

    # Restaurer
    for attr, value in saved.items():
        setattr(DatalakeConfig, attr, value)


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

    # Passer une liste de dicts (robuste à l'ordre des colonnes) plutôt que
    # dict_values, que Spark refuse.
    df = spark_session.createDataFrame(
        [sample_flight_dict],
        schema=schema_flights_raw
    )

    return df
