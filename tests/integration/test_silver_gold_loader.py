"""Tests d'intégration : orchestration Bronze -> Silver -> Gold.

Couvre SilverGoldLoader.run_full_etl de bout en bout + la déduplication
cross-batch (idempotence). Nécessite l'écriture Parquet -> skip sinon.
"""

import pytest
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, TimestampType,
)

from src.silver_gold_loader import SilverGoldLoader
from config.datalake_config import DatalakeConfig


# Schéma Bronze (sous-ensemble enrichi suffisant pour Silver/Gold) + partitions
_BRONZE_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("extraction_timestamp", TimestampType()),
    StructField("callsign", StringType()),
    StructField("airline_icao", StringType()),
    StructField("airline_name", StringType()),
    StructField("aircraft_code", StringType()),
    StructField("on_ground", IntegerType()),
    StructField("is_valid", BooleanType()),
    StructField("origin_iata", StringType()),
    StructField("destination_iata", StringType()),
    StructField("origin_airport_country_code", StringType()),
    StructField("destination_airport_country_code", StringType()),
    StructField("origin_airport_latitude", DoubleType()),
    StructField("origin_airport_longitude", DoubleType()),
    StructField("destination_airport_latitude", DoubleType()),
    StructField("destination_airport_longitude", DoubleType()),
    StructField("tech_year", StringType()),
    StructField("tech_month", StringType()),
])


def _bronze_rows(ts):
    """Trois vols valides en l'air : DAL x2 (Boeing, US->US), AFR x1 (Airbus, FR->US)."""
    return [
        ("F1", ts, "DAL1", "DAL", "Delta", "B738", 0, True, "JFK", "LAX",
         "US", "US", 40.6, -73.8, 33.9, -118.4, "2026", "2026-06"),
        ("F2", ts, "DAL2", "DAL", "Delta", "B739", 0, True, "ATL", "JFK",
         "US", "US", 33.6, -84.4, 40.6, -73.8, "2026", "2026-06"),
        ("F3", ts, "AFR1", "AFR", "Air France", "A320", 0, True, "CDG", "JFK",
         "FR", "US", 49.0, 2.55, 40.6, -73.8, "2026", "2026-06"),
    ]


def _write_bronze(spark, rows, path):
    spark.createDataFrame(rows, schema=_BRONZE_SCHEMA) \
        .write.mode("append").parquet(path)


def test_run_full_etl_end_to_end(spark_session, temp_datalake, parquet_write_supported):
    if not parquet_write_supported:
        pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

    bronze_path = DatalakeConfig.get_bronze_flights_path()
    _write_bronze(spark_session, _bronze_rows(datetime(2026, 6, 21, 14, 0, 0)), bronze_path)

    loader = SilverGoldLoader(spark_session, DatalakeConfig)
    result = loader.run_full_etl(bronze_path)

    # Silver : 3 vols uniques
    assert result["silver"].count() == 3

    # Gold : les 7 KPIs présents
    kpis = result["gold_kpis"]
    assert len(kpis) == 7
    expected = {
        "airline_volumes", "continental_regional", "longest_flight",
        "continental_avg_distance", "aircraft_manufacturers",
        "airline_aircraft_top3", "airport_imbalance",
    }
    assert set(kpis.keys()) == expected

    # Valeurs métier : compagnie la plus active = DAL (2 vols)
    top_airline = kpis["airline_volumes"].collect()
    assert top_airline[0]["airline_icao"] == "DAL"
    assert top_airline[0]["active_flights_count"] == 2

    # Constructeur le plus actif = Boeing (2 vols)
    top_mfr = kpis["aircraft_manufacturers"].collect()
    assert top_mfr[0]["manufacturer"] == "Boeing"


def test_run_full_etl_dedups_across_batches(spark_session, temp_datalake, parquet_write_supported):
    """Deux batches capturant les mêmes flight_id -> Silver dédupliqué (idempotence)."""
    if not parquet_write_supported:
        pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

    bronze_path = DatalakeConfig.get_bronze_flights_path()
    # Batch 1 puis batch 2 (timestamp plus récent) avec les mêmes flight_id
    _write_bronze(spark_session, _bronze_rows(datetime(2026, 6, 21, 14, 0, 0)), bronze_path)
    _write_bronze(spark_session, _bronze_rows(datetime(2026, 6, 21, 16, 0, 0)), bronze_path)

    loader = SilverGoldLoader(spark_session, DatalakeConfig)
    result = loader.run_full_etl(bronze_path)

    # 6 lignes Bronze mais 3 flight_id uniques -> dedup garde le plus récent
    assert result["silver"].count() == 3
