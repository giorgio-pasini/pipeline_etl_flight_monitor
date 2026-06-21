"""Tests unitaires pour les transformations Silver + Gold."""

import pytest
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, TimestampType,
)
from pyspark.sql.functions import col

from src.transformations import (
    clean_and_enrich_bronze,
    kpi_airline_volumes,
    kpi_aircraft_manufacturers,
    kpi_longest_flight,
    kpi_airport_imbalance,
)


# Schéma minimal (sous-ensemble de Bronze enrichi) suffisant pour les transformations
_SCHEMA = StructType([
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
])


def _row(**kw):
    base = {f.name: None for f in _SCHEMA.fields}
    base.update(kw)
    return base


def _make_df(spark, rows):
    return spark.createDataFrame([_row(**r) for r in rows], schema=_SCHEMA)


@pytest.fixture
def enriched_df(spark_session):
    """Trois vols valides en l'air : 2 Boeing (DAL), 1 Airbus (AFR)."""
    ts = datetime(2026, 6, 21, 14, 0, 0)
    rows = [
        dict(flight_id="F1", extraction_timestamp=ts, callsign="DAL1", airline_icao="DAL",
             airline_name="Delta", aircraft_code="B738", on_ground=0, is_valid=True,
             origin_iata="JFK", destination_iata="LAX",
             origin_airport_country_code="US", destination_airport_country_code="US",
             origin_airport_latitude=40.6, origin_airport_longitude=-73.8,
             destination_airport_latitude=33.9, destination_airport_longitude=-118.4),
        dict(flight_id="F2", extraction_timestamp=ts, callsign="DAL2", airline_icao="DAL",
             airline_name="Delta", aircraft_code="B739", on_ground=0, is_valid=True,
             origin_iata="ATL", destination_iata="JFK",
             origin_airport_country_code="US", destination_airport_country_code="US",
             origin_airport_latitude=33.6, origin_airport_longitude=-84.4,
             destination_airport_latitude=40.6, destination_airport_longitude=-73.8),
        dict(flight_id="F3", extraction_timestamp=ts, callsign="AFR1", airline_icao="AFR",
             airline_name="Air France", aircraft_code="A320", on_ground=0, is_valid=True,
             origin_iata="CDG", destination_iata="JFK",
             origin_airport_country_code="FR", destination_airport_country_code="US",
             origin_airport_latitude=49.0, origin_airport_longitude=2.55,
             destination_airport_latitude=40.6, destination_airport_longitude=-73.8),
    ]
    return clean_and_enrich_bronze(_make_df(spark_session, rows))


class TestCleanAndEnrich:
    def test_adds_continent(self, enriched_df):
        rows = {r["flight_id"]: r for r in enriched_df.collect()}
        assert rows["F1"]["origin_continent"] == "NA"
        assert rows["F3"]["origin_continent"] == "EU"

    def test_adds_manufacturer(self, enriched_df):
        rows = {r["flight_id"]: r for r in enriched_df.collect()}
        assert rows["F1"]["manufacturer"] == "Boeing"
        assert rows["F3"]["manufacturer"] == "Airbus"

    def test_computes_distance(self, enriched_df):
        rows = {r["flight_id"]: r for r in enriched_df.collect()}
        # JFK -> LAX ~ 3970 km
        assert rows["F1"]["distance_km"] is not None
        assert 3500 < rows["F1"]["distance_km"] < 4300

    def test_dedup_keeps_latest(self, spark_session):
        ts1 = datetime(2026, 6, 21, 14, 0, 0)
        ts2 = datetime(2026, 6, 21, 15, 0, 0)
        rows = [
            dict(flight_id="DUP", extraction_timestamp=ts1, airline_icao="DAL",
                 aircraft_code="B738", on_ground=0, is_valid=True, callsign="OLD"),
            dict(flight_id="DUP", extraction_timestamp=ts2, airline_icao="DAL",
                 aircraft_code="B738", on_ground=0, is_valid=True, callsign="NEW"),
        ]
        out = clean_and_enrich_bronze(_make_df(spark_session, rows)).collect()
        assert len(out) == 1
        assert out[0]["callsign"] == "NEW"


class TestKpis:
    def test_airline_volumes_top(self, enriched_df):
        result = kpi_airline_volumes(enriched_df).collect()
        assert len(result) == 1
        assert result[0]["airline_icao"] == "DAL"
        assert result[0]["active_flights_count"] == 2

    def test_aircraft_manufacturers_top(self, enriched_df):
        result = kpi_aircraft_manufacturers(enriched_df).collect()
        assert len(result) == 1
        assert result[0]["manufacturer"] == "Boeing"
        assert result[0]["active_flights_count"] == 2

    def test_longest_flight(self, enriched_df):
        result = kpi_longest_flight(enriched_df).collect()
        assert len(result) == 1
        # CDG->JFK (F3, transatlantique ~5800 km) est le plus long des trois
        assert result[0]["origin_iata"] == "CDG"
        assert result[0]["destination_iata"] == "JFK"

    def test_airport_imbalance(self, enriched_df):
        result = kpi_airport_imbalance(enriched_df).collect()
        assert len(result) == 1
        assert "imbalance_abs" in result[0].asDict()

    def test_excludes_on_ground_and_invalid(self, spark_session):
        ts = datetime(2026, 6, 21, 14, 0, 0)
        rows = [
            dict(flight_id="G", extraction_timestamp=ts, airline_icao="DAL",
                 aircraft_code="B738", on_ground=1, is_valid=True),  # au sol -> exclu
            dict(flight_id="I", extraction_timestamp=ts, airline_icao="DAL",
                 aircraft_code="B738", on_ground=0, is_valid=False),  # invalide -> exclu
        ]
        df = clean_and_enrich_bronze(_make_df(spark_session, rows))
        assert kpi_airline_volumes(df).count() == 0
