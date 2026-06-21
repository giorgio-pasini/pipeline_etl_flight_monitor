"""Tests unitaires pour les schémas Spark."""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.schemas import (
    schema_flights_raw,
    schema_dim_airlines,
    schema_dim_airports,
    schema_fact_flights,
    SCHEMAS,
)


class TestFlightsRawSchema:
    """Tests pour le schéma Bronze flights_raw."""

    def test_schema_exists(self):
        """Le schéma flights_raw doit exister."""
        assert schema_flights_raw is not None
        assert isinstance(schema_flights_raw, StructType)

    def test_schema_has_required_columns(self):
        """Le schéma doit contenir les colonnes obligatoires."""
        required_cols = [
            'flight_id', 'callsign', 'airline_icao', 'origin_iata',
            'destination_iata', 'altitude', 'ground_speed', 'on_ground',
            'extraction_timestamp', 'data_quality_flags', 'is_valid'
        ]
        schema_cols = [field.name for field in schema_flights_raw.fields]

        for col in required_cols:
            assert col in schema_cols, f"Colonne manquante: {col}"

    def test_schema_field_types(self):
        """Les types des colonnes doivent être corrects."""
        field_dict = {field.name: field.dataType for field in schema_flights_raw.fields}

        assert isinstance(field_dict['flight_id'], StringType)
        assert isinstance(field_dict['altitude'], (DoubleType, IntegerType))
        assert isinstance(field_dict['is_valid'], (type(None).__class__, IntegerType))
        assert isinstance(field_dict['extraction_timestamp'], TimestampType)

    def test_schema_can_create_dataframe(self, spark_session, sample_flight_dict):
        """On doit pouvoir créer un DataFrame avec ce schéma."""
        data = [sample_flight_dict.values()]
        df = spark_session.createDataFrame(data, schema=schema_flights_raw)

        assert df.count() == 1
        assert len(df.columns) == len(schema_flights_raw.fields)


class TestDimensionSchemas:
    """Tests pour les schémas de dimensions."""

    def test_airlines_schema_exists(self):
        """Le schéma dim_airlines doit exister."""
        assert schema_dim_airlines is not None
        assert 'airline_icao' in [f.name for f in schema_dim_airlines.fields]

    def test_airports_schema_exists(self):
        """Le schéma dim_airports doit exister."""
        assert schema_dim_airports is not None
        assert 'iata_code' in [f.name for f in schema_dim_airports.fields]

    def test_fact_flights_schema_exists(self):
        """Le schéma fact_flights doit exister."""
        assert schema_fact_flights is not None
        assert 'flight_id' in [f.name for f in schema_fact_flights.fields]


class TestSchemasRegistry:
    """Tests pour le registry de schémas."""

    def test_schemas_dict_populated(self):
        """Le dictionnaire SCHEMAS doit être rempli."""
        assert len(SCHEMAS) > 0

    def test_schemas_dict_has_bronze_tables(self):
        """Le dict doit contenir les tables Bronze."""
        assert 'flights_raw' in SCHEMAS

    def test_schemas_dict_has_silver_tables(self):
        """Le dict doit contenir les tables Silver."""
        expected_silver = ['fact_flights', 'dim_airlines', 'dim_airports']
        for table in expected_silver:
            assert table in SCHEMAS, f"Table Silver manquante: {table}"
