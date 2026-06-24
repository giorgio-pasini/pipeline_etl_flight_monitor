"""Tests unitaires pour les schémas Spark."""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.schemas import schema_flights_raw


class TestFlightsRawSchema:
    """Tests pour le schéma Bronze flights_raw."""

    def test_schema_has_required_columns(self):
        """Le schéma doit contenir les colonnes obligatoires."""
        required_cols = [
            'flight_id', 'callsign', 'airline_icao', 'origin_iata',
            'destination_iata', 'altitude', 'ground_speed', 'on_ground',
            'extraction_timestamp', 'batch_id'
        ]
        schema_cols = [field.name for field in schema_flights_raw.fields]

        for col in required_cols:
            assert col in schema_cols, f"Colonne manquante: {col}"

    def test_schema_field_types(self):
        """Les types des colonnes doivent être corrects."""
        field_dict = {field.name: field.dataType for field in schema_flights_raw.fields}

        assert isinstance(field_dict['flight_id'], StringType)
        assert isinstance(field_dict['altitude'], DoubleType)
        assert isinstance(field_dict['batch_id'], StringType)
        assert isinstance(field_dict['extraction_timestamp'], TimestampType)

    def test_schema_can_create_dataframe(self, spark_session, sample_flight_dict):
        """On doit pouvoir créer un DataFrame avec ce schéma."""
        # Créer une liste de tuples (non dict_values)
        data = [tuple(sample_flight_dict.values())]
        df = spark_session.createDataFrame(data, schema=schema_flights_raw)

        # Vérifier que le DataFrame a les bonnes colonnes
        assert df is not None
        assert len(df.columns) == len(schema_flights_raw.fields)
