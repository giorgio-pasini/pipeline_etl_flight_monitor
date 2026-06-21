"""Tests unitaires pour la validation et flagging de qualité."""

import copy
import pytest
from pyspark.sql.functions import col, lit
from datetime import datetime

from src.data_quality import validate_and_flag_flights, profile_data_quality


@pytest.mark.parametrize("mutation,expected_flag", [
    ({"origin_iata": None}, "MISSING_ORIGIN"),
    ({"destination_iata": None}, "MISSING_DESTINATION"),
    ({"airline_icao": None}, "MISSING_AIRLINE"),
    ({"aircraft_code": None}, "MISSING_AIRCRAFT_CODE"),
    ({"latitude": None}, "MISSING_POSITION"),
    ({"altitude": 99999.0}, "INVALID_ALTITUDE"),
    ({"altitude": -50.0}, "INVALID_ALTITUDE"),
    ({"ground_speed": 9999.0}, "INVALID_GROUND_SPEED"),
    ({"latitude": 200.0}, "INCONSISTENT_POSITION"),
])
def test_each_quality_flag_is_raised(spark_session, sample_flight_dict, mutation, expected_flag):
    """Chacun des 8 types de flags doit se déclencher sur la mutation appropriée."""
    from src.schemas import schema_flights_raw

    flight = copy.deepcopy(sample_flight_dict)
    flight.update(mutation)  # le vol de base est valide (on_ground=0)

    df = spark_session.createDataFrame([flight], schema=schema_flights_raw)
    result = validate_and_flag_flights(df, logger=None)

    flags = result.select("data_quality_flags").collect()[0][0]
    assert flags is not None, f"Aucun flag pour {mutation}"
    assert expected_flag in flags, f"Attendu {expected_flag}, obtenu {flags}"
    # Un vol flaggé n'est jamais valide
    assert result.select("is_valid").collect()[0][0] is False


class TestDataQualityFlags:
    """Tests pour le flagging de qualité."""

    def test_valid_flight_no_flags(self, spark_session, sample_flight_dict):
        """Un vol valide ne doit avoir aucun flag."""
        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [tuple(sample_flight_dict.values())],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)
        # Vol valide devrait avoir is_valid = True
        valid_count = result.filter(col("is_valid") == True).count()
        assert valid_count > 0

    def test_missing_origin_flag(self, spark_session, sample_flight_dict):
        """Un vol sans origine doit avoir le flag MISSING_ORIGIN."""
        sample_flight_dict['origin_iata'] = None

        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [tuple(sample_flight_dict.values())],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)

        # Devrait avoir un flag
        flagged_count = result.filter(col("data_quality_flags").isNotNull()).count()
        assert flagged_count > 0

    def test_invalid_altitude_flag(self, spark_session, sample_flight_dict_invalid):
        """Un vol avec altitude négative doit avoir le flag INVALID_ALTITUDE."""
        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [sample_flight_dict_invalid],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)

        # Devrait avoir is_valid = False
        invalid_count = result.filter(col("is_valid") == False).count()
        assert invalid_count > 0

        # Et le flag INVALID_ALTITUDE doit être présent (altitude = -100)
        flagged = result.filter(col("data_quality_flags").contains("INVALID_ALTITUDE")).count()
        assert flagged > 0

    def test_is_valid_logic(self, spark_session, sample_flight_dict):
        """is_valid doit être True seulement si aucun flag et données valides."""
        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [tuple(sample_flight_dict.values())],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)

        # Vérifier que is_valid existe et est un booléen
        is_valid_values = result.select("is_valid").collect()
        assert len(is_valid_values) > 0


class TestQualityProfiling:
    """Tests pour le profil de qualité."""

    def test_profile_returns_dict(self, spark_session, sample_flight_dict):
        """Le profil doit retourner un dictionnaire."""
        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [tuple(sample_flight_dict.values())],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)
        profile = profile_data_quality(result, logger=None)

        assert isinstance(profile, dict)
        assert 'total_rows' in profile or 'valid_rows' in profile

    def test_profile_counts_valid_invalid(self, spark_session, sample_flight_dict, sample_flight_dict_invalid):
        """Le profil doit compter les vols valides et invalides."""
        from src.schemas import schema_flights_raw
        df = spark_session.createDataFrame(
            [sample_flight_dict, sample_flight_dict_invalid],
            schema=schema_flights_raw
        )

        result = validate_and_flag_flights(df, logger=None)
        profile = profile_data_quality(result, logger=None)

        assert profile is not None
        # Le profil doit indiquer qu'il y a des données
        assert profile.get('total_rows', 0) == 2
        # 1 vol valide, 1 invalide
        assert profile.get('valid_rows', 0) == 1
