"""Tests pour le chargement bulk + cache des dimensions de référence."""

import time
from unittest.mock import Mock

import pytest

from src.dimension_loader import _cache_fresh, load_dim_airlines


class TestCacheFresh:
    def test_missing_path_not_fresh(self, tmp_path):
        assert _cache_fresh(str(tmp_path / "nope"), 7) is False

    def test_empty_dir_not_fresh(self, tmp_path):
        d = tmp_path / "dim"
        d.mkdir()
        assert _cache_fresh(str(d), 7) is False

    def test_recent_parquet_is_fresh(self, tmp_path):
        d = tmp_path / "dim"
        d.mkdir()
        (d / "part-0.parquet").write_text("x")
        assert _cache_fresh(str(d), 7) is True

    def test_old_parquet_not_fresh(self, tmp_path):
        d = tmp_path / "dim"
        d.mkdir()
        f = d / "part-0.parquet"
        f.write_text("x")
        # vieillir le fichier de 10 jours
        old = time.time() - 10 * 86400
        import os
        os.utime(f, (old, old))
        assert _cache_fresh(str(d), 7) is False


class TestLoadDimAirlines:
    def test_builds_dataframe_from_api(self, spark_session, temp_datalake, parquet_write_supported):
        if not parquet_write_supported:
            pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

        from config.datalake_config import DatalakeConfig

        api = Mock()
        api.get_airlines.return_value = [
            {"Name": "Delta Air Lines", "ICAO": "DAL", "IATA": "DL", "n_aircrafts": 900},
            {"Name": "Air France", "ICAO": "AFR", "IATA": "AF", "n_aircrafts": 200},
            {"Name": "No ICAO", "ICAO": None, "IATA": "XX", "n_aircrafts": 1},  # ignoré
        ]

        df = load_dim_airlines(spark_session, api, DatalakeConfig)
        rows = {r["airline_icao"]: r for r in df.collect()}
        assert set(rows) == {"DAL", "AFR"}
        assert rows["DAL"]["airline_name"] == "Delta Air Lines"
        assert "last_updated" in df.columns
