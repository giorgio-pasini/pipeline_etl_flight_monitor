"""Tests pour le chargement bulk + cache des dimensions de référence."""

import os
import time
import urllib.error
from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from src.dimension_loader import (
    _cache_fresh,
    load_dim_airlines,
    _read_static_airports,
    ensure_airports_dataset,
)


def _tf(x):
    try:
        return float(x) if x else None
    except (TypeError, ValueError):
        return None


class TestStaticAirports:
    def test_parses_openflights_format(self, tmp_path):
        # Format OpenFlights : id,name,city,country,IATA,ICAO,lat,lon,alt,...
        f = tmp_path / "airports.dat"
        f.write_text(
            '1,"John F Kennedy Intl","New York","United States","JFK","KJFK",40.63,-73.77,13,-5,"A","x","airport","src"\n'
            '2,"No IATA","X","France","\\N","LFXX",1.0,2.0,0,1,"E","y","airport","src"\n'
            '3,"Charles de Gaulle","Paris","France","CDG","LFPG",49.0,2.55,392,1,"E","z","airport","src"\n',
            encoding="utf-8",
        )
        rows = _read_static_airports(str(f), _tf)
        by_iata = {r[0]: r for r in rows}
        assert set(by_iata) == {"JFK", "CDG"}  # ligne sans IATA (\N) ignorée
        assert by_iata["JFK"][3] == "United States"
        assert by_iata["JFK"][4] == 40.63
        assert by_iata["CDG"][5] == 2.55

    def test_missing_file_returns_empty(self, tmp_path):
        assert _read_static_airports(str(tmp_path / "nope.dat"), _tf) == []


class TestEnsureAirportsDataset:
    URL = "https://example.invalid/airports.dat"
    PAYLOAD = b'1,"JFK Intl","New York","United States","JFK","KJFK",40.63,-73.77,13,-5,"A","x","airport","s"\n'

    def _patch_urlopen(self, monkeypatch, payload=PAYLOAD, exc=None):
        """Remplace urllib.request.urlopen ; renvoie un compteur d'appels."""
        calls = {"n": 0}

        @contextmanager
        def fake_urlopen(url, timeout=None):
            calls["n"] += 1
            if exc is not None:
                raise exc
            yield Mock(read=lambda: payload)

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
        return calls

    def test_fresh_file_no_download(self, tmp_path, monkeypatch):
        f = tmp_path / "airports.dat"
        f.write_bytes(self.PAYLOAD)
        calls = self._patch_urlopen(monkeypatch)
        assert ensure_airports_dataset(str(f), self.URL, max_age_days=14) is True
        assert calls["n"] == 0  # frais → aucun téléchargement

    def test_absent_downloads(self, tmp_path, monkeypatch):
        f = tmp_path / "sub" / "airports.dat"  # dossier parent inexistant
        calls = self._patch_urlopen(monkeypatch)
        assert ensure_airports_dataset(str(f), self.URL, max_age_days=14) is True
        assert calls["n"] == 1
        assert f.exists() and f.read_bytes() == self.PAYLOAD

    def test_stale_refreshes(self, tmp_path, monkeypatch):
        f = tmp_path / "airports.dat"
        f.write_bytes(b"old")
        old = time.time() - 15 * 86400  # 15 jours
        os.utime(f, (old, old))
        calls = self._patch_urlopen(monkeypatch, payload=self.PAYLOAD)
        assert ensure_airports_dataset(str(f), self.URL, max_age_days=14) is True
        assert calls["n"] == 1
        assert f.read_bytes() == self.PAYLOAD  # rafraîchi

    def test_network_failure_keeps_existing(self, tmp_path, monkeypatch):
        f = tmp_path / "airports.dat"
        f.write_bytes(b"existing")
        old = time.time() - 30 * 86400
        os.utime(f, (old, old))  # périmé → tente un téléchargement
        self._patch_urlopen(monkeypatch, exc=urllib.error.URLError("boom"))
        assert ensure_airports_dataset(str(f), self.URL, max_age_days=14) is True
        assert f.read_bytes() == b"existing"  # fallback : on garde l'existant

    def test_network_failure_absent_returns_false(self, tmp_path, monkeypatch):
        f = tmp_path / "airports.dat"
        self._patch_urlopen(monkeypatch, exc=urllib.error.URLError("boom"))
        assert ensure_airports_dataset(str(f), self.URL, max_age_days=14) is False
        assert not f.exists()


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
