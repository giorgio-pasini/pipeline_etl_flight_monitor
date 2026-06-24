"""Tests unitaires du dashboard (helpers de lecture, sans Spark)."""

import pandas as pd

import dashboard


def test_read_kpi_keeps_only_latest_snapshot(tmp_path, monkeypatch):
    """La table KPI peut contenir un snapshot par jour ; `_read_kpi` ne renvoie
    que le calcul le plus récent (max computed_at), colonnes techniques retirées."""
    kpi_dir = tmp_path / "kpi_airline_volumes"
    kpi_dir.mkdir()
    df = pd.DataFrame({
        "airline_icao": ["AAA", "BBB"],
        "active_flights_count": [10, 20],
        "computed_at": pd.to_datetime(["2026-06-23T10:00:00", "2026-06-23T12:00:00"]),
        "tech_year": ["2026", "2026"],
        "tech_month": ["2026-06", "2026-06"],
        "tech_day": ["2026-06-23", "2026-06-23"],
    })
    df.to_parquet(kpi_dir / "part-0.parquet")

    monkeypatch.setattr(dashboard, "GOLD_DIR", str(tmp_path))
    out = dashboard._read_kpi("kpi_airline_volumes")

    assert out is not None
    # Seul le snapshot le plus récent (12:00) est conservé
    assert list(out["airline_icao"]) == ["BBB"]
    assert list(out["active_flights_count"]) == [20]
    # Colonnes techniques retirées
    for c in ("computed_at", "tech_year", "tech_month", "tech_day"):
        assert c not in out.columns


def test_read_kpi_absent_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "GOLD_DIR", str(tmp_path))
    assert dashboard._read_kpi("kpi_inexistant") is None
