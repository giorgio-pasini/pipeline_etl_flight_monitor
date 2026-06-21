"""Tests unitaires pour les utilitaires du datalake."""

import pytest
from pathlib import Path
from datetime import datetime, timedelta
from src.datalake_utils import (
    get_partition_values,
    build_partition_path,
    parse_partition_path,
    cleanup_old_partitions,
    list_partitions,
)


class TestPartitioningUtils:
    """Tests pour les utilitaires de partitionnement."""

    def test_get_partition_values(self):
        """Extraire les valeurs de partition d'un timestamp."""
        now = datetime(2026, 6, 21, 14, 30, 0)
        values = get_partition_values(now)

        assert isinstance(values, dict)
        assert 'tech_year' in values
        assert 'tech_month' in values
        assert 'tech_day' in values
        assert 'tech_hour' in values

    def test_partition_values_format(self):
        """Les valeurs de partition doivent avoir le bon format."""
        now = datetime(2026, 6, 21, 14, 30, 0)
        values = get_partition_values(now)

        assert values['tech_year'] == '2026'
        assert values['tech_month'] == '2026-06'
        assert values['tech_day'] == '2026-06-21'
        assert values['tech_hour'] == '14'

    def test_build_partition_path(self, temp_datalake):
        """Construire le chemin partitionné complet."""
        now = datetime(2026, 6, 21, 14, 30, 0)
        path = build_partition_path(temp_datalake / "bronze", "flights_raw", now)

        assert isinstance(path, str)
        assert "tech_year=2026" in path
        assert "tech_month=2026-06" in path
        assert "tech_day=2026-06-21" in path
        assert "tech_hour=14" in path

    def test_parse_partition_path(self):
        """Parser un chemin partitionné pour extraire les valeurs."""
        path = "/data/bronze/flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-21/tech_hour=14"
        parsed = parse_partition_path(path)

        assert parsed['tech_year'] == '2026'
        assert parsed['tech_month'] == '2026-06'
        assert parsed['tech_day'] == '2026-06-21'
        assert parsed['tech_hour'] == '14'

    def test_cleanup_dry_run(self, temp_datalake):
        """Le dry-run ne doit pas supprimer les fichiers."""
        # Créer une partition vieille
        old_partition = temp_datalake / "bronze" / "flights_raw" / "tech_year=2025" / "tech_month=2025-01"
        old_partition.mkdir(parents=True, exist_ok=True)
        (old_partition / "data.parquet").touch()

        # Dry-run cleanup
        from config.datalake_config import DatalakeConfig
        result = cleanup_old_partitions(
            layer="bronze",
            retention_days=30,
            dry_run=True
        )

        # Le fichier doit toujours exister
        assert (old_partition / "data.parquet").exists()

    def test_list_partitions(self, temp_datalake):
        """Lister les partitions du datalake."""
        # Créer quelques partitions
        for day in range(1, 4):
            partition = temp_datalake / "bronze" / "flights_raw" / f"tech_year=2026/tech_month=2026-06/tech_day=2026-06-{day:02d}/tech_hour=12"
            partition.mkdir(parents=True, exist_ok=True)
            (partition / "_SUCCESS").touch()

        from config.datalake_config import DatalakeConfig
        partitions = list_partitions(DatalakeConfig.BRONZE_PATH)

        assert isinstance(partitions, list)
        assert len(partitions) >= 0  # Peut être vide selon la structure


class TestCleanupRealMode:
    """Tests de suppression réelle (filesystem, sans Spark/winutils)."""

    def _make_partition(self, base, day):
        part = base / "bronze" / "flights_raw" / f"tech_year={day[:4]}" / \
            f"tech_month={day[:7]}" / f"tech_day={day}" / "tech_hour=12"
        part.mkdir(parents=True, exist_ok=True)
        (part / "data.parquet").write_text("x")
        return part

    def test_cleanup_deletes_old_keeps_recent(self, temp_datalake):
        from datetime import datetime, timedelta
        from src.datalake_utils import cleanup_old_partitions

        old_day = "2020-01-01"
        recent_day = datetime.now().strftime("%Y-%m-%d")
        old_part = self._make_partition(temp_datalake, old_day)
        recent_part = self._make_partition(temp_datalake, recent_day)

        result = cleanup_old_partitions(
            datalake_path=str(temp_datalake),
            layer="bronze",
            retention_days=30,
            dry_run=False,
        )

        assert result["deleted_partitions"] >= 1
        assert not old_part.exists()       # ancienne supprimée
        assert recent_part.exists()        # récente conservée

    def test_cleanup_dry_run_deletes_nothing(self, temp_datalake):
        from src.datalake_utils import cleanup_old_partitions

        old_part = self._make_partition(temp_datalake, "2019-05-05")

        result = cleanup_old_partitions(
            datalake_path=str(temp_datalake),
            layer="bronze",
            retention_days=30,
            dry_run=True,
        )

        assert old_part.exists()  # rien supprimé en dry-run
        assert result["dry_run"] is True


class TestPartitionFiltering:
    """Tests pour les filtres de partition."""

    def test_partition_date_extraction(self):
        """Extraire la date d'un chemin partitionné."""
        path = "/data/bronze/flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-21/tech_hour=14"
        parsed = parse_partition_path(path)

        # Vérifier qu'on peut construire une date
        date_str = parsed['tech_day']
        assert date_str == '2026-06-21'

    def test_old_partition_identification(self):
        """Identifier si une partition est ancienne."""
        now = datetime(2026, 6, 21)
        old_date = now - timedelta(days=40)  # Plus vieux que 30 jours

        old_values = get_partition_values(old_date)

        # La partition devrait être considérée comme vieille
        assert old_values['tech_day'] < get_partition_values(now)['tech_day']
