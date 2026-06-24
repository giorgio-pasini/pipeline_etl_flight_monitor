"""Tests unitaires pour les utilitaires du datalake."""

from datetime import datetime
from src.datalake_utils import get_partition_values, cleanup_old_partitions


class TestPartitioningUtils:
    """Tests pour les utilitaires de partitionnement."""

    def test_partition_values_format(self):
        """Les valeurs de partition doivent avoir le bon format."""
        now = datetime(2026, 6, 21, 14, 30, 0)
        values = get_partition_values(now)

        assert values['tech_year'] == '2026'
        assert values['tech_month'] == '2026-06'
        assert values['tech_day'] == '2026-06-21'
        assert values['tech_hour'] == '14'


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
