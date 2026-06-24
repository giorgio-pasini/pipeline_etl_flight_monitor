"""Tests unitaires pour les utilitaires du datalake."""

from datetime import datetime
from src.datalake_utils import get_partition_values


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
