"""
Utilitaires pour le datalake : valeurs de partitionnement horodatées.
"""

from datetime import datetime
import logging


def get_partition_values(timestamp: datetime) -> dict:
    """
    Extraire les valeurs de partitionnement (tech_year, tech_month, etc.) d'un timestamp.

    Args:
        timestamp: datetime objet (ex: 2026-06-21 14:30:00)

    Returns:
        dict avec clés tech_year, tech_month, tech_day, tech_hour

    Example:
        >>> ts = datetime(2026, 6, 21, 14, 30, 0)
        >>> get_partition_values(ts)
        {
            'tech_year': '2026',
            'tech_month': '2026-06',
            'tech_day': '2026-06-21',
            'tech_hour': '14'
        }
    """
    return {
        "tech_year": timestamp.strftime("%Y"),
        "tech_month": timestamp.strftime("%Y-%m"),
        "tech_day": timestamp.strftime("%Y-%m-%d"),
        "tech_hour": timestamp.strftime("%H"),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ts = datetime(2026, 6, 21, 14, 30, 0)
    print(f"Partition values: {get_partition_values(ts)}")
