"""
Utilitaires pour le datalake : gestion des chemins partitionnés, timestamps, nettoyage.

Fournit :
- Génération de chemins avec partitions horodatées
- Parsing de chemins partitionnés
- Cleanup de partitions anciennes
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import logging
import os
import shutil
import re

logger = logging.getLogger(__name__)


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


def build_partition_path(base_path, table_name: str, timestamp: datetime) -> str:
    """
    Construire un chemin partitionné complet pour une table et une timestamp.

    Args:
        base_path: chemin de la couche (ex: "datalake/bronze")
        table_name: nom de la table (ex: "flights_raw")
        timestamp: datetime

    Returns:
        Chemin partitionné complet
        (ex: "datalake/bronze/flights_raw/tech_year=2026/tech_month=2026-06/...")

    Example:
        >>> build_partition_path("datalake/bronze", "flights_raw", datetime(2026, 6, 21, 14))
        "datalake/bronze/flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-21/tech_hour=14"
    """
    parts = get_partition_values(timestamp)
    path = f"{base_path}/{table_name}"
    for col in ["tech_year", "tech_month", "tech_day", "tech_hour"]:
        path = f"{path}/{col}={parts[col]}"
    return path


def build_partition_path_gold_kpi(base_path: str, kpi_date: str, kpi_hour: int) -> str:
    """
    Construire un chemin Gold partitionné par (kpi_date, kpi_hour).

    Args:
        base_path: chemin de base KPI (ex: "datalake/gold/kpi_airline_volumes")
        kpi_date: date au format YYYY-MM-DD
        kpi_hour: heure 0-23

    Returns:
        Chemin complet (ex: "datalake/gold/kpi_airline_volumes/kpi_date=2026-06-21/kpi_hour=14")
    """
    return f"{base_path}/kpi_date={kpi_date}/kpi_hour={kpi_hour:02d}"


def parse_partition_path(full_path: str) -> Optional[dict]:
    """
    Parser un chemin partitionné pour extraire les valeurs de partition.

    Args:
        full_path: chemin avec partitions (ex: "datalake/bronze/flights_raw/tech_year=2026/...")

    Returns:
        dict {tech_year: "2026", tech_month: "2026-06", ...} ou None si aucune partition

    Example:
        >>> parse_partition_path("datalake/bronze/flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-21/tech_hour=14")
        {"tech_year": "2026", "tech_month": "2026-06", "tech_day": "2026-06-21", "tech_hour": "14"}
    """
    # Regex : extraire les partitions key=value
    pattern = r"([a-z_]+)=([a-zA-Z0-9\-]+)"
    matches = re.findall(pattern, full_path)

    if not matches:
        return None

    return {k: v for k, v in matches}


def get_partition_datetime(partition_dict: dict) -> Optional[datetime]:
    """
    Reconstituer un datetime à partir d'un dict de partitions.

    Args:
        partition_dict: {tech_year, tech_month, tech_day, tech_hour}

    Returns:
        datetime ou None si parse échoue
    """
    try:
        if "tech_day" in partition_dict and "tech_hour" in partition_dict:
            day_str = partition_dict["tech_day"]  # YYYY-MM-DD
            hour_str = partition_dict["tech_hour"]  # HH
            return datetime.strptime(f"{day_str} {hour_str}", "%Y-%m-%d %H")
    except (KeyError, ValueError) as e:
        logger.warning(f"Impossible de parser partitions : {e}")

    return None


def cleanup_old_partitions(
    datalake_path: Optional[str] = None,
    layer: str = "bronze",  # "bronze", "silver", "gold"
    retention_days: int = 30,
    dry_run: bool = True,
) -> dict:
    """
    Supprimer les partitions antérieures à retention_days.

    Args:
        datalake_path: racine du datalake (défaut: DatalakeConfig.DATALAKE_ROOT)
        layer: "bronze", "silver" ou "gold"
        retention_days: nombre de jours à conserver
        dry_run: si True, ne pas supprimer, juste lister

    Returns:
        dict avec stats de suppression

    Example:
        >>> cleanup_old_partitions("/path/to/datalake", "bronze", 30, dry_run=False)
        {
            "layer": "bronze",
            "retention_days": 30,
            "deleted_partitions": 5,
            "freed_bytes": 1024000,
            "dry_run": False
        }
    """
    if datalake_path is None:
        from config.datalake_config import DatalakeConfig
        datalake_path = DatalakeConfig.DATALAKE_ROOT

    layer_path = Path(datalake_path) / layer
    if not layer_path.exists():
        logger.warning(f"Chemin non trouvé : {layer_path}")
        return {"error": f"Path not found: {layer_path}"}

    cutoff_date = datetime.now() - timedelta(days=retention_days)
    deleted_count = 0
    freed_bytes = 0

    # Chercher tous les dossiers tech_day ou kpi_date (os.walk : compatible Python < 3.12)
    date_pattern = r"(tech_day|kpi_date)=(\d{4}-\d{2}-\d{2})"

    for root, dirs, files in os.walk(str(layer_path)):
        for dir_name in dirs:
            match = re.search(date_pattern, dir_name)
            if match:
                date_str = match.group(2)
                try:
                    partition_date = datetime.strptime(date_str, "%Y-%m-%d")
                    if partition_date < cutoff_date:
                        dir_path = Path(root) / dir_name
                        size = sum(f.stat().st_size for f in dir_path.rglob("*") if f.is_file())

                        if dry_run:
                            logger.info(f"[DRY-RUN] Would delete: {dir_path}")
                        else:
                            shutil.rmtree(dir_path)
                            logger.info(f"Deleted: {dir_path}")

                        deleted_count += 1
                        freed_bytes += size
                except (ValueError, OSError) as e:
                    logger.warning(f"Erreur lors du traitement {dir_name}: {e}")

    return {
        "layer": layer,
        "retention_days": retention_days,
        "deleted_partitions": deleted_count,
        "freed_bytes": freed_bytes,
        "dry_run": dry_run,
    }


def list_partitions(datalake_path: str, table_name: str = "") -> list:
    """
    Lister toutes les partitions existantes pour une table.

    Args:
        datalake_path: racine du datalake (ou chemin de couche)
        table_name: nom relatif de la table (ex: "bronze/flights_raw"). Optionnel.

    Returns:
        list de dicts {tech_year, tech_month, tech_day, tech_hour} ou []
    """
    table_path = Path(datalake_path) / table_name if table_name else Path(datalake_path)
    if not table_path.exists():
        return []

    partitions = []
    year_dirs = list(table_path.glob("tech_year=*"))

    for year_dir in year_dirs:
        for month_dir in year_dir.glob("tech_month=*"):
            for day_dir in month_dir.glob("tech_day=*"):
                for hour_dir in day_dir.glob("tech_hour=*"):
                    # Extraire les valeurs
                    try:
                        parts = {
                            "tech_year": year_dir.name.split("=")[1],
                            "tech_month": month_dir.name.split("=")[1],
                            "tech_day": day_dir.name.split("=")[1],
                            "tech_hour": hour_dir.name.split("=")[1],
                        }
                        partitions.append(parts)
                    except IndexError:
                        continue

    return sorted(partitions, key=lambda p: (p["tech_day"], p["tech_hour"]), reverse=True)


def estimate_storage_usage(datalake_path: str) -> dict:
    """
    Estimer l'usage du stockage par couche.

    Returns:
        dict avec {bronze_bytes, silver_bytes, gold_bytes, total_bytes}
    """
    stats = {
        "bronze_bytes": 0,
        "silver_bytes": 0,
        "gold_bytes": 0,
        "total_bytes": 0,
    }

    for layer in ["bronze", "silver", "gold"]:
        layer_path = Path(datalake_path) / layer
        if layer_path.exists():
            size = sum(f.stat().st_size for f in layer_path.rglob("*") if f.is_file())
            stats[f"{layer}_bytes"] = size
            stats["total_bytes"] += size

    return stats


if __name__ == "__main__":
    # Test
    import sys
    logging.basicConfig(level=logging.INFO)

    ts = datetime(2026, 6, 21, 14, 30, 0)
    print(f"Partition values: {get_partition_values(ts)}")
    print(f"Full path: {build_partition_path('datalake/bronze', 'flights_raw', ts)}")

    kpi_path = build_partition_path_gold_kpi("datalake/gold/kpi_airline_volumes", "2026-06-21", 14)
    print(f"Gold KPI path: {kpi_path}")
