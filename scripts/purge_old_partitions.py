"""
Script de purge des anciennes partitions selon la rétention configurée.

Fournit des fonctions pour :
- Nettoyer les partitions Bronze/Silver/Gold qui sont trop vieilles
- Estimer l'espace disque libéré
- Mode dry-run pour vérifier avant suppression

À exécuter régulièrement via cron (ex: toutes les nuits).

Usage:
    # Dry-run pour voir ce qui serait supprimé
    python scripts/purge_old_partitions.py --dry-run --verbose

    # Vraie suppression
    python scripts/purge_old_partitions.py --layer bronze --datalake-root /path/to/datalake

    # Supprimer toutes les couches
    python scripts/purge_old_partitions.py --all-layers
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ajouter src au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.pipeline_config import PipelineConfig
from src.datalake_utils import cleanup_old_partitions, estimate_storage_usage, list_partitions


def setup_logging(verbose=False):
    """Configurer le logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s"
    )
    return logging.getLogger(__name__)


def format_bytes(bytes_val):
    """Formater une taille en bytes en format lisible."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def show_partition_summary(datalake_path: str, logger: logging.Logger):
    """Afficher un résumé des partitions existantes."""
    logger.info("\n=== Résumé des partitions ===\n")

    tables = [
        ("bronze", "flights_raw"),
        ("silver", "fact_flights"),
        ("gold", "kpi_airline_volumes"),
    ]

    for layer, table in tables:
        table_path = f"{layer}/{table}"
        partitions = list_partitions(datalake_path, table_path)
        if partitions:
            oldest = partitions[-1]  # Dernier = plus ancien (sorted DESC)
            newest = partitions[0]

            logger.info(f"{layer}/{table}:")
            logger.info(f"  Total partitions: {len(partitions)}")
            logger.info(f"  Newest: {newest['tech_day']} {newest['tech_hour']:02d}:00")
            logger.info(f"  Oldest: {oldest['tech_day']} {oldest['tech_hour']:02d}:00")
        else:
            logger.info(f"{layer}/{table}: (aucune partition trouvée)")


def show_retention_schedule(config: PipelineConfig, logger: logging.Logger):
    """Afficher le calendrier de rétention."""
    logger.info("\n=== Calendrier de rétention ===\n")

    now = datetime.now()

    layers = {
        "bronze": config.BRONZE_RETENTION_DAYS,
        "silver": config.SILVER_RETENTION_DAYS,
        "gold": config.GOLD_RETENTION_DAYS,
    }

    for layer, days in layers.items():
        cutoff = now - timedelta(days=days)
        logger.info(f"{layer:8} : conserve {days:3d} jours | coupe avant {cutoff.strftime('%Y-%m-%d')}")


def purge_single_layer(
    datalake_path: str,
    layer: str,
    retention_days: int,
    dry_run: bool = True,
    logger: logging.Logger = None
) -> dict:
    """Purge une seule couche."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"\n=== Purge {layer} ===")
    logger.info(f"Rétention : {retention_days} jours")
    logger.info(f"Mode : {'DRY-RUN' if dry_run else 'SUPPRESSION RÉELLE'}")

    stats = cleanup_old_partitions(
        datalake_path,
        layer,
        retention_days,
        dry_run=dry_run
    )

    if stats.get("error"):
        logger.error(f"Erreur : {stats['error']}")
    else:
        logger.info(f"Partitions traitées : {stats['deleted_partitions']}")
        logger.info(f"Espace libéré : {format_bytes(stats['freed_bytes'])}")

    return stats


def estimate_and_report(datalake_path: str, logger: logging.Logger):
    """Estimer et rapporter l'usage du stockage."""
    logger.info("\n=== Estimation stockage ===\n")

    usage = estimate_storage_usage(datalake_path)

    logger.info(f"Bronze:  {format_bytes(usage['bronze_bytes'])}")
    logger.info(f"Silver:  {format_bytes(usage['silver_bytes'])}")
    logger.info(f"Gold:    {format_bytes(usage['gold_bytes'])}")
    logger.info(f"TOTAL:   {format_bytes(usage['total_bytes'])}")

    return usage


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="Purger les anciennes partitions du datalake selon la rétention configurée."
    )

    parser.add_argument(
        "--datalake-root",
        type=str,
        default=None,
        help="Chemin racine du datalake (override env var)"
    )

    parser.add_argument(
        "--layer",
        type=str,
        choices=["bronze", "silver", "gold"],
        default=None,
        help="Couche à purger (default: toutes)"
    )

    parser.add_argument(
        "--all-layers",
        action="store_true",
        help="Purger toutes les couches (bronze, silver, gold)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Mode dry-run (afficher ce qui serait supprimé, ne rien supprimer)"
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Mode suppression réelle (ATTENTION: DESTRUCTIF)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Logs verbeux (debug)"
    )

    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Seulement afficher l'usage du stockage (ne rien supprimer)"
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    # Override config si nécessaire
    if args.datalake_root:
        PipelineConfig.DATALAKE_ROOT = args.datalake_root

    PipelineConfig.validate()

    logger.info("=== Purge des partitions anciennes ===\n")
    logger.info(f"Datalake root: {PipelineConfig.DATALAKE_ROOT}\n")

    # Mode dry-run par défaut
    dry_run = not args.execute
    if args.execute:
        logger.warning("⚠️  MODE SUPPRESSION RÉELLE - LES DONNÉES SERONT SUPPRIMÉES!")
        logger.warning("⚠️  Assurez-vous d'avoir une sauvegarde.\n")

    if args.estimate_only:
        logger.info("Mode: estimation du stockage uniquement\n")
        estimate_and_report(PipelineConfig.DATALAKE_ROOT, logger)
        show_partition_summary(PipelineConfig.DATALAKE_ROOT, logger)
        return 0

    # Afficher le résumé et le calendrier
    show_partition_summary(PipelineConfig.DATALAKE_ROOT, logger)
    show_retention_schedule(PipelineConfig, logger)

    # Décider quelles couches purger
    if args.all_layers:
        layers_to_purge = [
            ("bronze", PipelineConfig.BRONZE_RETENTION_DAYS),
            ("silver", PipelineConfig.SILVER_RETENTION_DAYS),
            ("gold", PipelineConfig.GOLD_RETENTION_DAYS),
        ]
    elif args.layer:
        retention_map = {
            "bronze": PipelineConfig.BRONZE_RETENTION_DAYS,
            "silver": PipelineConfig.SILVER_RETENTION_DAYS,
            "gold": PipelineConfig.GOLD_RETENTION_DAYS,
        }
        layers_to_purge = [(args.layer, retention_map[args.layer])]
    else:
        layers_to_purge = []
        logger.info("\nAucune couche spécifiée. Usage : --layer bronze|silver|gold ou --all-layers")
        return 1

    # Exécuter les purges
    total_freed = 0
    total_deleted = 0

    for layer, retention_days in layers_to_purge:
        stats = purge_single_layer(
            PipelineConfig.DATALAKE_ROOT,
            layer,
            retention_days,
            dry_run=dry_run,
            logger=logger
        )

        if not stats.get("error"):
            total_freed += stats.get("freed_bytes", 0)
            total_deleted += stats.get("deleted_partitions", 0)

    # Résumé
    logger.info("\n" + "="*60)
    logger.info("✅ Purge complétée\n")
    logger.info(f"Partitions supprimées : {total_deleted}")
    logger.info(f"Espace libéré : {format_bytes(total_freed)}")
    logger.info(f"Mode : {'DRY-RUN (aucune suppression réelle)' if dry_run else 'SUPPRESSION RÉELLE'}")

    if dry_run:
        logger.info("\n💡 Pour exécuter la suppression réelle, relancez avec --execute")

    # Afficher l'usage du stockage après
    estimate_and_report(PipelineConfig.DATALAKE_ROOT, logger)

    return 0


if __name__ == "__main__":
    sys.exit(main())
