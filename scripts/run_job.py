"""
Job ETL final — point d'entrée unique du pipeline (Étape 7).

Ce script est un wrapper CLI mince autour de `src.batch_job.run_batch`, qui
contient TOUTE la logique du pipeline (extraction → validation → Bronze →
Silver/Gold → métriques). On évite ainsi toute duplication de logique.

Usage:
    python scripts/run_job.py
    python scripts/run_job.py --with-silver-gold
    python scripts/run_job.py --zones europe northamerica
"""

import sys
import logging
import argparse
from pathlib import Path

# Import local modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig
from src.batch_job import run_batch, create_spark_session, setup_logging


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline - Job final (orchestré toutes les 2h via scheduler)"
    )

    parser.add_argument(
        "--zones",
        type=str,
        nargs="+",
        default=["global"],
        help="Zones à collecter (default: global)"
    )

    parser.add_argument(
        "--with-silver-gold",
        action="store_true",
        help="Lancer aussi la transformation Silver + Gold (KPIs)"
    )

    parser.add_argument(
        "--datalake-root",
        type=str,
        default=None,
        help="Override DATALAKE_ROOT"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Logs verbeux"
    )

    args = parser.parse_args()

    if args.datalake_root:
        DatalakeConfig.DATALAKE_ROOT = args.datalake_root

    DatalakeConfig.validate()

    logger = setup_logging(DatalakeConfig)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    spark = create_spark_session(DatalakeConfig)

    try:
        success = run_batch(
            spark,
            DatalakeConfig,
            logger,
            zones=args.zones,
            with_silver_gold=args.with_silver_gold,
        )
        return 0 if success else 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
