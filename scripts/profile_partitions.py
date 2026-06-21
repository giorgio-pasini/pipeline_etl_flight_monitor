"""
Script pour profiler et optimiser le partitionnement du datalake.

Usage:
    python scripts/profile_partitions.py --layer bronze
    python scripts/profile_partitions.py --layer silver
    python scripts/profile_partitions.py --layer gold
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Import local
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from config.datalake_config import DatalakeConfig
from src.partitioning_optimizer import PartitioningOptimizer, save_optimization_report


def setup_logging(verbose=False):
    """Configurer le logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s"
    )
    return logging.getLogger(__name__)


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="Profile and optimize datalake partitioning"
    )

    parser.add_argument(
        "--layer",
        type=str,
        choices=["bronze", "silver", "gold"],
        default="bronze",
        help="Datalake layer to profile (default: bronze)"
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output report path (default: datalake/_logs/partition_profile_<timestamp>.json)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging"
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    logger.info("=" * 70)
    logger.info("Partitioning Optimizer")
    logger.info(f"Layer: {args.layer.upper()}")
    logger.info("=" * 70)

    # Créer session Spark
    spark = SparkSession.builder \
        .appName("partition-optimizer") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Sélectionner le chemin
        if args.layer == "bronze":
            datalake_path = DatalakeConfig.get_bronze_flights_path()
        elif args.layer == "silver":
            datalake_path = DatalakeConfig.get_silver_fact_flights_path()
        else:  # gold
            datalake_path = f"{DatalakeConfig.GOLD_PATH}/kpi_airline_volumes"

        logger.info(f"Profiling: {datalake_path}")

        # Profiler
        optimizer = PartitioningOptimizer(spark, DatalakeConfig)
        report = optimizer.generate_optimization_report(datalake_path)

        # Sauvegarder le rapport
        if args.output is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output = f"{DatalakeConfig.LOG_PATH}/partition_profile_{args.layer}_{timestamp}.json"

        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        save_optimization_report(report, args.output)

        # Afficher un résumé
        logger.info("\n" + "=" * 70)
        logger.info("PARTITION OPTIMIZATION REPORT SUMMARY")
        logger.info("=" * 70)

        if "error" in report:
            logger.error(f"Error: {report['error']}")
        else:
            skew = report.get("skew_analysis", {})
            size = report.get("size_analysis", {})
            spark_rec = report.get("spark_recommendations", {})

            logger.info(f"\nSKEW ANALYSIS:")
            logger.info(f"  Partitions: {skew.get('partition_count', 'N/A')}")
            logger.info(f"  Skew ratio: {skew.get('skew_ratio', 'N/A'):.2f}x")
            logger.info(f"  Recommendation: {skew.get('recommendation', 'N/A')}")

            logger.info(f"\nSIZE ANALYSIS:")
            logger.info(f"  Total rows: {size.get('total_rows', 'N/A'):,}")
            logger.info(f"  Total size: {size.get('total_size_mb_estimated', 'N/A'):.0f} MB")
            logger.info(f"  Avg partition: {size.get('avg_partition_size_mb', 'N/A'):.0f} MB")

            logger.info(f"\nSPARK RECOMMENDATIONS:")
            logger.info(f"  Recommended shuffle partitions: {spark_rec.get('recommended_shuffle_partitions', 'N/A')}")
            for note in spark_rec.get('notes', []):
                logger.info(f"    - {note}")

        logger.info(f"\nReport saved to: {args.output}")
        logger.info("=" * 70)

        return 0

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
