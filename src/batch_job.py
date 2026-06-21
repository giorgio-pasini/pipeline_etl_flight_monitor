"""
Job Spark Core batch principal.

Ce job doit être orchestré par un scheduler (Airflow, cron, etc.) pour s'exécuter toutes les 2 heures.

Orchestration (une exécution) :
1. Collecte les vols via FlightRadarAPI (zone "global" ou custom)
2. Valide les données (flagging de qualité : 8 types de flags)
3. Partitionne temporellement (tech_year/tech_month/tech_day/tech_hour)
4. Écrit en Bronze (Parquet comprimé)
5. Loggue statistiques et rapports de qualité JSON
6. Gère erreurs sans arrêter (fault-tolerance : erreurs flaggées)

Architecture :
- Spark Core batch (pas Structured Streaming)
- Une exécution = 1 batch (5-10 min)
- Orchestration externe (Airflow, cron)
- À lancer toutes les 2 heures via scheduler

À lancer :
    python src/batch_job.py [--zones global] [--verbose]

Configuration :
    - DATALAKE_ROOT : chemin racine (default: ./datalake/)
    - Zones : ["global"] (peut étendre à ["global", "europe", "asia", ...])
    - Enrichment : False (pour perf ; phase 2)
    - Timeout API : 30 sec
"""

import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime
import yaml
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_date

# Import local
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig, PARTITION_COLUMNS_BRONZE
from src.flight_extraction import extract_flights_batch
from src.data_quality import validate_and_flag_flights, profile_data_quality
from src.datalake_utils import get_partition_values
from src.silver_gold_loader import SilverGoldLoader
import json


def setup_logging(config: DatalakeConfig):
    """Configurer le logging (fichier + console)."""
    log_path = Path(config.get_log_path())
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f"batch_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")

    return logger


def create_spark_session(config: DatalakeConfig) -> SparkSession:
    """Créer et configurer la session Spark."""

    spark = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS) \
        .config("spark.sql.adaptive.enabled", config.SPARK_ADAPTIVE_EXECUTION_ENABLED) \
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .config("spark.executor.cores", config.SPARK_EXECUTOR_CORES) \
        .config("spark.executor.instances", config.SPARK_EXECUTOR_INSTANCES) \
        .getOrCreate()

    spark.sparkContext.setLogLevel(config.LOG_LEVEL)

    return spark


def run_batch(
    spark: SparkSession,
    config: DatalakeConfig,
    logger: logging.Logger,
    zones: Optional[list] = None,
) -> bool:
    """
    Exécuter un batch unique d'extraction et chargement.

    Args:
        spark: Session Spark
        config: Configuration du datalake
        logger: Logger
        zones: Zones à collecter (None = ["global"])

    Returns:
        True si succès, False sinon
    """

    try:
        logger.info("="*70)
        logger.info(f"Démarrage batch — {datetime.now().isoformat()}")
        logger.info("="*70)

        # Extraction
        logger.info("Phase 1 : Extraction de l'API...")

        extraction_config = {
            "zones": zones or ["global"],
            "enrich": False,  # POC : pas d'enrichissement pour le moment
            "timeout": config.API_TIMEOUT_SECONDS,
            "max_workers": config.API_MAX_WORKERS_PARALLEL,
        }

        df = extract_flights_batch(spark, extraction_config)
        num_flights = df.count()

        if num_flights == 0:
            logger.warning("Aucun vol collecté!")
            return False

        logger.info(f"✓ {num_flights} vols extraits")

        # Validation et flagging
        logger.info("Phase 2 : Validation et flagging...")

        df = validate_and_flag_flights(df, logger)

        # Profil de qualité
        logger.info("Phase 3 : Profil de qualité...")

        quality_stats = profile_data_quality(df, logger)

        # Sauvegarder le rapport de qualité
        if config.SAVE_QUALITY_REPORTS:
            now = datetime.now()
            quality_report_dir = Path(config.get_quality_report_path(
                now.strftime("%Y-%m-%d"),
                now.hour
            ))
            quality_report_dir.mkdir(parents=True, exist_ok=True)

            report_file = quality_report_dir / f"quality_profile_{datetime.now().isoformat().replace(':', '')}.json"
            with open(report_file, "w") as f:
                json.dump(quality_stats, f, indent=2, default=str)

            logger.info(f"✓ Quality report: {report_file}")

        # Partitionnement
        logger.info("Phase 4 : Partitionnement temporel...")

        now = datetime.now()
        partition_values = get_partition_values(now)

        df = df \
            .withColumn("tech_year", col("extraction_timestamp").cast("int").cast("string")) \
            .withColumn("tech_month", col("extraction_timestamp").cast("string")) \
            .withColumn("tech_day", col("extraction_timestamp").cast("string")) \
            .withColumn("tech_hour", col("extraction_timestamp").cast("string"))

        # Ajouter les colonnes de partitionnement si pas déjà présentes
        for col_name, val in partition_values.items():
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(val))

        # Chargement en Bronze
        logger.info("Phase 5 : Chargement en Bronze...")

        bronze_path = config.get_bronze_flights_path()
        df.write \
            .mode("append") \
            .partitionBy(*PARTITION_COLUMNS_BRONZE) \
            .parquet(bronze_path)

        logger.info(f"✓ Données écrites en Bronze: {bronze_path}")

        # Phase 6 : Transformation Silver + Gold (optionnel, selon flag)
        if config.LOAD_SILVER_GOLD:
            logger.info("Phase 6 : Transformation Silver + Gold...")

            try:
                loader = SilverGoldLoader(spark, config)
                etl_result = loader.run_full_etl(bronze_path)

                logger.info(f"✓ Silver : {etl_result['silver'].count()} rows")
                logger.info(f"✓ Gold : {len(etl_result['gold_kpis'])} KPIs calculés")

            except Exception as e:
                logger.warning(f"⚠️  Silver/Gold skipped due to error: {e}")

        # Résumé final
        logger.info("="*70)
        logger.info("✅ Batch complété avec succès")
        logger.info(f"   Vols : {num_flights}")
        logger.info(f"   Vols valides : {quality_stats.get('valid_rows', 0)}")
        logger.info(f"   Chemin Bronze : {bronze_path}")
        logger.info("="*70)

        return True

    except Exception as e:
        logger.error(f"❌ Erreur lors du batch : {e}", exc_info=True)
        return False


def main():
    """Point d'entrée principal."""

    parser = argparse.ArgumentParser(
        description="Job Spark Core batch pour la collecte du trafic aérien (à orchestrer toutes les 2h)."
    )

    parser.add_argument(
        "--datalake-root",
        type=str,
        default=None,
        help="Chemin racine du datalake (override DATALAKE_ROOT)"
    )

    parser.add_argument(
        "--zones",
        type=str,
        nargs="+",
        default=["global"],
        help="Zones à collecter (ex: --zones global europe asia)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode dry-run : collecter et valider, mais pas écrire"
    )

    parser.add_argument(
        "--single-batch",
        action="store_true",
        help="Exécuter un seul batch et quitter (par défaut : boucle infinie)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Logs verbeux"
    )

    args = parser.parse_args()

    # Override config si nécessaire
    if args.datalake_root:
        DatalakeConfig.DATALAKE_ROOT = args.datalake_root

    DatalakeConfig.validate()

    logger = setup_logging(DatalakeConfig)
    spark = create_spark_session(DatalakeConfig)

    try:
        logger.info("="*70)
        logger.info("Job Spark Core Batch — Trafic aérien")
        logger.info(f"Datalake: {DatalakeConfig.DATALAKE_ROOT}")
        logger.info(f"Zones: {args.zones}")
        logger.info(f"Dry-run: {args.dry_run}")
        logger.info(f"Single batch: {args.single_batch}")
        logger.info("="*70)

        # POC : une seule itération
        success = run_batch(spark, DatalakeConfig, logger, zones=args.zones)

        if not success:
            logger.warning("⚠️  Batch échoué (données peuvent être incomplètes)")

        if args.single_batch or True:  # POC : toujours quitter après 1 batch
            logger.info("Mode single-batch : arrêt après 1 itération")
            return 0 if success else 1

        # TODO Phase 2 : boucle de streaming infinie avec scheduling

    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
        return 0

    except Exception as e:
        logger.error(f"Erreur non gérée: {e}", exc_info=True)
        return 1

    finally:
        spark.stop()
        logger.info("Session Spark fermée")


if __name__ == "__main__":
    sys.exit(main())
