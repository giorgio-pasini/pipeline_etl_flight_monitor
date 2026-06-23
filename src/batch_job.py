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
import os
import sys
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Import local
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig, PARTITION_COLUMNS_BRONZE
from src.flight_extraction import extract_flights_batch
from src.data_quality import validate_and_flag_flights, profile_data_quality
from src.datalake_utils import get_partition_values
from src.silver_gold_loader import SilverGoldLoader
from src.job_metrics import JobMetrics
from src.alerting import check_and_alert
import json


def setup_logging(config: DatalakeConfig):
    """Configurer le logging (fichier + console), robuste UTF-8 sous Windows."""
    log_path = Path(config.get_log_path())
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f"batch_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # La console Windows est souvent en cp1252 et plante sur les emojis (✓, ⚠️).
    # On force la sortie standard en UTF-8 (errors=replace en filet de sécurité).
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")

    return logger


def create_spark_session(config: DatalakeConfig) -> SparkSession:
    """Créer et configurer la session Spark."""

    # Forcer l'interpréteur Python des workers Spark = celui qui lance le job.
    # Sans cela, sous Windows, le `python` par défaut peut être un stub
    # (alias Microsoft Store) qui ne se connecte jamais → erreur Spark
    # « Python worker failed to connect back » (SocketTimeoutException).
    # `setdefault` : on respecte une valeur déjà posée par l'utilisateur.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS) \
        .config("spark.sql.adaptive.enabled", config.SPARK_ADAPTIVE_EXECUTION_ENABLED) \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .config("spark.executor.cores", config.SPARK_EXECUTOR_CORES) \
        .config("spark.executor.instances", config.SPARK_EXECUTOR_INSTANCES)

    # Windows : exposer hadoop.dll (NativeIO) à la JVM via java.library.path.
    # Sans cela, l'écriture Parquet échoue avec UnsatisfiedLinkError sur
    # NativeIO$Windows.access0 même si winutils.exe est présent.
    hadoop_home = os.environ.get("HADOOP_HOME")
    if os.name == "nt" and hadoop_home:
        native_bin = os.path.join(hadoop_home, "bin")
        builder = builder.config("spark.driver.extraLibraryPath", native_bin)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)

    return spark


def run_batch(
    spark: SparkSession,
    config: DatalakeConfig,
    logger: logging.Logger,
    zones: Optional[list] = None,
    with_silver_gold: Optional[bool] = None,
) -> bool:
    """
    Exécuter un batch unique d'extraction et chargement.

    Pipeline complet (point d'entrée unique du projet) :
    1. Extraction API
    2. Validation + flagging qualité
    3. Profil de qualité + analyse (en vol / au sol, dimensions)
    4. Partitionnement temporel
    5. Écriture Bronze
    6. (optionnel) Transformation Silver + Gold
    7. Métriques JSON (consommées par le dashboard Streamlit)

    Args:
        spark: Session Spark
        config: Configuration du datalake
        logger: Logger
        zones: Zones à collecter (None = ["global"])
        with_silver_gold: Forcer Silver/Gold (None = config.LOAD_SILVER_GOLD)

    Returns:
        True si succès, False sinon
    """

    if with_silver_gold is None:
        with_silver_gold = config.LOAD_SILVER_GOLD

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics = JobMetrics(batch_id)

    try:
        logger.info("="*70)
        logger.info(f"Démarrage batch {batch_id} — {datetime.now().isoformat()}")
        logger.info("="*70)

        # Phase 1 : Extraction
        logger.info("Phase 1 : Extraction de l'API...")
        start_phase = time.time()

        extraction_config = {
            "zones": zones or config.COLLECTION_ZONES,
            "enrich": config.API_ENRICH_DETAILS,  # off : l'enrichissement vient des dims bulk
            "timeout": config.API_TIMEOUT_SECONDS,
            "max_workers": config.API_MAX_WORKERS_PARALLEL,
            "max_retries": config.API_MAX_RETRIES,
            "email": config.FR24_EMAIL,
            "password": config.FR24_PASSWORD,
            "retry_max_attempts": config.API_RETRY_MAX_ATTEMPTS,
            "retry_base_delay": config.API_RETRY_BASE_DELAY,
            "retry_max_delay": config.API_RETRY_MAX_DELAY,
        }

        df = extract_flights_batch(spark, extraction_config)
        num_flights = df.count()
        metrics.set_extraction(num_flights, time.time() - start_phase)

        if num_flights == 0:
            logger.warning("Aucun vol collecté!")
            metrics.add_warning("empty_extraction", "Aucun vol retourné par l'API")
            _finalize_and_alert(metrics, config, logger)
            return False

        logger.info(f"✓ {num_flights} vols extraits")

        # Phase 2 : Validation et flagging
        logger.info("Phase 2 : Validation et flagging...")

        df = validate_and_flag_flights(df, logger)
        # Cache : le DataFrame provient d'une liste locale et subit de nombreuses
        # actions (counts validation/profil/dimensions). Sans cache, chaque action
        # recalcule toute la conversion -> très lent. On matérialise une fois.
        df = df.cache()

        # Phase 3 : Profil de qualité
        logger.info("Phase 3 : Profil de qualité...")

        quality_stats = profile_data_quality(df, logger)

        valid_rows = quality_stats.get("valid_rows", 0)
        on_ground = quality_stats.get("on_ground", 0)
        on_flight = quality_stats.get("on_flight", 0)
        metrics.set_validation(valid_rows, num_flights - valid_rows)
        metrics.set_analysis(on_ground, on_flight)

        pct_valid = (valid_rows / num_flights * 100) if num_flights > 0 else 0
        if pct_valid < config.ALERT_THRESHOLD_PCT_VALID:
            metrics.add_warning(
                "low_quality",
                f"Qualité {pct_valid:.1f}% < seuil {config.ALERT_THRESHOLD_PCT_VALID}%"
            )

        # Dimensions (cardinalités) — métriques sur les autres tables du modèle
        unique_airlines = df.filter(col("airline_icao").isNotNull()).select("airline_icao").distinct().count()
        unique_airports = (
            df.select(col("origin_iata").alias("iata"))
            .union(df.select(col("destination_iata").alias("iata")))
            .filter(col("iata").isNotNull())
            .distinct().count()
        )
        unique_aircraft = df.filter(col("aircraft_code").isNotNull()).select("aircraft_code").distinct().count()
        metrics.set_dimension("dim_airlines", unique_airlines)
        metrics.set_dimension("dim_airports", unique_airports)
        metrics.set_dimension("dim_aircraft_models", unique_aircraft)

        # Sauvegarder le rapport de qualité
        if config.SAVE_QUALITY_REPORTS:
            now = datetime.now()
            quality_report_dir = Path(config.get_quality_report_path(
                now.strftime("%Y-%m-%d"),
                now.hour
            ))
            quality_report_dir.mkdir(parents=True, exist_ok=True)

            report_file = quality_report_dir / f"quality_profile_{now.isoformat().replace(':', '')}.json"
            with open(report_file, "w") as f:
                json.dump(quality_stats, f, indent=2, default=str)

            logger.info(f"✓ Quality report: {report_file}")

        # Phase 4 : Partitionnement temporel (valeurs correctes via get_partition_values)
        logger.info("Phase 4 : Partitionnement temporel...")

        partition_values = get_partition_values(datetime.now())
        for col_name, val in partition_values.items():
            df = df.withColumn(col_name, lit(val))

        # Phase 5 : Chargement en Bronze
        logger.info("Phase 5 : Chargement en Bronze...")

        bronze_path = config.get_bronze_flights_path()
        df.write \
            .mode("append") \
            .partitionBy(*PARTITION_COLUMNS_BRONZE) \
            .parquet(bronze_path)

        logger.info(f"✓ Données écrites en Bronze: {bronze_path}")

        # Phase 6 : Transformation Silver + Gold (optionnel)
        if with_silver_gold:
            logger.info("Phase 6 : Transformation Silver + Gold...")
            start_phase = time.time()

            try:
                # Dimensions de référence (bulk + cache) — enrichissement par jointure
                logger.info("  Chargement des dimensions de référence (cache 7j)...")
                from src.dimension_loader import load_all_dimensions
                dims = load_all_dimensions(spark, config)

                loader = SilverGoldLoader(spark, config)
                etl_result = loader.run_full_etl(
                    bronze_path,
                    dim_airports=dims.get("dim_airports"),
                    dim_airlines=dims.get("dim_airlines"),
                )

                metrics.set_gold(len(etl_result['gold_kpis']), time.time() - start_phase)
                for kpi_name, kpi_df in etl_result['gold_kpis'].items():
                    metrics.set_kpi_result(kpi_name, kpi_df.count())

                logger.info(f"✓ Silver : {etl_result['silver'].count()} rows")
                logger.info(f"✓ Gold : {len(etl_result['gold_kpis'])} KPIs calculés")

            except Exception as e:
                logger.warning(f"⚠️  Silver/Gold skipped due to error: {e}")
                metrics.add_warning("silver_gold_error", str(e))

        # Phase 7 : Métriques + alerting
        _finalize_and_alert(metrics, config, logger)

        # Résumé final
        logger.info("="*70)
        logger.info("✅ Batch complété avec succès")
        logger.info(f"   Vols : {num_flights}")
        logger.info(f"   Vols valides : {valid_rows} ({pct_valid:.1f}%)")
        logger.info(f"   Chemin Bronze : {bronze_path}")
        logger.info("="*70)

        return True

    except Exception as e:
        logger.error(f"❌ Erreur lors du batch : {e}", exc_info=True)
        metrics.add_error("batch_error", str(e), phase="pipeline")
        _finalize_and_alert(metrics, config, logger)
        return False


def _finalize_and_alert(metrics: JobMetrics, config: DatalakeConfig, logger: logging.Logger):
    """Finaliser les métriques, les sauvegarder, puis évaluer/déclencher les alertes."""
    metrics.finalize()
    _save_metrics(metrics, config, logger)
    try:
        check_and_alert(metrics.metrics, config, logger_obj=logger)
    except Exception as e:
        logger.warning(f"⚠️  Alerting échoué : {e}")


def _save_metrics(metrics: JobMetrics, config: DatalakeConfig, logger: logging.Logger):
    """Sauvegarder les métriques JSON dans LOG_PATH (lu par le dashboard)."""
    try:
        metrics_dir = Path(config.LOG_PATH)
        metrics_dir.mkdir(parents=True, exist_ok=True)
        path = metrics.save_to_json(str(metrics_dir / f"{metrics.batch_id}_metrics.json"))
        logger.info(f"✓ Métriques sauvegardées : {path}")
    except Exception as e:
        logger.warning(f"⚠️  Impossible de sauvegarder les métriques : {e}")


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
        default=None,
        help="Zones à collecter (défaut: COLLECTION_ZONES de la config). Ex: --zones global europe asia"
    )

    parser.add_argument(
        "--with-silver-gold",
        action="store_true",
        help="Lancer aussi la transformation Silver + Gold (KPIs)"
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
        logger.info(f"Silver/Gold: {args.with_silver_gold}")
        logger.info("="*70)

        # Le job exécute UN batch puis se termine.
        # La récurrence (toutes les 2h) est gérée par un scheduler externe
        # (cron / Task Scheduler — voir documentation/DOCUMENTATION.md § 6), pas par une boucle interne.
        success = run_batch(
            spark, DatalakeConfig, logger,
            zones=args.zones,
            with_silver_gold=args.with_silver_gold,
        )

        if not success:
            logger.warning("⚠️  Batch échoué (données peuvent être incomplètes)")

        return 0 if success else 1

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
