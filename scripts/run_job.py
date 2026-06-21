"""
Job ETL final — Exécution complète du pipeline (Étape 7).

Exécute toutes les phases :
1. Extraction (API)
2. Validation + Data Quality
3. Load Bronze
4. Transform Silver + Gold (optionnel)
5. Enregistrer métriques
6. Sauvegarder logs

Usage:
    python scripts/run_job.py
    python scripts/run_job.py --with-silver-gold
    python scripts/run_job.py --zones europe northamerica
"""

import sys
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path

# Import local modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from config.datalake_config import DatalakeConfig, PARTITION_COLUMNS_BRONZE
from src.flight_extraction import extract_flights_batch
from src.data_quality import validate_and_flag_flights, profile_data_quality
from src.job_metrics import JobMetrics
from src.silver_gold_loader import SilverGoldLoader


def setup_logging(batch_id: str) -> logging.Logger:
    """Configurer le logging."""
    log_file = f"{DatalakeConfig.LOG_PATH}/pipeline.log"
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s : %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("job")


def run_job(spark: SparkSession, zones: list = None, with_silver_gold: bool = False) -> bool:
    """
    Exécuter le job ETL complet.

    Args:
        spark: Session Spark
        zones: Zones à collecter (default: ["global"])
        with_silver_gold: Charger Silver + Gold ?

    Returns:
        True si succès, False sinon
    """
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = logging.getLogger("job")
    metrics = JobMetrics(batch_id)

    if zones is None:
        zones = DatalakeConfig.COLLECTION_ZONES

    logger.info("=" * 70)
    logger.info(f"Starting ETL Job: {batch_id}")
    logger.info(f"Zones: {zones}")
    logger.info(f"Load Silver/Gold: {with_silver_gold}")
    logger.info("=" * 70)

    try:
        # ====================================================================
        # Phase 1 : Extraction
        # ====================================================================
        logger.info("\n[Phase 1] Extraction...")
        start_phase = time.time()

        flights_df, num_flights = extract_flights_batch(spark, zones)
        extraction_duration = time.time() - start_phase

        metrics.set_extraction(num_flights, extraction_duration)
        logger.info(f"✓ Extracted {num_flights} flights in {extraction_duration:.2f}s")

        if num_flights == 0:
            logger.warning("⚠️ No flights extracted")
            metrics.add_warning("empty_extraction", "No flights from API")

        # ====================================================================
        # Phase 2 : Validation + Data Quality
        # ====================================================================
        logger.info("\n[Phase 2] Validation & Data Quality...")
        start_phase = time.time()

        df_validated = validate_and_flag_flights(flights_df, logger=logger)

        valid_rows = df_validated.filter(col("is_valid") == True).count()
        invalid_rows = num_flights - valid_rows
        pct_valid = (valid_rows / num_flights * 100) if num_flights > 0 else 0

        metrics.set_validation(valid_rows, invalid_rows)
        validation_duration = time.time() - start_phase

        logger.info(f"✓ Validation completed in {validation_duration:.2f}s")
        logger.info(f"  • Valid rows: {valid_rows} ({pct_valid:.1f}%)")
        logger.info(f"  • Invalid rows: {invalid_rows}")

        # Data quality profile
        quality_stats = profile_data_quality(df_validated)
        logger.info(f"  • On ground: {quality_stats.get('on_ground_rows', 0)}")
        logger.info(f"  • In flight: {num_flights - quality_stats.get('on_ground_rows', 0)}")

        # SLA check: % valid >= 70%
        if pct_valid < 70:
            metrics.add_warning("low_quality", f"Data quality {pct_valid:.1f}% < 70% threshold")
            logger.warning(f"⚠️  Data quality below 70% threshold: {pct_valid:.1f}%")

        # Analysis metrics
        on_ground = quality_stats.get("on_ground_rows", 0)
        in_flight = valid_rows - on_ground
        metrics.set_analysis(on_ground, in_flight)

        # Dimensions
        if valid_rows > 0:
            unique_airlines = df_validated.select("airline_icao").distinct().count()
            unique_airports = df_validated.select("origin_iata").union(
                df_validated.select("destination_iata")
            ).distinct().count()
            unique_aircraft = df_validated.select("aircraft_code").distinct().count()

            metrics.set_dimension("dim_airlines", unique_airlines)
            metrics.set_dimension("dim_airports", unique_airports)
            metrics.set_dimension("dim_aircraft_models", unique_aircraft)
            metrics.set_dimension("dim_countries_continents", 195)

            logger.info(f"  • Airlines: {unique_airlines} unique")
            logger.info(f"  • Airports: {unique_airports} unique")
            logger.info(f"  • Aircraft models: {unique_aircraft} unique")

        # ====================================================================
        # Phase 3 : Load Bronze
        # ====================================================================
        logger.info("\n[Phase 3] Load Bronze...")
        start_phase = time.time()

        bronze_path = DatalakeConfig.get_bronze_flights_path()

        df_validated.write \
            .mode("append") \
            .partitionBy(*PARTITION_COLUMNS_BRONZE) \
            .parquet(bronze_path)

        bronze_duration = time.time() - start_phase
        logger.info(f"✓ Loaded to Bronze in {bronze_duration:.2f}s")
        logger.info(f"  • Path: {bronze_path}")
        logger.info(f"  • Rows written: {num_flights}")

        # ====================================================================
        # Phase 4 : Silver + Gold (optionnel)
        # ====================================================================
        if with_silver_gold:
            logger.info("\n[Phase 4] Silver + Gold Transformation...")
            start_phase = time.time()

            try:
                loader = SilverGoldLoader(spark, DatalakeConfig)
                etl_result = loader.run_full_etl(bronze_path)

                silver_rows = etl_result['silver'].count()
                gold_duration = time.time() - start_phase

                metrics.set_gold(7, gold_duration)

                # KPI results
                for kpi_name, kpi_df in etl_result['gold_kpis'].items():
                    kpi_rows = kpi_df.count()
                    kpi_clean_name = kpi_name.replace("_", "")
                    metrics.set_kpi_result(kpi_clean_name, kpi_rows)
                    logger.info(f"  • {kpi_name}: {kpi_rows} rows")

                logger.info(f"✓ Silver + Gold completed in {gold_duration:.2f}s")

            except Exception as e:
                logger.warning(f"⚠️  Silver/Gold skipped: {str(e)}")
                metrics.add_warning("silver_gold_error", str(e))

        # ====================================================================
        # Phase 5 : Finaliser métriques et logs
        # ====================================================================
        logger.info("\n[Phase 5] Finalizing metrics...")

        metrics.finalize()
        metrics_path = metrics.save_to_json()

        logger.info(f"✓ Metrics saved: {metrics_path}")

        # Afficher résumé
        logger.info("\n" + metrics.get_summary())

        # ====================================================================
        # Résumé final
        # ====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("✅ ETL Job completed successfully")
        logger.info(f"   Batch ID: {batch_id}")
        logger.info(f"   Duration: {metrics.metrics['total_duration_seconds']:.1f}s")
        logger.info(f"   Status: {metrics.metrics['status']}")
        logger.info(f"   Errors: {metrics.metrics['num_errors']}")
        logger.info(f"   Warnings: {metrics.metrics['num_warnings']}")
        logger.info("=" * 70 + "\n")

        return True

    except Exception as e:
        logger.error(f"❌ Error in job: {str(e)}", exc_info=True)
        metrics.add_error("job_error", str(e), phase="pipeline")
        metrics.finalize()
        metrics.save_to_json()
        raise


def main():
    """Point d'entrée."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline - Final Job (Étape 7)"
    )

    parser.add_argument(
        "--zones",
        type=str,
        nargs="+",
        default=["global"],
        help="Zones to collect (default: global)"
    )

    parser.add_argument(
        "--with-silver-gold",
        action="store_true",
        help="Load Silver + Gold transformations"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = setup_logging(batch_id)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"etl-pipeline-{batch_id}") \
        .master(DatalakeConfig.SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", DatalakeConfig.SPARK_SHUFFLE_PARTITIONS) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        success = run_job(
            spark,
            zones=args.zones,
            with_silver_gold=args.with_silver_gold
        )
        return 0 if success else 1

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
