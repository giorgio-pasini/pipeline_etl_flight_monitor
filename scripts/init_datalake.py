"""
Script d'initialisation du datalake.

Crée :
- Répertoires bronze/, silver/, gold/
- Structure de partitions pour les KPIs Gold (exemple pour la première exécution)
- Fichiers _SUCCESS pour marquer les tables comme valides

À exécuter une fois avant le première exécution du pipeline.

Usage:
    python scripts/init_datalake.py --datalake-root /path/to/datalake [--verbose]
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# Ajouter src au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig
from src.schemas import SCHEMAS


def setup_logging(verbose=False):
    """Configurer le logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s"
    )
    return logging.getLogger(__name__)


def create_directory_structure(config: DatalakeConfig, logger: logging.Logger):
    """Créer l'arborescence du datalake."""
    layers = {
        "Bronze (données brutes)": config.BRONZE_PATH,
        "Silver (données nettoyées)": config.SILVER_PATH,
        "Gold (KPIs)": config.GOLD_PATH,
    }

    for name, path in layers.items():
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ {name}: {path}")
        except Exception as e:
            logger.error(f"✗ {name}: {e}")
            return False

    return True


def create_bronze_tables(config: DatalakeConfig, logger: logging.Logger):
    """Créer les tables Bronze."""
    logger.info("\n=== Bronze Layer ===")

    # flights_raw
    flights_raw_path = Path(config.get_bronze_flights_path())
    flights_raw_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Table flights_raw: {flights_raw_path}")

    # Checkpoints Spark
    checkpoint_path = Path(config.get_bronze_checkpoints_path())
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Checkpoints path: {checkpoint_path}")

    # Créer un dossier exemple de partition
    example_date = datetime.now()
    example_partition = (
        flights_raw_path /
        f"tech_year={example_date.year}" /
        f"tech_month={example_date.strftime('%Y-%m')}" /
        f"tech_day={example_date.strftime('%Y-%m-%d')}" /
        f"tech_hour={example_date.strftime('%H')}"
    )
    example_partition.mkdir(parents=True, exist_ok=True)
    (example_partition / "_SUCCESS").touch()
    logger.info(f"✓ Example partition created: {example_partition}")


def create_silver_tables(config: DatalakeConfig, logger: logging.Logger):
    """Créer les tables Silver."""
    logger.info("\n=== Silver Layer ===")

    # Fact table
    fact_flights_path = Path(config.get_silver_fact_flights_path())
    fact_flights_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Table fact_flights: {fact_flights_path}")

    # Dimension tables
    dim_tables = [
        "dim_airlines",
        "dim_airports",
        "dim_aircraft_models",
        "dim_countries_continents",
    ]

    for dim_name in dim_tables:
        dim_path = Path(config.get_silver_dim_path(dim_name))
        dim_path.mkdir(parents=True, exist_ok=True)
        (dim_path / "_SUCCESS").touch()
        logger.info(f"✓ Table {dim_name}: {dim_path}")

    # Quality logs directory (optionnel)
    quality_path = Path(config.SILVER_PATH) / "_quality_logs"
    quality_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Quality logs path: {quality_path}")


def create_gold_tables(config: DatalakeConfig, logger: logging.Logger):
    """Créer les tables Gold (KPIs)."""
    logger.info("\n=== Gold Layer (KPIs) ===")

    kpi_tables = [
        "kpi_airline_volumes",
        "kpi_continental_regional",
        "kpi_longest_flights",
        "kpi_continental_avg_distance",
        "kpi_aircraft_manufacturers",
        "kpi_airline_aircraft_models",
        "kpi_airport_imbalance",
    ]

    for kpi_name in kpi_tables:
        kpi_path = Path(config.get_gold_kpi_path(kpi_name))
        kpi_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Table {kpi_name}: {kpi_path}")

    # Metadata directory
    metadata_path = Path(config.GOLD_PATH) / "_metadata"
    metadata_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Metadata path: {metadata_path}")


def create_logging_directory(config: DatalakeConfig, logger: logging.Logger):
    """Créer le répertoire de logging."""
    logger.info("\n=== Logging ===")

    log_path = Path(config.get_log_path())
    log_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Log path: {log_path}")


def write_schema_documentation(config: DatalakeConfig, logger: logging.Logger):
    """Écrire la documentation des schémas."""
    logger.info("\n=== Schema Documentation ===")

    schemas_doc_path = Path(config.DATALAKE_ROOT) / "_schemas"
    schemas_doc_path.mkdir(parents=True, exist_ok=True)

    for table_name, schema in SCHEMAS.items():
        doc_content = f"# Schema: {table_name}\n\n"
        doc_content += "## Fields\n\n"
        for field in schema.fields:
            nullable = "?" if field.nullable else "!"
            doc_content += f"- `{field.name}` : {field.dataType.typeName()}{nullable}\n"

        doc_path = schemas_doc_path / f"{table_name}.md"
        doc_path.write_text(doc_content)

    logger.info(f"✓ Schema documentation written to {schemas_doc_path}")


def create_example_config_file(config: DatalakeConfig, logger: logging.Logger):
    """Créer un fichier .env exemple."""
    logger.info("\n=== Configuration ===")

    env_example = f"""# Configuration du datalake (variables d'environnement)

# Chemin racine
DATALAKE_ROOT={config.DATALAKE_ROOT}

# Rétention (jours)
BRONZE_RETENTION_DAYS={config.BRONZE_RETENTION_DAYS}
SILVER_RETENTION_DAYS={config.SILVER_RETENTION_DAYS}
GOLD_RETENTION_DAYS={config.GOLD_RETENTION_DAYS}

# Spark
SPARK_MASTER={config.SPARK_MASTER}
SPARK_SHUFFLE_PARTITIONS={config.SPARK_SHUFFLE_PARTITIONS}
SPARK_EXECUTOR_MEMORY={config.SPARK_EXECUTOR_MEMORY}
SPARK_EXECUTOR_CORES={config.SPARK_EXECUTOR_CORES}

# Logging
LOG_LEVEL={config.LOG_LEVEL}
"""

    env_path = Path(config.DATALAKE_ROOT) / ".env.example"
    env_path.write_text(env_example)
    logger.info(f"✓ Example .env file: {env_path}")


def write_initialization_log(config: DatalakeConfig, logger: logging.Logger):
    """Écrire un fichier de log d'initialisation."""
    logger.info("\n=== Initialization Log ===")

    init_log = f"""# Datalake Initialization Log

**Timestamp:** {datetime.now().isoformat()}
**Datalake Root:** {config.DATALAKE_ROOT}

## Structure Created

- Bronze: {config.BRONZE_PATH}
- Silver: {config.SILVER_PATH}
- Gold: {config.GOLD_PATH}
- Logs: {config.LOG_PATH}

## Configuration

- Batch interval: {config.BATCH_INTERVAL_HOURS} hours
- Bronze retention: {config.BRONZE_RETENTION_DAYS} days
- Silver retention: {config.SILVER_RETENTION_DAYS} days
- Gold retention: {config.GOLD_RETENTION_DAYS} days

## Next Steps

1. Configure environment variables (see .env.example)
2. Run the Spark streaming job: python src/batch_job.py
3. Monitor the logs in {config.LOG_PATH}
"""

    log_path = Path(config.DATALAKE_ROOT) / "_INITIALIZATION.md"
    log_path.write_text(init_log)
    logger.info(f"✓ Initialization log: {log_path}")


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="Initialiser la structure du datalake."
    )
    parser.add_argument(
        "--datalake-root",
        type=str,
        default=None,
        help="Chemin racine du datalake (override DATALAKE_ROOT env var)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Output verbeux (debug logs)"
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    # Override config si nécessaire
    if args.datalake_root:
        DatalakeConfig.DATALAKE_ROOT = args.datalake_root

    logger.info("=== Initialization du datalake ===\n")
    logger.info(f"Racine : {DatalakeConfig.DATALAKE_ROOT}\n")

    # Étapes d'initialisation
    steps = [
        ("Création arborescence", lambda: create_directory_structure(DatalakeConfig, logger)),
        ("Tables Bronze", lambda: create_bronze_tables(DatalakeConfig, logger)),
        ("Tables Silver", lambda: create_silver_tables(DatalakeConfig, logger)),
        ("Tables Gold", lambda: create_gold_tables(DatalakeConfig, logger)),
        ("Répertoire logging", lambda: create_logging_directory(DatalakeConfig, logger)),
        ("Documentation schémas", lambda: write_schema_documentation(DatalakeConfig, logger)),
        ("Fichier .env.example", lambda: create_example_config_file(DatalakeConfig, logger)),
        ("Log d'initialisation", lambda: write_initialization_log(DatalakeConfig, logger)),
    ]

    failed = False
    for step_name, step_fn in steps:
        try:
            if not step_fn():
                failed = True
                logger.error(f"✗ {step_name} échoué")
        except Exception as e:
            failed = True
            logger.error(f"✗ {step_name} : {e}", exc_info=args.verbose)

    # Résumé final
    logger.info("\n" + "="*60)
    if failed:
        logger.error("❌ Initialisation échouée (voir les erreurs ci-dessus)")
        return 1
    else:
        logger.info("✅ Datalake initialisé avec succès!")
        logger.info(f"\nPour démarrer le pipeline :")
        logger.info(f"  export DATALAKE_ROOT={DatalakeConfig.DATALAKE_ROOT}")
        logger.info(f"  python src/batch_job.py")
        return 0


if __name__ == "__main__":
    sys.exit(main())
