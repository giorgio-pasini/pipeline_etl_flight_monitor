"""
Initialisation (légère) de l'arborescence du datalake.

Crée les répertoires bronze/ silver/ gold/ _logs/. **Optionnel** : le pipeline
(`run_job.py`) crée lui-même les dossiers nécessaires (via `DatalakeConfig.validate()`
et les écritures Spark). Ce script sert à pré-créer l'arborescence si on le souhaite.

Usage:
    python scripts/init_datalake.py [--datalake-root /path] [--verbose]
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.datalake_config import DatalakeConfig


def setup_logging(verbose=False):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    )
    return logging.getLogger(__name__)


def create_directory_structure(config: DatalakeConfig, logger: logging.Logger) -> bool:
    """Créer l'arborescence du datalake (bronze/silver/gold/_logs)."""
    layers = {
        "Bronze (données brutes)": config.BRONZE_PATH,
        "Silver (données nettoyées)": config.SILVER_PATH,
        "Gold (KPIs)": config.GOLD_PATH,
        "Logs (métriques/alertes)": config.LOG_PATH,
    }
    for name, path in layers.items():
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ {name}: {path}")
        except Exception as e:
            logger.error(f"✗ {name}: {e}")
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Initialiser l'arborescence du datalake")
    parser.add_argument("--datalake-root", type=str, default=None, help="Override DATALAKE_ROOT")
    parser.add_argument("--verbose", action="store_true", help="Logs verbeux")
    args = parser.parse_args()

    if args.datalake_root:
        DatalakeConfig.DATALAKE_ROOT = args.datalake_root

    logger = setup_logging(args.verbose)
    logger.info(f"Initialisation du datalake : {DatalakeConfig.DATALAKE_ROOT}")
    ok = create_directory_structure(DatalakeConfig, logger)
    logger.info("✓ Datalake initialisé." if ok else "✗ Échec de l'initialisation.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
