"""Tests end-to-end pour le pipeline complet."""

import pytest
import logging
from pathlib import Path
from src.batch_job import run_batch, create_spark_session
from config.pipeline_config import PipelineConfig


pytestmark = pytest.mark.e2e


class TestE2EBatchJobWithRealAPI:
    """Tests E2E du batch job avec l'API réelle."""

    @pytest.mark.slow
    def test_batch_job_full_cycle(self, temp_datalake):
        """Smoke API réel : extraction → validation → Bronze. Vérifie le contrat
        de l'API FlightRadar24 (un changement côté API ferait échouer ce test)."""
        spark = create_spark_session(PipelineConfig)
        logger = logging.getLogger(__name__)

        try:
            success = run_batch(spark, PipelineConfig, logger, zones=["global"])

            # L'API réelle doit renvoyer des vols -> batch réussi + Bronze écrit + ≥ 1 vol
            assert success is True
            bronze_path = PipelineConfig.get_bronze_flights_path()
            assert Path(bronze_path).exists()
            assert spark.read.parquet(bronze_path).count() >= 1

        finally:
            spark.stop()
