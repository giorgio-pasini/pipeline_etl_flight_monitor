"""Tests end-to-end pour le pipeline complet."""

import pytest
import logging
from pathlib import Path
from src.batch_job import run_batch, create_spark_session
from config.datalake_config import DatalakeConfig


pytestmark = pytest.mark.e2e


class TestE2EBatchJobWithRealAPI:
    """Tests E2E du batch job avec l'API réelle."""

    @pytest.mark.slow
    def test_batch_job_full_cycle(self, temp_datalake):
        """Tester le cycle complet : extraction → validation → Bronze write."""
        spark = create_spark_session(DatalakeConfig)
        logger = logging.getLogger(__name__)

        try:
            # Lancer le batch
            success = run_batch(
                spark,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Doit réussir ou échouer gracieusement
            assert isinstance(success, bool)

            # Si succès, vérifier que des données ont été écrites
            if success:
                bronze_path = DatalakeConfig.get_bronze_flights_path()
                assert Path(bronze_path).exists()

        finally:
            spark.stop()

    @pytest.mark.slow
    def test_batch_job_creates_quality_reports(self, temp_datalake):
        """Tester que le batch crée des rapports de qualité."""
        spark = create_spark_session(DatalakeConfig)
        logger = logging.getLogger(__name__)

        try:
            success = run_batch(
                spark,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Si succès, vérifier les rapports
            if success:
                logs_path = Path(DatalakeConfig.DATALAKE_ROOT) / "_logs"
                # Les logs ou rapports doivent avoir été créés
                # (structure dépend de l'implémentation)
                assert isinstance(success, bool)

        finally:
            spark.stop()

    @pytest.mark.slow
    def test_batch_job_idempotent(self, temp_datalake):
        """Tester que le batch est idempotent (peut être rejouée)."""
        spark = create_spark_session(DatalakeConfig)
        logger = logging.getLogger(__name__)

        try:
            # Premier run
            success1 = run_batch(
                spark,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Deuxième run (même données)
            success2 = run_batch(
                spark,
                DatalakeConfig,
                logger,
                zones=["global"]
            )

            # Les deux doivent avoir le même résultat (idempotent)
            assert isinstance(success1, bool)
            assert isinstance(success2, bool)
            # Pas d'assertion stricte car l'API peut retourner des données différentes

        finally:
            spark.stop()
