"""
Configurations Spark optimisées basées sur les patterns du pipeline.

Différents profils pour différents cas d'usage :
- POC : petit volume, local
- Production : grand volume, cluster
- Analytics : optimisé pour requêtes KPI
"""

import os


class SparkTuningProfiles:
    """Profils de tuning Spark pour différents cas d'usage."""

    # ============================================================================
    # POC — Local, petit volume
    # ============================================================================
    POC = {
        "spark.sql.shuffle.partitions": "4",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "1",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "notes": "Small local setup, auto-coalesce partitions",
    }

    # ============================================================================
    # Production — Cluster, gros volume
    # ============================================================================
    PRODUCTION = {
        "spark.sql.shuffle.partitions": "200",
        "spark.executor.memory": "8g",
        "spark.driver.memory": "4g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "10",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.forcePartitionNum": "false",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "20",
        "notes": "Large cluster, adaptive execution, dynamic allocation",
    }

    # ============================================================================
    # Analytics — Optimisé pour KPI queries
    # ============================================================================
    ANALYTICS = {
        "spark.sql.shuffle.partitions": "100",
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "5",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.statistics.histogram.enabled": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.network.timeout": "600s",
        "notes": "Optimized for aggregation queries, histogram stats, partition pruning",
    }

    # ============================================================================
    # Batch — ETL batch processing (notre use case)
    # ============================================================================
    BATCH = {
        "spark.sql.shuffle.partitions": "150",
        "spark.executor.memory": "6g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.files.ignoreCorruptFiles": "true",  # Tolère fichiers corrompus
        "spark.task.maxFailures": "4",
        "spark.stage.maxConsecutiveAttempts": "4",
        "spark.sql.broadcastTimeout": "600",  # 10 min pour broadcast
        "notes": "Batch ETL processing, fault-tolerant, partition pruning",
    }

    @staticmethod
    def get_profile(profile_name: str = None) -> dict:
        """
        Retourner le profil de tuning approprié.

        Args:
            profile_name: "poc", "production", "analytics", "batch", ou None (auto-detect)

        Returns:
            Dict avec configurations Spark
        """
        if profile_name is None:
            # Auto-detect basé sur env var
            profile_name = os.getenv("SPARK_PROFILE", "batch").lower()

        profiles = {
            "poc": SparkTuningProfiles.POC,
            "production": SparkTuningProfiles.PRODUCTION,
            "analytics": SparkTuningProfiles.ANALYTICS,
            "batch": SparkTuningProfiles.BATCH,
        }

        return profiles.get(profile_name, SparkTuningProfiles.BATCH)

    @staticmethod
    def describe_profile(profile_name: str = "batch") -> str:
        """Décrire un profil de tuning."""
        profile = SparkTuningProfiles.get_profile(profile_name)
        notes = profile.pop("notes", "")

        description = f"\nSpark Tuning Profile: {profile_name.upper()}\n"
        description += "=" * 60 + "\n"
        description += notes + "\n\n"
        description += "Configuration:\n"

        for key, value in sorted(profile.items()):
            description += f"  {key}: {value}\n"

        return description


# ============================================================================
# Recommandations de tuning basées sur les patterns d'accès
# ============================================================================

PARTITIONING_RECOMMENDATIONS = {
    "bronze_flights_raw": {
        "primary": ["tech_year", "tech_month", "tech_day", "tech_hour"],
        "secondary": ["on_ground"],  # Optionnel pour filtres fréquents
        "rationale": "Temporal partitioning per spec, on_ground for KPI queries",
    },
    "silver_fact_flights": {
        "primary": ["tech_year", "tech_month"],
        "secondary": ["origin_continent", "is_valid"],
        "rationale": "Coarser temporal, continent for regional KPIs, validity for filtering",
    },
    "gold_kpi_tables": {
        "primary": ["tech_year", "tech_month"],
        "secondary": None,
        "rationale": "Lightweight partitioning for aggregated data",
    },
}

INDEX_RECOMMENDATIONS = {
    "bronze_flights_raw": [
        "flight_id",  # PK, fréquemment recherché
        "airline_icao",  # Top KPI filter
        "on_ground",  # Très fréquent (KPI 1, 3, etc.)
    ],
    "silver_fact_flights": [
        "flight_id",
        "airline_icao",
        "origin_continent",
    ],
}

BROADCAST_RECOMMENDATIONS = {
    "dim_airlines": "YES",  # ~2000 rows, petit
    "dim_airports": "YES",  # ~50k rows, peut être broadcasté
    "dim_aircraft_models": "YES",  # ~3000 rows
    "dim_countries_continents": "YES",  # ~200 rows, très petit
}
