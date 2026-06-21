"""
Analyse et optimisation du partitionnement.

Objectif :
- Analyser les patterns d'accès (quels KPIs utilisent quelles colonnes)
- Profiler les performances par partition
- Recommander ajustements Spark config
- Mesurer impact (temps requête, taille stockage)
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class PartitioningOptimizer:
    """Analyse et optimise le partitionnement du datalake."""

    def __init__(self, spark: SparkSession, datalake_config):
        """
        Initialiser l'optimiseur.

        Args:
            spark: Session Spark
            datalake_config: Configuration (DatalakeConfig)
        """
        self.spark = spark
        self.config = datalake_config

    def analyze_partition_skew(self, df: DataFrame, partition_cols: list) -> dict:
        """
        Analyser la distribution des données par partition.

        Détecte le "skew" (déséquilibre) : certaines partitions beaucoup + de données.

        Args:
            df: DataFrame à analyser
            partition_cols: Colonnes de partitionnement (ex: ["tech_year", "tech_month", "tech_day"])

        Returns:
            Dict avec statistiques de distribution
        """
        logger.info(f"Analyzing partition skew for {partition_cols}...")

        stats = (
            df.groupBy(*partition_cols)
            .agg(
                count("*").alias("row_count"),
                spark_sum(1).alias("total_rows")  # Dummy pour Spark
            )
            .collect()
        )

        row_counts = [row["row_count"] for row in stats]
        total_rows = sum(row_counts) if row_counts else 0

        if not row_counts:
            return {
                "partition_cols": partition_cols,
                "partition_count": 0,
                "total_rows": 0,
                "skew_ratio": 0,
            }

        min_rows = min(row_counts)
        max_rows = max(row_counts)
        avg_rows = total_rows / len(row_counts) if row_counts else 0
        skew_ratio = max_rows / min_rows if min_rows > 0 else 0

        logger.info(f"  Min rows per partition: {min_rows}")
        logger.info(f"  Max rows per partition: {max_rows}")
        logger.info(f"  Avg rows per partition: {avg_rows:.0f}")
        logger.info(f"  Skew ratio (max/min): {skew_ratio:.2f}x")

        return {
            "partition_cols": partition_cols,
            "partition_count": len(row_counts),
            "total_rows": total_rows,
            "min_rows_per_partition": int(min_rows),
            "max_rows_per_partition": int(max_rows),
            "avg_rows_per_partition": float(avg_rows),
            "skew_ratio": float(skew_ratio),
            "recommendation": "Add secondary partitioning" if skew_ratio > 3 else "OK",
        }

    def estimate_partition_sizes(self, df: DataFrame, partition_cols: list) -> dict:
        """
        Estimer la taille des données par partition.

        Args:
            df: DataFrame
            partition_cols: Colonnes de partitionnement

        Returns:
            Dict avec tailles estimées
        """
        logger.info(f"Estimating partition sizes...")

        # Taille approximative en mémoire (8 bytes par double, 50 bytes par string moyen)
        total_rows = df.count()
        num_cols = len(df.columns)

        # Estimation simple : ~100 bytes/row
        estimated_size_mb = (total_rows * 100) / (1024 * 1024)

        partition_stats = (
            df.groupBy(*partition_cols)
            .agg(count("*").alias("row_count"))
            .collect()
        )

        partitions_info = []
        for row in partition_stats:
            partition_values = {col: row[col] for col in partition_cols}
            size_mb = (row["row_count"] * 100) / (1024 * 1024)
            partitions_info.append({
                "partition": partition_values,
                "rows": row["row_count"],
                "size_mb_estimated": float(size_mb),
            })

        return {
            "total_rows": total_rows,
            "total_size_mb_estimated": float(estimated_size_mb),
            "num_partitions": len(partitions_info),
            "avg_partition_size_mb": float(estimated_size_mb / len(partitions_info)) if partitions_info else 0,
            "partitions": partitions_info[:10],  # Top 10
        }

    def recommend_spark_config(self, df: DataFrame) -> dict:
        """
        Recommander la configuration Spark optimale basée sur les données.

        Args:
            df: DataFrame analysé

        Returns:
            Dict avec configurations recommandées
        """
        logger.info("Recommending Spark configuration...")

        total_rows = df.count()
        num_partitions = df.rdd.getNumPartitions()

        # Règles heuristiques
        recommended_shuffle_partitions = max(4, min(200, total_rows // 100000))
        recommended_partitions_per_executor = 3  # Standard

        recommendations = {
            "current_num_partitions": num_partitions,
            "recommended_shuffle_partitions": recommended_shuffle_partitions,
            "recommended_partitions_per_executor": recommended_partitions_per_executor,
            "notes": [
                f"Total rows: {total_rows:,}",
                f"Current partitions: {num_partitions}",
                f"Rows per partition: {total_rows // num_partitions:,}" if num_partitions > 0 else "N/A",
                f"Recommend coalesce to {recommended_shuffle_partitions} for better parallelism",
            ],
        }

        return recommendations

    def analyze_query_patterns(self) -> dict:
        """
        Analyser les patterns d'accès des requêtes (quelles colonnes, quels filtres).

        Returns:
            Dict avec patterns recommandés
        """
        logger.info("Analyzing KPI query patterns...")

        # Patterns d'accès identifiés (basés sur les KPIs)
        patterns = {
            "kpi_airline_volumes": {
                "columns": ["airline_icao", "airline_name", "on_ground"],
                "filters": ["on_ground=0", "is_valid=True"],
                "recommended_partitioning": ["tech_year", "tech_month"],
            },
            "kpi_continental_regional": {
                "columns": ["origin_continent", "destination_continent", "airline_icao"],
                "filters": ["origin_continent=destination_continent", "is_valid=True"],
                "recommended_partitioning": ["tech_year", "tech_month", "origin_continent"],
            },
            "kpi_longest_flight": {
                "columns": ["callsign", "airline_icao", "origin_iata", "destination_iata", "distance"],
                "filters": ["on_ground=0", "is_valid=True"],
                "recommended_partitioning": ["tech_year", "tech_month"],
            },
        }

        return {
            "analyzed_kpis": len(patterns),
            "common_columns": ["tech_year", "tech_month", "is_valid", "on_ground"],
            "common_filters": ["is_valid=True", "on_ground=0"],
            "patterns": patterns,
            "recommendation": "Add tech_month as secondary partition for faster KPI queries",
        }

    def profile_query_performance(self, df: DataFrame, query_name: str, query_func) -> dict:
        """
        Profiler la performance d'une requête donnée.

        Args:
            df: DataFrame source
            query_name: Nom de la requête (ex: "kpi_airline_volumes")
            query_func: Fonction qui exécute la requête

        Returns:
            Dict avec timing et résultats
        """
        logger.info(f"Profiling query: {query_name}...")

        start_time = datetime.now()
        result_df = query_func(df)
        num_results = result_df.count()
        end_time = datetime.now()

        elapsed_seconds = (end_time - start_time).total_seconds()

        logger.info(f"  Query time: {elapsed_seconds:.2f}s")
        logger.info(f"  Result rows: {num_results}")

        return {
            "query_name": query_name,
            "elapsed_seconds": float(elapsed_seconds),
            "result_rows": num_results,
            "performance": "GOOD" if elapsed_seconds < 5 else "SLOW",
        }

    def generate_optimization_report(self, datalake_path: str) -> dict:
        """
        Générer un rapport d'optimisation complet du datalake.

        Args:
            datalake_path: Chemin du datalake (Bronze/Silver/Gold)

        Returns:
            Dict avec tous les analyses
        """
        logger.info("=" * 70)
        logger.info("Generating partition optimization report...")
        logger.info("=" * 70)

        # Lire les données
        try:
            df = self.spark.read.parquet(datalake_path)
        except Exception as e:
            logger.error(f"Error reading {datalake_path}: {e}")
            return {"error": str(e)}

        report = {
            "timestamp": datetime.now().isoformat(),
            "datalake_path": datalake_path,
            "skew_analysis": self.analyze_partition_skew(df, ["tech_year", "tech_month", "tech_day"]),
            "size_analysis": self.estimate_partition_sizes(df, ["tech_year", "tech_month"]),
            "spark_recommendations": self.recommend_spark_config(df),
            "query_patterns": self.analyze_query_patterns(),
            "action_items": [
                "Monitor partition skew (current ratio: check above)",
                "Consider secondary partitioning if skew_ratio > 3",
                "Review query patterns and adjust partitioning",
                "Profile slow queries and optimize",
            ],
        }

        logger.info("=" * 70)
        logger.info("✓ Optimization report generated")
        logger.info("=" * 70)

        return report


def save_optimization_report(report: dict, output_path: str):
    """Sauvegarder le rapport en JSON."""
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"Report saved to {output_path}")
