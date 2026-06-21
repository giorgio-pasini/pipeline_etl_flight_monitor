"""
Chargement des couches Silver et Gold.

Silver : Données nettoyées et enrichies (fact_flights + dimensions)
Gold : Tables KPI agrégées
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

from .transformations import (
    clean_and_enrich_bronze,
    kpi_airline_volumes,
    kpi_continental_regional,
    kpi_longest_flight,
    kpi_continental_avg_distance,
    kpi_aircraft_manufacturers,
    kpi_airline_aircraft_top3,
    kpi_airport_imbalance,
    build_dim_airports,
    build_dim_airlines,
    build_dim_aircraft_models,
    build_dim_countries_continents,
)

logger = logging.getLogger(__name__)


class SilverGoldLoader:
    """Orchestre le chargement Silver et Gold depuis Bronze."""

    def __init__(self, spark: SparkSession, datalake_config):
        """
        Initialiser le loader.

        Args:
            spark: Session Spark
            datalake_config: Configuration du datalake (DatalakeConfig)
        """
        self.spark = spark
        self.config = datalake_config

    def load_silver(self, bronze_df: DataFrame) -> DataFrame:
        """
        Charger la couche Silver.

        Opérations :
        - Nettoyage (dedup, normalisation)
        - Enrichissement (continents, distances)
        - Écriture en Parquet partitionnée

        Args:
            bronze_df: DataFrame Bronze

        Returns:
            DataFrame Silver (fact_flights enrichi)
        """
        logger.info("Loading Silver layer...")

        # Nettoyage et enrichissement
        silver_df = clean_and_enrich_bronze(bronze_df)
        silver_df = silver_df.cache()  # réutilisé par fact + 4 dimensions

        # Écriture du fact en Silver
        silver_path = self.config.SILVER_PATH + "/fact_flights"
        silver_df.write.mode("append").partitionBy("tech_year", "tech_month").parquet(silver_path)
        logger.info(f"✓ Silver fact_flights loaded: {silver_path}")

        # Dimensions dérivées (snapshot courant, overwrite)
        self._load_dimensions(silver_df)

        return silver_df

    def _load_dimensions(self, silver_df: DataFrame) -> None:
        """Construire et écrire les 4 tables de dimensions dans Silver."""
        builders = {
            "dim_airports": build_dim_airports,
            "dim_airlines": build_dim_airlines,
            "dim_aircraft_models": build_dim_aircraft_models,
            "dim_countries_continents": build_dim_countries_continents,
        }
        for name, builder in builders.items():
            dim_df = builder(silver_df)
            path = self.config.get_silver_dim_path(name)
            dim_df.write.mode("overwrite").parquet(path)
            logger.info(f"  ✓ {name}: {dim_df.count()} lignes -> {path}")

    def load_gold(self, silver_df: DataFrame) -> dict:
        """
        Charger les 7 tables Gold (KPIs).

        Chaque KPI est calculé et écrit en Parquet partitionné.

        Args:
            silver_df: DataFrame Silver

        Returns:
            Dict[kpi_name, DataFrame]
        """
        logger.info("Loading Gold layer (7 KPIs)...")

        kpis = {}
        timestamp = datetime.now()
        tech_year = timestamp.strftime("%Y")
        tech_month = timestamp.strftime("%Y-%m")

        # KPI 1 : Airline volumes
        logger.info("  Calculating KPI 1 : Airline volumes...")
        kpis['airline_volumes'] = kpi_airline_volumes(silver_df)
        self._write_gold(kpis['airline_volumes'], "kpi_airline_volumes", tech_year, tech_month)

        # KPI 2 : Continental regional
        logger.info("  Calculating KPI 2 : Continental regional...")
        kpis['continental_regional'] = kpi_continental_regional(silver_df)
        self._write_gold(kpis['continental_regional'], "kpi_continental_regional", tech_year, tech_month)

        # KPI 3 : Longest flight
        logger.info("  Calculating KPI 3 : Longest flight...")
        kpis['longest_flight'] = kpi_longest_flight(silver_df)
        self._write_gold(kpis['longest_flight'], "kpi_longest_flight", tech_year, tech_month)

        # KPI 4 : Continental avg distance
        logger.info("  Calculating KPI 4 : Continental avg distance...")
        kpis['continental_avg_distance'] = kpi_continental_avg_distance(silver_df)
        self._write_gold(kpis['continental_avg_distance'], "kpi_continental_avg_distance", tech_year, tech_month)

        # KPI 5 : Aircraft manufacturers
        logger.info("  Calculating KPI 5 : Aircraft manufacturers...")
        kpis['aircraft_manufacturers'] = kpi_aircraft_manufacturers(silver_df)
        self._write_gold(kpis['aircraft_manufacturers'], "kpi_aircraft_manufacturers", tech_year, tech_month)

        # KPI 6 : Airline aircraft top 3
        logger.info("  Calculating KPI 6 : Airline aircraft top 3...")
        kpis['airline_aircraft_top3'] = kpi_airline_aircraft_top3(silver_df)
        self._write_gold(kpis['airline_aircraft_top3'], "kpi_airline_aircraft_top3", tech_year, tech_month)

        # KPI BONUS : Airport imbalance
        logger.info("  Calculating KPI BONUS : Airport imbalance...")
        kpis['airport_imbalance'] = kpi_airport_imbalance(silver_df)
        self._write_gold(kpis['airport_imbalance'], "kpi_airport_imbalance", tech_year, tech_month)

        logger.info("✓ Gold layer loaded (7 KPIs)")

        return kpis

    def _write_gold(self, df: DataFrame, table_name: str, tech_year: str, tech_month: str):
        """
        Écrire une table Gold en Parquet partitionnée.

        Args:
            df: DataFrame KPI
            table_name: Nom de la table (ex: "kpi_airline_volumes")
            tech_year: Année (ex: "2026")
            tech_month: Mois (ex: "2026-06")
        """
        gold_path = f"{self.config.GOLD_PATH}/{table_name}/tech_year={tech_year}/tech_month={tech_month}"

        df.write.mode("append").parquet(gold_path)

        logger.info(f"    ✓ {table_name} written to {gold_path}")

    def run_full_etl(self, bronze_path: str) -> dict:
        """
        Exécuter le pipeline complet Bronze → Silver → Gold.

        Args:
            bronze_path: Chemin vers les données Bronze

        Returns:
            Dict[silver, gold_kpis] avec les DataFrames
        """
        logger.info("=" * 70)
        logger.info("Starting full ETL (Bronze → Silver → Gold)")
        logger.info("=" * 70)

        # Lire Bronze
        logger.info(f"Reading Bronze from {bronze_path}...")
        bronze_df = self.spark.read.parquet(bronze_path)
        logger.info(f"✓ {bronze_df.count()} rows read from Bronze")

        # Load Silver
        silver_df = self.load_silver(bronze_df)

        # Load Gold
        gold_kpis = self.load_gold(silver_df)

        logger.info("=" * 70)
        logger.info("✅ Full ETL completed successfully")
        logger.info("=" * 70)

        return {
            'silver': silver_df,
            'gold_kpis': gold_kpis,
        }
