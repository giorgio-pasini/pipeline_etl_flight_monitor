"""
Chargement des couches Silver et Gold.

Silver : Données nettoyées et enrichies (fact_flights + dimensions)
Gold : Tables KPI agrégées
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timezone

from config.pipeline_config import PARTITION_COLUMNS_SILVER

from .transformations import (
    clean_and_enrich_bronze,
    enrich_with_dimensions,
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
        self.dim_counts = {}  # cardinalité des dimensions construites (rempli par _load_dimensions)

    def load_silver(self, bronze_df: DataFrame, dim_airports=None, dim_airlines=None) -> DataFrame:
        """
        Charger la couche Silver.

        Opérations :
        - Enrichissement par jointure avec les dimensions de référence (si fournies)
        - Nettoyage (dedup, continent, distance)
        - Écriture en Parquet partitionnée + dimensions

        Args:
            bronze_df: DataFrame Bronze (feed)
            dim_airports, dim_airlines: dimensions de référence bulk (optionnelles)

        Returns:
            DataFrame Silver (fact_flights enrichi)
        """
        logger.info("Loading Silver layer...")

        # Enrichissement par jointure (remplit pays/coords/airline_name depuis les dims)
        fact = enrich_with_dimensions(bronze_df, dim_airports, dim_airlines)

        # Nettoyage + dérivations (continent, distance, manufacturer)
        silver_df = clean_and_enrich_bronze(fact)
        silver_df = silver_df.cache()  # réutilisé par fact + dimensions

        # Écriture du fact en Silver (partitionné jusqu'à l'heure : tech_year/month/day/hour).
        # overwrite + partitionOverwriteMode=dynamic : seule la partition (jour, heure) du batch
        # est remplacée (idempotent au re-run ; l'historique des autres heures est conservé).
        silver_path = self.config.SILVER_PATH + "/fact_flights"
        silver_df.write.mode("overwrite").partitionBy(*PARTITION_COLUMNS_SILVER).parquet(silver_path)
        logger.info(f"✓ Silver fact_flights loaded: {silver_path}")

        # Dimensions : bulk si fournies (référentiels complets), sinon dérivées du fact
        self._load_dimensions(silver_df, dim_airports, dim_airlines)

        return silver_df

    def _load_dimensions(self, silver_df: DataFrame, dim_airports=None, dim_airlines=None) -> None:
        """Écrire les tables de dimensions dérivées du fact dans Silver.

        dim_aircraft_models / dim_countries_continents : toujours dérivés du fact.
        dim_airports / dim_airlines : si **bulk** (fournis), ils sont DÉJÀ persistés en `_current`
        par `dimension_loader` → on ne les réécrit pas (réécrire un DataFrame lu depuis `_current`
        vers `_current` ferait échouer Spark : lecture + overwrite du même chemin). On ne (re)génère
        que s'ils ont été dérivés du fact (fallback, dims non fournies).
        """
        dims = {
            "dim_aircraft_models": build_dim_aircraft_models(silver_df),
            "dim_countries_continents": build_dim_countries_continents(silver_df),
        }
        if dim_airports is None:
            dims["dim_airports"] = build_dim_airports(silver_df)
        if dim_airlines is None:
            dims["dim_airlines"] = build_dim_airlines(silver_df)

        counts = {}
        for name, dim_df in dims.items():
            path = self.config.get_silver_dim_path(name)
            dim_df.write.mode("overwrite").parquet(path)
            n = dim_df.count()
            counts[name] = n
            logger.info(f"  ✓ {name}: {n} lignes -> {path}")
        self.dim_counts = counts

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
        # Partition horodatée jusqu'à l'HEURE (UTC) : chaque run 2 h a sa propre partition de
        # résultats -> retrouvables par (Date, Heure) ; re-run de la même heure = overwrite.
        ts = datetime.now(timezone.utc)
        partition = (
            f"tech_year={ts.strftime('%Y')}/tech_month={ts.strftime('%Y-%m')}"
            f"/tech_day={ts.strftime('%Y-%m-%d')}/tech_hour={ts.strftime('%H')}"
        )

        # KPI 1 : Airline volumes
        logger.info("  Calculating KPI 1 : Airline volumes...")
        kpis['airline_volumes'] = kpi_airline_volumes(silver_df)
        self._write_gold(kpis['airline_volumes'], "kpi_airline_volumes", partition)

        # KPI 2 : Continental regional
        logger.info("  Calculating KPI 2 : Continental regional...")
        kpis['continental_regional'] = kpi_continental_regional(silver_df)
        self._write_gold(kpis['continental_regional'], "kpi_continental_regional", partition)

        # KPI 3 : Longest flight
        logger.info("  Calculating KPI 3 : Longest flight...")
        kpis['longest_flight'] = kpi_longest_flight(silver_df)
        self._write_gold(kpis['longest_flight'], "kpi_longest_flight", partition)

        # KPI 4 : Continental avg distance
        logger.info("  Calculating KPI 4 : Continental avg distance...")
        kpis['continental_avg_distance'] = kpi_continental_avg_distance(silver_df)
        self._write_gold(kpis['continental_avg_distance'], "kpi_continental_avg_distance", partition)

        # KPI 5 : Aircraft manufacturers
        logger.info("  Calculating KPI 5 : Aircraft manufacturers...")
        kpis['aircraft_manufacturers'] = kpi_aircraft_manufacturers(silver_df)
        self._write_gold(kpis['aircraft_manufacturers'], "kpi_aircraft_manufacturers", partition)

        # KPI 6 : Airline aircraft top 3
        logger.info("  Calculating KPI 6 : Airline aircraft top 3...")
        kpis['airline_aircraft_top3'] = kpi_airline_aircraft_top3(silver_df)
        self._write_gold(kpis['airline_aircraft_top3'], "kpi_airline_aircraft_top3", partition)

        # KPI BONUS : Airport imbalance
        logger.info("  Calculating KPI BONUS : Airport imbalance...")
        kpis['airport_imbalance'] = kpi_airport_imbalance(silver_df)
        self._write_gold(kpis['airport_imbalance'], "kpi_airport_imbalance", partition)

        logger.info("✓ Gold layer loaded (7 KPIs)")

        return kpis

    def _write_gold(self, df: DataFrame, table_name: str, partition: str):
        """
        Écrire une table Gold en Parquet, partitionnée jusqu'à l'heure
        (tech_year/month/day/hour).

        Args:
            df: DataFrame KPI
            table_name: Nom de la table (ex: "kpi_airline_volumes")
            partition: chemin de partition « tech_year=…/…/tech_hour=… »
        """
        gold_path = f"{self.config.GOLD_PATH}/{table_name}/{partition}"

        # overwrite : un seul snapshot par (jour, heure) ; re-run de la même heure = remplacement.
        df.write.mode("overwrite").parquet(gold_path)

        logger.info(f"    ✓ {table_name} written to {gold_path}")

    def run_full_etl(self, bronze_path: str = None, *, bronze_df: DataFrame = None,
                     dim_airports=None, dim_airlines=None) -> dict:
        """
        Exécuter le pipeline complet Bronze → Silver → Gold.

        Args:
            bronze_path: Chemin vers les données Bronze (lu si `bronze_df` absent).
            bronze_df: DataFrame Bronze **du batch courant** (snapshot). Fourni par le job
                pour ne traiter QUE le batch courant (et non tout l'historique Bronze).
            dim_airports, dim_airlines: dimensions de référence bulk (optionnelles)

        Returns:
            Dict[silver, gold_kpis] avec les DataFrames
        """
        logger.info("=" * 70)
        logger.info("Starting full ETL (Bronze → Silver → Gold)")
        logger.info("=" * 70)

        # Source : DF du batch courant si fourni (snapshot), sinon relecture du chemin Bronze.
        if bronze_df is None:
            logger.info(f"Reading Bronze from {bronze_path}...")
            bronze_df = self.spark.read.parquet(bronze_path)
        logger.info(f"✓ {bronze_df.count()} rows à traiter (snapshot du batch)")

        # Load Silver (enrichi par jointure avec les dimensions si fournies)
        silver_df = self.load_silver(bronze_df, dim_airports, dim_airlines)

        # Load Gold
        gold_kpis = self.load_gold(silver_df)

        logger.info("=" * 70)
        logger.info("✅ Full ETL completed successfully")
        logger.info("=" * 70)

        return {
            'silver': silver_df,
            'gold_kpis': gold_kpis,
            'dim_counts': self.dim_counts,
        }
