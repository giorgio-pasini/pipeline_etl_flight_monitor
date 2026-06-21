"""
Fonctions de transformation Silver et Gold.

Silver : Nettoyage, enrichissement (continent, constructeur), distance
Gold : Agrégations et calcul des 7 KPIs

Note : les sorties Gold sont des tables "larges" prêtes à l'usage (pas
strictement le star-schema des dimensions, non matérialisées dans ce POC).
Chaque KPI inclut une colonne `computed_at` pour la traçabilité.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, lit, count, row_number, desc,
    avg, min as spark_min, max as spark_max, current_timestamp,
    radians, sin, cos, asin, sqrt, pow as spark_pow
)
from pyspark.sql.window import Window

from .reference_data import continent_code_expr, manufacturer_expr

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0


# ============================================================================
# SILVER — Nettoyage et enrichissement
# ============================================================================

def _haversine_km(lat1, lon1, lat2, lon2):
    """Expression Spark : distance haversine (km) entre deux points."""
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        spark_pow(sin(dlat / 2), 2) +
        cos(radians(lat1)) * cos(radians(lat2)) * spark_pow(sin(dlon / 2), 2)
    )
    return lit(2 * EARTH_RADIUS_KM) * asin(sqrt(a))


def clean_and_enrich_bronze(df: DataFrame) -> DataFrame:
    """
    Nettoyer et enrichir les données Bronze.

    Opérations :
    - Déduplication (garder le vol le plus récent par flight_id)
    - Dériver le continent depuis le country_code (origine/destination)
    - Dériver le constructeur depuis le code avion
    - Calculer la distance haversine origine→destination (km)

    Args:
        df: DataFrame Bronze (flights_raw, contient is_valid / data_quality_flags)

    Returns:
        DataFrame enrichi, prêt pour Silver
    """
    # Dedup : garder le vol le plus récent (par extraction_timestamp)
    window = Window.partitionBy("flight_id").orderBy(desc("extraction_timestamp"))
    df_dedup = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    # Enrichissement : continents (depuis le country_code des aéroports)
    df_enriched = (
        df_dedup
        .withColumn("origin_continent", continent_code_expr("origin_airport_country_code"))
        .withColumn("destination_continent", continent_code_expr("destination_airport_country_code"))
        .withColumn("manufacturer", manufacturer_expr("aircraft_code"))
    )

    # Distance haversine (km) si coordonnées aéroports disponibles
    df_enriched = df_enriched.withColumn(
        "distance_km",
        when(
            col("origin_airport_latitude").isNotNull() &
            col("origin_airport_longitude").isNotNull() &
            col("destination_airport_latitude").isNotNull() &
            col("destination_airport_longitude").isNotNull(),
            _haversine_km(
                col("origin_airport_latitude"), col("origin_airport_longitude"),
                col("destination_airport_latitude"), col("destination_airport_longitude"),
            )
        ).otherwise(lit(None))
    )

    return df_enriched


# ============================================================================
# GOLD — KPIs
# ============================================================================

def kpi_airline_volumes(df: DataFrame) -> DataFrame:
    """KPI 1 : Compagnie avec le + de vols en cours."""
    return (
        df.filter((col("on_ground") == 0) & (col("is_valid") == True))
        .groupBy("airline_icao", "airline_name")
        .agg(count("*").alias("active_flights_count"))
        .orderBy(desc("active_flights_count"))
        .limit(1)
        .withColumn("computed_at", current_timestamp())
    )


def kpi_continental_regional(df: DataFrame) -> DataFrame:
    """
    KPI 2 : Par continent, compagnie avec + de vols régionaux
    (continent origine == continent destination, connus).
    """
    regional = df.filter(
        (col("origin_continent") == col("destination_continent")) &
        (col("origin_continent") != lit("UNKNOWN")) &
        (col("on_ground") == 0) &
        (col("is_valid") == True)
    )

    window = Window.partitionBy("origin_continent").orderBy(desc("regional_flights_count"))

    return (
        regional
        .groupBy("origin_continent", "airline_icao", "airline_name")
        .agg(count("*").alias("regional_flights_count"))
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .drop("rank")
        .withColumn("computed_at", current_timestamp())
    )


def kpi_longest_flight(df: DataFrame) -> DataFrame:
    """KPI 3 : Vol en cours au trajet le + long (distance haversine)."""
    return (
        df.filter(
            (col("on_ground") == 0) & (col("is_valid") == True) &
            col("distance_km").isNotNull()
        )
        .orderBy(desc("distance_km"))
        .select("callsign", "airline_icao", "airline_name",
                "origin_iata", "destination_iata", "distance_km")
        .limit(1)
        .withColumn("computed_at", current_timestamp())
    )


def kpi_continental_avg_distance(df: DataFrame) -> DataFrame:
    """KPI 4 : Par continent (origine), distance de vol moyenne."""
    return (
        df.filter(
            (col("on_ground") == 0) & (col("is_valid") == True) &
            col("distance_km").isNotNull() &
            (col("origin_continent") != lit("UNKNOWN"))
        )
        .groupBy("origin_continent")
        .agg(
            avg("distance_km").alias("avg_distance_km"),
            spark_min("distance_km").alias("min_distance_km"),
            spark_max("distance_km").alias("max_distance_km"),
            count("*").alias("flight_count"),
        )
        .orderBy("origin_continent")
        .withColumn("computed_at", current_timestamp())
    )


def kpi_aircraft_manufacturers(df: DataFrame) -> DataFrame:
    """KPI 5 : Constructeur d'avions avec le + de vols actifs."""
    return (
        df.filter(
            (col("on_ground") == 0) & (col("is_valid") == True) &
            (col("manufacturer") != lit("Other"))
        )
        .groupBy("manufacturer")
        .agg(count("*").alias("active_flights_count"))
        .orderBy(desc("active_flights_count"))
        .limit(1)
        .withColumn("computed_at", current_timestamp())
    )


def kpi_airline_aircraft_top3(df: DataFrame) -> DataFrame:
    """KPI 6 : Par pays de compagnie, top 3 des modèles d'avion en usage.

    Le pays de la compagnie est approximé par le pays de l'aéroport d'origine
    (faute de dimension compagnie enrichie dans ce POC).
    """
    window = Window.partitionBy("origin_airport_country_code").orderBy(desc("usage_count"))

    return (
        df.filter(
            (col("on_ground") == 0) & (col("is_valid") == True) &
            col("aircraft_code").isNotNull() &
            col("origin_airport_country_code").isNotNull()
        )
        .groupBy("origin_airport_country_code", "aircraft_code")
        .agg(count("*").alias("usage_count"))
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") <= 3)
        .withColumn("computed_at", current_timestamp())
    )


def kpi_airport_imbalance(df: DataFrame) -> DataFrame:
    """KPI BONUS : Aéroport au + grand écart |départs - arrivées|."""
    valid = df.filter(col("is_valid") == True)

    departures = valid.groupBy("origin_iata").agg(count("*").alias("departures"))
    arrivals = valid.groupBy("destination_iata").agg(count("*").alias("arrivals"))

    joined = (
        departures.join(
            arrivals,
            departures["origin_iata"] == arrivals["destination_iata"],
            "outer",
        )
        .withColumn("departures", coalesce(col("departures"), lit(0)))
        .withColumn("arrivals", coalesce(col("arrivals"), lit(0)))
        .withColumn(
            "airport_iata",
            coalesce(col("origin_iata"), col("destination_iata")),
        )
        .withColumn("imbalance", col("departures") - col("arrivals"))
    )

    from pyspark.sql.functions import abs as spark_abs

    return (
        joined
        .withColumn("imbalance_abs", spark_abs(col("imbalance")))
        .select("airport_iata", "departures", "arrivals", "imbalance", "imbalance_abs")
        .orderBy(desc("imbalance_abs"))
        .limit(1)
        .withColumn("computed_at", current_timestamp())
    )


# ============================================================================
# SILVER — Dimensions (dérivées du fact enrichi, en Spark)
# ============================================================================

def build_dim_airports(df: DataFrame) -> DataFrame:
    """
    dim_airports : aéroports distincts (origine ∪ destination) avec pays/coords/continent.

    Dérivé du fact enrichi (clean_and_enrich_bronze a déjà ajouté *_continent).
    """
    origin = df.select(
        col("origin_iata").alias("airport_iata"),
        col("origin_airport_name").alias("airport_name"),
        col("origin_airport_country_code").alias("country_code"),
        col("origin_airport_country_name").alias("country_name"),
        col("origin_continent").alias("continent_code"),
        col("origin_airport_latitude").alias("latitude"),
        col("origin_airport_longitude").alias("longitude"),
    )
    dest = df.select(
        col("destination_iata").alias("airport_iata"),
        col("destination_airport_name").alias("airport_name"),
        col("destination_airport_country_code").alias("country_code"),
        col("destination_airport_country_name").alias("country_name"),
        col("destination_continent").alias("continent_code"),
        col("destination_airport_latitude").alias("latitude"),
        col("destination_airport_longitude").alias("longitude"),
    )
    return (
        origin.union(dest)
        .filter(col("airport_iata").isNotNull() & (col("airport_iata") != ""))
        .dropDuplicates(["airport_iata"])
        .withColumn("last_updated", current_timestamp())
    )


def build_dim_airlines(df: DataFrame) -> DataFrame:
    """dim_airlines : compagnies distinctes (icao, iata, name)."""
    return (
        df.select("airline_icao", "airline_iata", "airline_name")
        .filter(col("airline_icao").isNotNull() & (col("airline_icao") != ""))
        .dropDuplicates(["airline_icao"])
        .withColumn("last_updated", current_timestamp())
    )


def build_dim_aircraft_models(df: DataFrame) -> DataFrame:
    """dim_aircraft_models : modèles distincts (code, model, manufacturer)."""
    return (
        df.select("aircraft_code", "aircraft_model", "manufacturer")
        .filter(col("aircraft_code").isNotNull() & (col("aircraft_code") != ""))
        .dropDuplicates(["aircraft_code"])
        .withColumn("last_updated", current_timestamp())
    )


def build_dim_countries_continents(df: DataFrame) -> DataFrame:
    """dim_countries_continents : pays distincts (code, name, continent)."""
    origin = df.select(
        col("origin_airport_country_code").alias("country_code"),
        col("origin_airport_country_name").alias("country_name"),
        col("origin_continent").alias("continent_code"),
    )
    dest = df.select(
        col("destination_airport_country_code").alias("country_code"),
        col("destination_airport_country_name").alias("country_name"),
        col("destination_continent").alias("continent_code"),
    )
    return (
        origin.union(dest)
        .filter(col("country_code").isNotNull() & (col("country_code") != ""))
        .dropDuplicates(["country_code"])
        .withColumn("last_updated", current_timestamp())
    )
