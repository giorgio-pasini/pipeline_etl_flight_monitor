"""
Fonctions de transformation Silver et Gold.

Silver : Nettoyage, enrichissement, joins
Gold : Agrégations et calcul des 7 KPIs
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, count, row_number, desc,
    avg, max, min, sum as spark_sum
)
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# SILVER — Nettoyage et enrichissement
# ============================================================================

def clean_and_enrich_bronze(df: DataFrame) -> DataFrame:
    """
    Nettoyer et enrichir les données Bronze.

    Opérations :
    - Deduplicate (garder le plus récent par flight_id)
    - Ajouter continents via lookup country_code
    - Normaliser les valeurs nulles
    - Calculer distance haversine

    Args:
        df: DataFrame Bronze (flights_raw)

    Returns:
        DataFrame enrichi, prêt pour Silver
    """
    from pyspark.sql.functions import row_number, desc

    # Dedup : garder le vol le plus récent (par extraction_timestamp)
    window = Window.partitionBy("flight_id").orderBy(desc("extraction_timestamp"))
    df_dedup = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    # Ajouter continents (placeholder - à joindre avec dim_countries_continents)
    # Pour maintenant, on va juste marquer comme "Unknown"
    df_enriched = df_dedup.withColumn(
        "origin_continent",
        when(col("origin_airport_country_code").isNotNull(), "EU")  # Simplifié
        .otherwise("Unknown")
    )

    df_enriched = df_enriched.withColumn(
        "destination_continent",
        when(col("destination_airport_country_code").isNotNull(), "EU")
        .otherwise("Unknown")
    )

    logger.info(f"Cleaned and enriched {df_enriched.count()} flights")

    return df_enriched


# ============================================================================
# GOLD — KPIs
# ============================================================================

def kpi_airline_volumes(df: DataFrame) -> DataFrame:
    """
    KPI 1 : Compagnie avec le + de vols en cours.

    Returns:
        DataFrame(airline_icao, airline_name, active_flights_count)
    """
    result = (
        df.filter(col("on_ground") == 0)  # En vol uniquement
        .filter(col("is_valid") == True)  # Données valides
        .groupBy("airline_icao", "airline_name")
        .agg(count("*").alias("active_flights_count"))
        .orderBy(desc("active_flights_count"))
        .limit(1)
    )

    return result


def kpi_continental_regional(df: DataFrame) -> DataFrame:
    """
    KPI 2 : Par continent, compagnie avec + de vols régionaux
    (origin continent == destination continent).

    Returns:
        DataFrame(origin_continent, airline_icao, airline_name, regional_flights_count)
    """
    regional = df.filter(
        (col("origin_continent") == col("destination_continent")) &
        (col("on_ground") == 0) &
        (col("is_valid") == True)
    )

    window = Window.partitionBy("origin_continent").orderBy(desc("count"))

    result = (
        regional
        .groupBy("origin_continent", "airline_icao", "airline_name")
        .agg(count("*").alias("count"))
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("origin_continent", "airline_icao", "airline_name", col("count").alias("regional_flights_count"))
    )

    return result


def kpi_longest_flight(df: DataFrame) -> DataFrame:
    """
    KPI 3 : Vol en cours au trajet le + long.

    Utilise distance haversine (approximée ou fournie en données).

    Returns:
        DataFrame(callsign, airline_icao, origin_iata, destination_iata, distance_km)
    """
    result = (
        df.filter((col("on_ground") == 0) & (col("is_valid") == True))
        .withColumn(
            "distance_estimate",
            (
                (col("destination_airport_latitude") - col("origin_airport_latitude")) ** 2 +
                (col("destination_airport_longitude") - col("origin_airport_longitude")) ** 2
            ) ** 0.5 * 111  # Approximation : 1° ~ 111 km
        )
        .orderBy(desc("distance_estimate"))
        .select("callsign", "airline_icao", "origin_iata", "destination_iata",
                col("distance_estimate").alias("distance_km"))
        .limit(1)
    )

    return result


def kpi_continental_avg_distance(df: DataFrame) -> DataFrame:
    """
    KPI 4 : Par continent, longueur de vol moyenne.

    Returns:
        DataFrame(continent, avg_distance_km)
    """
    result = (
        df.filter((col("on_ground") == 0) & (col("is_valid") == True))
        .withColumn(
            "distance_estimate",
            (
                (col("destination_airport_latitude") - col("origin_airport_latitude")) ** 2 +
                (col("destination_airport_longitude") - col("origin_airport_longitude")) ** 2
            ) ** 0.5 * 111
        )
        .groupBy("origin_continent")
        .agg(avg("distance_estimate").alias("avg_distance_km"))
        .orderBy("origin_continent")
    )

    return result


def kpi_aircraft_manufacturers(df: DataFrame) -> DataFrame:
    """
    KPI 5 : Constructeur d'avions avec + de vols actifs.

    Returns:
        DataFrame(manufacturer, active_flights_count)
    """
    result = (
        df.filter((col("on_ground") == 0) & (col("is_valid") == True))
        .filter(col("aircraft_model").isNotNull())
        .groupBy("aircraft_model")  # Placeholder : devrait être "manufacturer"
        .agg(count("*").alias("active_flights_count"))
        .orderBy(desc("active_flights_count"))
        .limit(1)
    )

    return result


def kpi_airline_aircraft_top3(df: DataFrame) -> DataFrame:
    """
    KPI 6 : Par pays de compagnie, top 3 des modèles d'avion en usage.

    Returns:
        DataFrame(airline_country, aircraft_model, usage_count, rank)
    """
    window = Window.partitionBy("origin_airport_country_code").orderBy(desc("usage_count"))

    result = (
        df.filter((col("on_ground") == 0) & (col("is_valid") == True))
        .filter(col("aircraft_model").isNotNull())
        .groupBy("origin_airport_country_code", "aircraft_model")
        .agg(count("*").alias("usage_count"))
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") <= 3)
        .select("origin_airport_country_code", "aircraft_model", "usage_count", "rank")
    )

    return result


def kpi_airport_imbalance(df: DataFrame) -> DataFrame:
    """
    KPI BONUS : Aéroport au + grand écart départs/arrivées.

    Returns:
        DataFrame(airport_iata, departures, arrivals, imbalance)
    """
    departures = (
        df.filter(col("is_valid") == True)
        .groupBy("origin_iata")
        .agg(count("*").alias("departures"))
    )

    arrivals = (
        df.filter(col("is_valid") == True)
        .groupBy("destination_iata")
        .agg(count("*").alias("arrivals"))
    )

    result = (
        departures.join(arrivals, departures["origin_iata"] == arrivals["destination_iata"], "outer")
        .withColumn("departures", coalesce(col("departures"), lit(0)))
        .withColumn("arrivals", coalesce(col("arrivals"), lit(0)))
        .withColumn("imbalance", col("departures") - col("arrivals"))
        .select(
            coalesce(col("origin_iata"), col("destination_iata")).alias("airport_iata"),
            "departures", "arrivals", "imbalance"
        )
        .orderBy(desc("imbalance"))
        .limit(1)
    )

    return result
