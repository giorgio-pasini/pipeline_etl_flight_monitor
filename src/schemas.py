"""
Schéma Spark de la couche Bronze (`flights_raw`).

Seul ce schéma est **appliqué** (à l'écriture Bronze, via `flight_extraction`). Les tables
Silver (dimensions + `fact_flights`) sont construites dynamiquement (`dimension_loader`,
`transformations`) et les KPIs Gold le sont aussi — leur structure n'est pas figée par un
StructType ici. Le modèle de données complet est documenté dans DOCUMENTATION.md §2.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
)


# ============================================================================
# COUCHE BRONZE — Données brutes de l'API FlightRadarAPI
# ============================================================================

schema_flights_raw = StructType([
    # Métadonnées d'extraction
    StructField("extraction_timestamp", TimestampType(), nullable=False),
    StructField("batch_id", StringType(), nullable=False),
    StructField("source_zone", StringType(), nullable=True),  # "global", "europe", etc.

    # Données brutes de l'API
    StructField("flight_id", StringType(), nullable=False),  # PK
    StructField("callsign", StringType(), nullable=True),
    StructField("flight_number", StringType(), nullable=True),
    StructField("airline_icao", StringType(), nullable=True),
    StructField("airline_iata", StringType(), nullable=True),
    StructField("aircraft_code", StringType(), nullable=True),
    StructField("registration", StringType(), nullable=True),
    StructField("origin_iata", StringType(), nullable=True),
    StructField("destination_iata", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("altitude", DoubleType(), nullable=True),  # Pieds
    StructField("ground_speed", DoubleType(), nullable=True),  # Nœuds
    StructField("heading", DoubleType(), nullable=True),  # Degrés
    StructField("on_ground", IntegerType(), nullable=True),  # 0 ou 1
    StructField("vertical_speed", DoubleType(), nullable=True),  # fpm
    StructField("icao_24bit", StringType(), nullable=True),

    # Données enrichies (optionnelles, si get_flight_details())
    StructField("aircraft_model", StringType(), nullable=True),
    StructField("airline_name", StringType(), nullable=True),
    StructField("origin_airport_name", StringType(), nullable=True),
    StructField("origin_airport_country_code", StringType(), nullable=True),
    StructField("origin_airport_country_name", StringType(), nullable=True),
    StructField("origin_airport_latitude", DoubleType(), nullable=True),
    StructField("origin_airport_longitude", DoubleType(), nullable=True),
    StructField("destination_airport_name", StringType(), nullable=True),
    StructField("destination_airport_country_code", StringType(), nullable=True),
    StructField("destination_airport_country_name", StringType(), nullable=True),
    StructField("destination_airport_latitude", DoubleType(), nullable=True),
    StructField("destination_airport_longitude", DoubleType(), nullable=True),
    StructField("status_text", StringType(), nullable=True),
])
