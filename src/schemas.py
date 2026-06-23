"""
Définition des schémas Spark pour toutes les couches du pipeline (Bronze, Silver, Gold).

Ces schémas garantissent :
- Cohérence de type à travers le pipeline
- Validation lors de la lecture/écriture
- Documentation du modèle de données
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, TimestampType,
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


# ============================================================================
# COUCHE SILVER — Tables nettoyées et enrichies
# ============================================================================

schema_dim_airlines = StructType([
    StructField("airline_icao", StringType(), nullable=False),  # PK
    StructField("airline_iata", StringType(), nullable=True),
    StructField("airline_name", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("n_aircrafts_fleet", IntegerType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

schema_dim_airports = StructType([
    StructField("airport_iata", StringType(), nullable=False),  # PK
    StructField("airport_icao", StringType(), nullable=True),
    StructField("airport_name", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("country_name", StringType(), nullable=True),
    StructField("continent_code", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("altitude_feet", DoubleType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

schema_dim_aircraft_models = StructType([
    StructField("aircraft_code", StringType(), nullable=False),  # PK
    StructField("aircraft_model", StringType(), nullable=True),
    StructField("manufacturer", StringType(), nullable=True),  # "Boeing", "Airbus", etc.
    StructField("aircraft_family", StringType(), nullable=True),  # "737", "A320", etc.
    StructField("last_updated", TimestampType(), nullable=False),
])

schema_dim_countries_continents = StructType([
    StructField("country_code", StringType(), nullable=False),  # PK, ISO 3166-1 alpha-2
    StructField("country_name", StringType(), nullable=True),
    StructField("continent_code", StringType(), nullable=True),
    StructField("continent_name", StringType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

schema_fact_flights = StructType([
    StructField("flight_id", StringType(), nullable=False),  # PK
    StructField("batch_id", StringType(), nullable=False),
    StructField("extraction_timestamp", TimestampType(), nullable=False),

    # Dimensions (foreign keys)
    StructField("airline_icao", StringType(), nullable=False),
    StructField("origin_airport_iata", StringType(), nullable=False),
    StructField("destination_airport_iata", StringType(), nullable=False),
    StructField("aircraft_code", StringType(), nullable=True),

    # Mesures (état du vol)
    StructField("callsign", StringType(), nullable=True),
    StructField("flight_number", StringType(), nullable=True),
    StructField("registration", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("altitude_feet", DoubleType(), nullable=True),
    StructField("ground_speed_knots", DoubleType(), nullable=True),
    StructField("heading_degrees", DoubleType(), nullable=True),
    StructField("vertical_speed_fpm", DoubleType(), nullable=True),
    StructField("on_ground", IntegerType(), nullable=True),  # 0 ou 1

    # Données calculées
    StructField("distance_nm", DoubleType(), nullable=True),  # Distance haversine
    StructField("data_quality_flags", StringType(), nullable=True),  # Flags séparés par virgule
    StructField("is_valid", BooleanType(), nullable=False),  # True si utilisable pour KPIs
])


# ============================================================================
# COUCHE GOLD — Tables KPI
# ============================================================================
# Les 7 tables Gold sont produites dynamiquement par `src/transformations.py`
# (chaque KPI inclut `computed_at`) et écrites partitionnées tech_year/month/day.
# Leur structure n'est pas figée par un schéma forcé ici ; voir DOCUMENTATION.md §2.


# ============================================================================
# Dictionnaire de référence (pour accès facile / documentation)
# ============================================================================

SCHEMAS = {
    # Bronze
    "flights_raw": schema_flights_raw,

    # Silver
    "dim_airlines": schema_dim_airlines,
    "dim_airports": schema_dim_airports,
    "dim_aircraft_models": schema_dim_aircraft_models,
    "dim_countries_continents": schema_dim_countries_continents,
    "fact_flights": schema_fact_flights,
}
