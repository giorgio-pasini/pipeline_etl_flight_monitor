"""
Chargement bulk + cache des dimensions de référence (anti-quota).

Au lieu d'enrichir chaque vol via get_flight_details (des milliers d'appels),
on charge UNE fois les référentiels via des appels bulk peu coûteux :
- get_airlines()  -> 1 appel  (nom des compagnies)
- get_airports()  -> 1 appel/pays (249), mis en cache 7j

Les DataFrames produits sont joints au fact en Silver (transformations.enrich_with_dimensions).
"""

import time
import logging
from pathlib import Path
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import current_timestamp

from .flight_extraction import FlightExtractor, retry_with_backoff
from .reference_data import country_code_from_name_expr, continent_code_expr

logger = logging.getLogger(__name__)


_AIRLINES_SCHEMA = StructType([
    StructField("airline_icao", StringType()),
    StructField("airline_iata", StringType()),
    StructField("airline_name", StringType()),
])

_AIRPORTS_SCHEMA = StructType([
    StructField("airport_iata", StringType()),
    StructField("airport_icao", StringType()),
    StructField("airport_name", StringType()),
    StructField("country_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])


def _cache_fresh(path: str, max_age_days: int) -> bool:
    """True si le cache Parquet existe et est plus récent que max_age_days."""
    p = Path(path)
    if not p.exists():
        return False
    files = list(p.rglob("*.parquet"))
    if not files:
        return False
    newest = max(f.stat().st_mtime for f in files)
    age_days = (time.time() - newest) / 86400.0
    return age_days < max_age_days


def load_dim_airlines(spark: SparkSession, api, config) -> Optional[DataFrame]:
    """
    Charger la dimension compagnies (cache-aware).

    Returns:
        DataFrame(airline_icao, airline_iata, airline_name, last_updated) ou None.
    """
    path = config.get_silver_dim_path("dim_airlines")

    if _cache_fresh(path, config.DIM_CACHE_MAX_AGE_DAYS):
        logger.info(f"dim_airlines : cache frais réutilisé ({path})")
        return spark.read.parquet(path)

    try:
        airlines = retry_with_backoff(lambda: api.get_airlines(),
                                      max_retries=3, logger_obj=logger)
    except Exception as e:
        logger.warning(f"get_airlines échoué : {e}")
        return spark.read.parquet(path) if Path(path).exists() else None

    rows = [
        (a.get("ICAO"), a.get("IATA"), a.get("Name"))
        for a in airlines if a.get("ICAO")
    ]
    df = (
        spark.createDataFrame(rows, schema=_AIRLINES_SCHEMA)
        .dropDuplicates(["airline_icao"])
        .withColumn("last_updated", current_timestamp())
    )
    df.write.mode("overwrite").parquet(path)
    logger.info(f"✓ dim_airlines chargée ({df.count()} compagnies) -> {path}")
    return df


def load_dim_airports(spark: SparkSession, api, config) -> Optional[DataFrame]:
    """
    Charger la dimension aéroports (cache-aware), tous pays.

    Returns:
        DataFrame(airport_iata, airport_icao, airport_name, country_name,
                  country_code, continent_code, latitude, longitude, last_updated) ou None.
    """
    path = config.get_silver_dim_path("dim_airports")

    if _cache_fresh(path, config.DIM_CACHE_MAX_AGE_DAYS):
        logger.info(f"dim_airports : cache frais réutilisé ({path})")
        return spark.read.parquet(path)

    try:
        from FlightRadarAPI import Countries
        countries = list(Countries)
        logger.info(f"Chargement bulk des aéroports ({len(countries)} pays)...")
        airports = retry_with_backoff(lambda: api.get_airports(countries),
                                      max_retries=2, logger_obj=logger)
    except Exception as e:
        logger.warning(f"get_airports échoué : {e}")
        return spark.read.parquet(path) if Path(path).exists() else None

    def _f(x):
        try:
            return float(x) if x is not None else None
        except (TypeError, ValueError):
            return None

    rows = []
    for ap in airports:
        iata = getattr(ap, "iata", None)
        if not iata:
            continue
        rows.append((
            iata,
            getattr(ap, "icao", None),
            getattr(ap, "name", None),
            getattr(ap, "country", None),
            _f(getattr(ap, "latitude", None)),
            _f(getattr(ap, "longitude", None)),
        ))

    df = (
        spark.createDataFrame(rows, schema=_AIRPORTS_SCHEMA)
        .dropDuplicates(["airport_iata"])
        .withColumn("country_code", country_code_from_name_expr("country_name"))
        .withColumn("continent_code", continent_code_expr("country_code"))
        .withColumn("last_updated", current_timestamp())
    )
    df.write.mode("overwrite").parquet(path)
    logger.info(f"✓ dim_airports chargée ({df.count()} aéroports) -> {path}")
    return df


def load_all_dimensions(spark: SparkSession, config) -> Dict[str, Optional[DataFrame]]:
    """
    Charger les dimensions de référence (compagnies + aéroports) via une API
    authentifiée (login + RetryPolicy), avec cache.

    Returns:
        {"dim_airlines": DF|None, "dim_airports": DF|None}
    """
    extractor = FlightExtractor(
        timeout_seconds=config.API_TIMEOUT_SECONDS,
        max_workers=config.API_MAX_WORKERS_PARALLEL,
        email=config.FR24_EMAIL or None,
        password=config.FR24_PASSWORD or None,
        retry_max_attempts=config.API_RETRY_MAX_ATTEMPTS,
        retry_base_delay=config.API_RETRY_BASE_DELAY,
        retry_max_delay=config.API_RETRY_MAX_DELAY,
    )
    api = extractor.api

    return {
        "dim_airlines": load_dim_airlines(spark, api, config),
        "dim_airports": load_dim_airports(spark, api, config),
    }
