"""
Chargement bulk + cache des dimensions de référence (anti-quota).

Au lieu d'enrichir chaque vol via get_flight_details (des milliers d'appels),
on charge UNE fois les référentiels via des appels bulk peu coûteux :
- get_airlines()  -> 1 appel  (nom des compagnies)
- get_airports()  -> 1 appel/pays (249), mis en cache 7j

Les DataFrames produits sont joints au fact en Silver (transformations.enrich_with_dimensions).
"""

import os
import time
import logging
import urllib.request
import urllib.error
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


def _read_static_airports(path: str, to_float):
    """Lire le jeu OpenFlights airports.dat (CSV) -> liste de tuples.

    Format : id, name, city, country, IATA, ICAO, lat, lon, alt, ...
    Retourne (iata, icao, name, country_name, latitude, longitude) pour les
    aéroports ayant un code IATA valide.
    """
    import csv
    p = Path(path)
    if not p.exists():
        return []
    rows = []
    with open(p, encoding="utf-8") as f:
        for rec in csv.reader(f):
            if len(rec) < 8:
                continue
            iata = rec[4].strip()
            if not iata or iata == "\\N" or len(iata) != 3:
                continue
            rows.append((
                iata,
                (rec[5].strip() if rec[5].strip() != "\\N" else None),  # icao
                rec[1].strip(),                                          # name
                rec[3].strip(),                                          # country_name
                to_float(rec[6]),                                        # lat
                to_float(rec[7]),                                        # lon
            ))
    return rows


def ensure_airports_dataset(path: str, url: str, max_age_days: int, timeout: int = 30) -> bool:
    """Garantir la présence + la fraîcheur du jeu OpenFlights ``airports.dat``.

    - **absent**  → téléchargé depuis ``url`` et posé à ``path`` (dossier parent créé) ;
    - **périmé**  (date de modif > ``max_age_days`` jours) → rafraîchi ;
    - **frais**   → aucune action (no-op).

    En cas d'échec réseau : si un fichier existe déjà on le **conserve** (fallback hors-ligne)
    et on retourne True ; s'il est absent on retourne False (``load_dim_airports`` gère alors
    le repli sur cache/None).

    Returns:
        True si un fichier exploitable est en place à ``path``, False sinon.
    """
    p = Path(path)

    if p.exists():
        age_days = (time.time() - p.stat().st_mtime) / 86400.0
        if age_days < max_age_days:
            logger.info(f"airports.dat à jour ({age_days:.1f}j < {max_age_days}j) : {p}")
            return True
        logger.info(f"airports.dat périmé ({age_days:.1f}j ≥ {max_age_days}j) → rafraîchissement")
    else:
        logger.info(f"airports.dat absent → téléchargement depuis {url}")

    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read()
        if not data:
            raise ValueError("réponse vide")
        tmp.write_bytes(data)
        os.replace(tmp, p)  # remplacement atomique
        logger.info(f"✓ airports.dat écrit ({len(data) // 1024} Ko) : {p}")
        return True
    except (urllib.error.URLError, ValueError, OSError) as e:
        if p.exists():
            logger.warning(f"Téléchargement airports.dat échoué ({e}) → repli sur le fichier existant")
            return True
        logger.error(f"Téléchargement airports.dat échoué et aucun fichier local ({e})")
        return False


def _resolve_countries(config):
    """Résoudre la liste de pays (enum Countries) depuis config.DIM_AIRPORTS_COUNTRIES.

    "ALL" -> tous ; sinon liste de noms d'enum (les inconnus sont ignorés).
    """
    from FlightRadarAPI import Countries
    spec = getattr(config, "DIM_AIRPORTS_COUNTRIES", "ALL")
    if spec.strip().upper() == "ALL":
        return list(Countries)
    out = []
    for name in spec.split(","):
        member = getattr(Countries, name.strip(), None)
        if member is not None:
            out.append(member)
        else:
            logger.warning(f"Pays inconnu dans DIM_AIRPORTS_COUNTRIES: {name.strip()}")
    return out or list(Countries)


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

    def _f(x):
        try:
            return float(x) if x not in (None, "", "\\N") else None
        except (TypeError, ValueError):
            return None

    # Source statique (OpenFlights) — fiable, sans appel API ni quota.
    if getattr(config, "DIM_AIRPORTS_SOURCE", "static") == "static":
        # Bootstrap auto : télécharge/rafraîchit le jeu si absent ou périmé (TTL config).
        ensure_airports_dataset(
            config.DIM_AIRPORTS_STATIC_PATH,
            config.DIM_AIRPORTS_STATIC_URL,
            config.DIM_AIRPORTS_MAX_AGE_DAYS,
            timeout=config.API_TIMEOUT_SECONDS,
        )
        rows = _read_static_airports(config.DIM_AIRPORTS_STATIC_PATH, _f)
        if not rows:
            logger.warning(f"Fichier aéroports statique vide/introuvable : {config.DIM_AIRPORTS_STATIC_PATH}")
            return spark.read.parquet(path) if Path(path).exists() else None
        logger.info(f"dim_airports : {len(rows)} aéroports depuis le jeu statique")
    else:
        try:
            countries = _resolve_countries(config)
            logger.info(f"Chargement bulk des aéroports ({len(countries)} pays)...")
            airports = retry_with_backoff(lambda: api.get_airports(countries),
                                          max_retries=2, logger_obj=logger)
        except Exception as e:
            logger.warning(f"get_airports échoué : {e}")
            return spark.read.parquet(path) if Path(path).exists() else None
        rows = [
            (getattr(ap, "iata", None), getattr(ap, "icao", None), getattr(ap, "name", None),
             getattr(ap, "country", None), _f(getattr(ap, "latitude", None)),
             _f(getattr(ap, "longitude", None)))
            for ap in airports if getattr(ap, "iata", None)
        ]

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
