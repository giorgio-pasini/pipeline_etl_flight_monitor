"""
Extraction des données de l'API FlightRadarAPI.

Fournit les fonctions pour :
- Collecter les vols par zone (ou globalement)
- Enrichir les vols avec get_flight_details()
- Formater en DataFrames Spark
- Gérer les erreurs et timeouts

À utiliser depuis le job Spark Core Batch.
"""

import logging
import time
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime
import uuid

from FlightRadarAPI import FlightRadar24API
from pyspark.sql import SparkSession, DataFrame

from .schemas import schema_flights_raw

logger = logging.getLogger(__name__)

# Champs dont le schéma attend un DoubleType (l'API renvoie parfois des int)
_FLOAT_FIELDS = (
    "latitude", "longitude", "altitude", "ground_speed", "heading", "vertical_speed",
    "origin_airport_latitude", "origin_airport_longitude",
    "destination_airport_latitude", "destination_airport_longitude",
)
# Champs dont le schéma attend un IntegerType
_INT_FIELDS = ("on_ground",)


def _to_float(x):
    """Coercion robuste vers float (None si vide/non convertible)."""
    if x is None or x == "" or x == "N/A":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _to_int(x):
    """Coercion robuste vers int (None si vide/non convertible)."""
    if x is None or x == "" or x == "N/A":
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    logger_obj: Optional[logging.Logger] = None,
):
    """
    Exécuter `func` avec retries et backoff exponentiel.

    Args:
        func: callable sans argument à exécuter
        max_retries: nombre maximal de tentatives (>= 1)
        base_delay: délai initial en secondes
        backoff_factor: multiplicateur du délai entre tentatives
        exceptions: tuple d'exceptions déclenchant un retry
        logger_obj: logger optionnel

    Returns:
        Le résultat de `func`

    Raises:
        La dernière exception si toutes les tentatives échouent.
    """
    log = logger_obj or logger
    delay = base_delay
    last_exc = None

    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exc = e
            if attempt < max_retries:
                log.warning(
                    f"Tentative {attempt}/{max_retries} échouée ({e}); "
                    f"nouvelle tentative dans {delay:.1f}s"
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                log.error(f"Échec après {max_retries} tentatives: {e}")

    raise last_exc


class FlightExtractor:
    """Classe d'extraction des vols depuis l'API FlightRadarAPI."""

    def __init__(self, timeout_seconds: int = 30, max_workers: int = 8, max_retries: int = 3):
        """
        Initialiser l'extracteur.

        Args:
            timeout_seconds: Timeout pour chaque appel API
            max_workers: Nombre de threads parallèles pour enrichissement
            max_retries: Nombre de tentatives sur échec transitoire de l'API
        """
        self.api = FlightRadar24API(timeout=timeout_seconds, max_workers=max_workers)
        self.timeout_seconds = timeout_seconds
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__)

    def get_flights_for_zone(
        self,
        zone_name: Optional[str] = None,
        airline: Optional[str] = None,
        aircraft_type: Optional[str] = None,
        enrich: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Collecter les vols pour une zone donnée.

        Args:
            zone_name: Nom de la zone ("europe", "northamerica", etc.) ou None pour global
            airline: Filtre ICAO (ex: "DL" pour Delta)
            aircraft_type: Filtre type avion (ex: "B737")
            enrich: Si True, appeler get_flight_details() pour chaque vol

        Returns:
            Liste de dicts avec données de vol

        Raises:
            Exception si l'API échoue (logged, pas re-raised pour fault-tolerance)
        """

        try:
            bounds = None
            if zone_name:
                zones = self.api.get_zones()
                if zone_name not in zones:
                    self.logger.warning(f"Zone inconnue: {zone_name}")
                    return []
                bounds = self.api.get_bounds(zones[zone_name])

            # Appel API protégé par retries + backoff exponentiel (échecs transitoires :
            # timeouts réseau, rate-limit, 5xx). Échoue proprement après max_retries.
            flights = retry_with_backoff(
                lambda: self.api.get_flights(
                    bounds=bounds,
                    airline=airline,
                    aircraft_type=aircraft_type,
                    details=enrich,
                ),
                max_retries=self.max_retries,
                logger_obj=self.logger,
            )

            self.logger.info(
                f"Collecté {len(flights)} vols "
                f"(zone={zone_name or 'global'}, enriched={enrich})"
            )

            return flights

        except Exception as e:
            # Fault-tolerance : on n'interrompt pas le batch, la zone retourne vide
            self.logger.error(f"Erreur lors de la collecte (zone={zone_name}): {e}")
            return []

    def flights_to_dicts(
        self,
        flights: List[Any],
        batch_id: Optional[str] = None,
        zone_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Convertir des objets Flight en dictionnaires plats pour Spark.

        Args:
            flights: Liste d'objets FlightRadarAPI.Flight
            batch_id: Identifiant du batch (pour traçabilité). Généré si absent.
            zone_name: Nom de la zone (pour métadonnées)

        Returns:
            Liste de dicts prêts pour un DataFrame Spark
        """

        if batch_id is None:
            batch_id = str(uuid.uuid4())[:8]

        dicts = []
        timestamp = datetime.utcnow()

        for flight in flights:
            try:
                # Données brutes de l'API
                d = {
                    "extraction_timestamp": timestamp,
                    "batch_id": batch_id,
                    "source_zone": zone_name or "global",
                    "flight_id": flight.id,
                    "callsign": flight.callsign,
                    "flight_number": flight.number,
                    "airline_icao": flight.airline_icao,
                    "airline_iata": flight.airline_iata,
                    "aircraft_code": flight.aircraft_code,
                    "registration": flight.registration,
                    "origin_iata": flight.origin_airport_iata,
                    "destination_iata": flight.destination_airport_iata,
                    "latitude": flight.latitude,
                    "longitude": flight.longitude,
                    "altitude": flight.altitude,
                    "ground_speed": flight.ground_speed,
                    "heading": flight.heading,
                    "on_ground": flight.on_ground,
                    "vertical_speed": flight.vertical_speed,
                    "icao_24bit": flight.icao_24bit,
                }

                # Données enrichies (si disponibles après get_flight_details)
                enrichment_fields = [
                    "aircraft_model",
                    "airline_name",
                    "origin_airport_name",
                    "origin_airport_country_code",
                    "origin_airport_country_name",
                    "origin_airport_latitude",
                    "origin_airport_longitude",
                    "destination_airport_name",
                    "destination_airport_country_code",
                    "destination_airport_country_name",
                    "destination_airport_latitude",
                    "destination_airport_longitude",
                    "status_text",
                ]

                for field in enrichment_fields:
                    d[field] = getattr(flight, field, None)

                # Coercion de types : l'API renvoie parfois des int là où le
                # schéma attend un double (et inversement) -> Spark est strict.
                for f in _FLOAT_FIELDS:
                    d[f] = _to_float(d.get(f))
                for f in _INT_FIELDS:
                    d[f] = _to_int(d.get(f))

                dicts.append(d)

            except Exception as e:
                self.logger.warning(f"Erreur conversion vol {flight.id}: {e}")
                continue

        return dicts

    def flights_to_spark_df(
        self,
        spark: SparkSession,
        flights: List[Any],
        batch_id: str,
        zone_name: Optional[str] = None,
    ) -> DataFrame:
        """
        Convertir des vols en DataFrame Spark avec schéma forcé.

        Args:
            spark: Session Spark
            flights: Liste d'objets Flight
            batch_id: Identifiant du batch
            zone_name: Nom de la zone

        Returns:
            DataFrame avec schéma schema_flights_raw
        """

        dicts = self.flights_to_dicts(flights, batch_id, zone_name)

        if not dicts:
            self.logger.warning("Aucun vol à convertir en DataFrame")
            # Retourner un DataFrame vide avec le bon schéma
            return spark.createDataFrame([], schema=schema_flights_raw)

        df = spark.createDataFrame(dicts, schema=schema_flights_raw)

        self.logger.info(f"DataFrame créé: {df.count()} vols, {len(df.columns)} colonnes")

        return df

    def collect_and_convert(
        self,
        spark: SparkSession,
        zones: Optional[List[str]] = None,
        enrich: bool = False,
    ) -> DataFrame:
        """
        Collecter les vols de multiple zones et les convertir en un seul DataFrame.

        Args:
            spark: Session Spark
            zones: Liste des zones à collecter (None = ['global'])
            enrich: Si True, enrichir avec get_flight_details()

        Returns:
            DataFrame unifié (union de toutes les zones)
        """

        if zones is None:
            zones = ["global"]

        batch_id = str(uuid.uuid4())[:8]
        dfs = []

        for zone in zones:
            zone_name = zone if zone != "global" else None
            flights = self.get_flights_for_zone(
                zone_name=zone_name,
                enrich=enrich
            )

            if flights:
                df = self.flights_to_spark_df(spark, flights, batch_id, zone)
                dfs.append(df)

        if not dfs:
            self.logger.warning("Aucun DataFrame collecté")
            return spark.createDataFrame([], schema=schema_flights_raw)

        # Union de tous les DataFrames
        result_df = dfs[0]
        for df in dfs[1:]:
            result_df = result_df.union(df)

        # Dédup cross-zones : un même vol peut apparaître dans des zones limitrophes
        # (les bounds se chevauchent). On garde une occurrence par flight_id.
        result_df = result_df.dropDuplicates(["flight_id"])

        self.logger.info(f"Batch {batch_id}: total {result_df.count()} vols (dédupliqués)")

        return result_df


def extract_flights_batch(
    spark: SparkSession,
    config: Dict[str, Any],
) -> DataFrame:
    """
    Fonction de niveau supérieur pour extraction d'un batch.

    À utiliser depuis le job Spark Core Batch.

    Args:
        spark: Session Spark
        config: dict avec clés:
            - zones: List[str] (ou ["global"])
            - enrich: bool (si enrichir avec get_flight_details)
            - timeout: int (timeout API en secondes)

    Returns:
        DataFrame avec schéma schema_flights_raw
    """

    extractor = FlightExtractor(
        timeout_seconds=config.get("timeout", 30),
        max_workers=config.get("max_workers", 8),
        max_retries=config.get("max_retries", 3),
    )

    df = extractor.collect_and_convert(
        spark=spark,
        zones=config.get("zones", ["global"]),
        enrich=config.get("enrich", False)
    )

    return df


if __name__ == "__main__":
    # Test
    import logging
    logging.basicConfig(level=logging.INFO)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("FlightExtractionTest").getOrCreate()

    extractor = FlightExtractor()
    df = extractor.collect_and_convert(spark, zones=["global"], enrich=False)

    print(f"Extracted {df.count()} flights")
    df.show(3)

    spark.stop()
