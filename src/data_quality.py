"""
Fonctions de validation et de flagging de qualité des données.

Philosophie :
- Ne JAMAIS échouer en silence — toujours logger et marquer les données problématiques
- Permettre au pipeline de continuer même avec des données incomplètes (fault-tolerant)
- Ajouter des flags explicites (data_quality_flags) pour traçabilité downstream
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, concat_ws, length, upper, trim, expr
)
import logging

_module_logger = logging.getLogger(__name__)


def validate_and_flag_flights(df: DataFrame, logger=None) -> DataFrame:
    """
    Valide les vols et ajoute des flags de qualité.

    Flags possibles (séparés par virgule dans 'data_quality_flags') :
    - MISSING_ORIGIN: origin_iata absent ou vide
    - MISSING_DESTINATION: destination_iata absent ou vide
    - MISSING_AIRLINE: airline_icao absent ou vide
    - MISSING_AIRCRAFT_CODE: aircraft_code absent ou vide
    - MISSING_POSITION: latitude ou longitude absent
    - INVALID_ALTITUDE: altitude <= 0 ou > 50000 ft
    - INVALID_GROUND_SPEED: ground_speed < 0 ou > 600 knots
    - INCONSISTENT_POSITION: lat/lon hors limites géographiques

    Colonnes ajoutées :
    - data_quality_flags : String (concaténation des flags trouvés)
    - is_valid : Boolean (True si 0 flags ET on_ground=0 ET route complète)

    Args:
        df: DataFrame avec schéma flights_raw
        logger: logger instance (optionnel)

    Returns:
        DataFrame enrichi avec colonnes de qualité
    """

    log = logger if logger is not None else _module_logger

    # Normalize: trimmer et uppercase les codes
    df = df \
        .withColumn("origin_iata", trim(upper(col("origin_iata")))) \
        .withColumn("destination_iata", trim(upper(col("destination_iata")))) \
        .withColumn("airline_icao", trim(upper(col("airline_icao")))) \
        .withColumn("aircraft_code", trim(upper(col("aircraft_code"))))

    # Construire les flags
    flags = []

    # Flag 1: origin manquant
    flags.append(
        when((col("origin_iata").isNull()) | (length(col("origin_iata")) == 0), "MISSING_ORIGIN")
        .otherwise(None)
    )

    # Flag 2: destination manquant
    flags.append(
        when((col("destination_iata").isNull()) | (length(col("destination_iata")) == 0), "MISSING_DESTINATION")
        .otherwise(None)
    )

    # Flag 3: airline manquant
    flags.append(
        when((col("airline_icao").isNull()) | (length(col("airline_icao")) == 0), "MISSING_AIRLINE")
        .otherwise(None)
    )

    # Flag 4: aircraft_code manquant (moins critique, beaucoup de vols au sol n'en ont pas)
    flags.append(
        when((col("aircraft_code").isNull()) | (length(col("aircraft_code")) == 0), "MISSING_AIRCRAFT_CODE")
        .otherwise(None)
    )

    # Flag 5: Position manquante (pour les vols en cours)
    flags.append(
        when(
            (col("on_ground") == 0) &
            ((col("latitude").isNull()) | (col("longitude").isNull())),
            "MISSING_POSITION"
        )
        .otherwise(None)
    )

    # Flag 6: Altitude invalide (limites physiques: 0 - 50000 pieds)
    flags.append(
        when(
            col("on_ground") == 0,  # Seulement pour les vols en cours
            when(
                (col("altitude").isNull()) | (col("altitude") < 0) | (col("altitude") > 50000),
                "INVALID_ALTITUDE"
            ).otherwise(None)
        )
        .otherwise(None)
    )

    # Flag 7: Vitesse sol invalide (limites: 0 - 600 nœuds)
    flags.append(
        when(
            col("on_ground") == 0,
            when(
                (col("ground_speed").isNull()) | (col("ground_speed") < 0) | (col("ground_speed") > 600),
                "INVALID_GROUND_SPEED"
            ).otherwise(None)
        )
        .otherwise(None)
    )

    # Flag 8: Position géographique hors limites (-90 à 90 lat, -180 à 180 lon)
    flags.append(
        when(
            col("on_ground") == 0,
            when(
                (col("latitude").isNull()) | (col("latitude") < -90) | (col("latitude") > 90) |
                (col("longitude").isNull()) | (col("longitude") < -180) | (col("longitude") > 180),
                "INCONSISTENT_POSITION"
            ).otherwise(None)
        )
        .otherwise(None)
    )

    # Concaténer les flags non-None
    data_quality_flags = concat_ws(",", *flags)

    df = df.withColumn("data_quality_flags",
                       when(data_quality_flags == "", None).otherwise(data_quality_flags))

    # is_valid : vol utilisable pour les KPIs
    # Critères :
    # - on_ground = 0 (en vol)
    # - origin ET destination présents
    # - airline présent
    # - Aucun flag de qualité (data_quality_flags IS NULL)
    df = df.withColumn(
        "is_valid",
        when(
            (col("on_ground") == 0) &
            (col("origin_iata").isNotNull()) & (length(col("origin_iata")) > 0) &
            (col("destination_iata").isNotNull()) & (length(col("destination_iata")) > 0) &
            (col("airline_icao").isNotNull()) & (length(col("airline_icao")) > 0) &
            (col("data_quality_flags").isNull()),
            True
        ).otherwise(False)
    )

    # Stats pour le log
    total = df.count()
    valid = df.filter(col("is_valid") == True).count()
    on_ground = df.filter(col("on_ground") == 1).count()

    pct_valid = (valid / total * 100) if total > 0 else 0
    pct_on_ground = (on_ground / total * 100) if total > 0 else 0

    log.info(f"Data quality check: {total} vols total | {valid} valides ({pct_valid:.1f}%) | "
             f"{on_ground} au sol ({pct_on_ground:.1f}%)")

    return df


def profile_data_quality(df: DataFrame, logger=None) -> dict:
    """
    Profil complet de la qualité des données.

    Returns:
        dict avec statistiques détaillées
    """

    log = logger if logger is not None else _module_logger

    total = df.count()
    if total == 0:
        log.warning("DataFrame vide — impossible de profiler")
        return {}

    stats = {
        "total_rows": total,
        "valid_rows": df.filter(col("is_valid") == True).count(),
        "on_ground": df.filter(col("on_ground") == 1).count(),
        "on_flight": df.filter(col("on_ground") == 0).count(),
        "missing_origin": df.filter(col("origin_iata").isNull() | (length(col("origin_iata")) == 0)).count(),
        "missing_destination": df.filter(col("destination_iata").isNull() | (length(col("destination_iata")) == 0)).count(),
        "missing_airline": df.filter(col("airline_icao").isNull() | (length(col("airline_icao")) == 0)).count(),
        "missing_aircraft": df.filter(col("aircraft_code").isNull() | (length(col("aircraft_code")) == 0)).count(),
    }

    # Flags les plus courants
    flag_counts = df \
        .filter(col("data_quality_flags").isNotNull()) \
        .select(expr("explode(split(data_quality_flags, ',')) as flag")) \
        .groupBy("flag").count() \
        .orderBy("count", ascending=False)

    stats["top_flags"] = flag_counts.collect() if flag_counts.count() > 0 else []

    # Log
    log.info(f"=== Data Quality Profile ===")
    log.info(f"Total rows: {stats['total_rows']}")
    log.info(f"Valid rows: {stats['valid_rows']} ({stats['valid_rows']/total*100:.1f}%)")
    log.info(f"On ground: {stats['on_ground']} ({stats['on_ground']/total*100:.1f}%)")
    log.info(f"On flight: {stats['on_flight']} ({stats['on_flight']/total*100:.1f}%)")
    log.info(f"Missing origin: {stats['missing_origin']}")
    log.info(f"Missing destination: {stats['missing_destination']}")
    log.info(f"Missing airline: {stats['missing_airline']}")
    log.info(f"Missing aircraft: {stats['missing_aircraft']}")

    if stats["top_flags"]:
        log.info("Top quality flags:")
        for row in stats["top_flags"][:5]:
            log.info(f"  {row['flag']}: {row['count']}")

    return stats
