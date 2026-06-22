"""
Configuration centralisée du datalake.

Cette configuration est utilisée partout dans le pipeline pour :
- Chemins de stockage (bronze, silver, gold)
- Paramètres de rétention
- Timing des batches
- Paramètres Spark
- Logging

Peut être surchargée via variables d'environnement.
"""

import os
from pathlib import Path
from datetime import timedelta


class DatalakeConfig:
    """Configuration du datalake — source unique de vérité."""

    # ============================================================================
    # Racine du datalake
    # ============================================================================

    DATALAKE_ROOT = os.getenv("DATALAKE_ROOT", "datalake")
    if not os.path.isabs(DATALAKE_ROOT):
        DATALAKE_ROOT = str(Path(__file__).parent.parent / DATALAKE_ROOT)

    # ============================================================================
    # Couches du datalake
    # ============================================================================

    BRONZE_PATH = f"{DATALAKE_ROOT}/bronze"
    SILVER_PATH = f"{DATALAKE_ROOT}/silver"
    GOLD_PATH = f"{DATALAKE_ROOT}/gold"

    # ============================================================================
    # Rétention des données (en jours)
    # ============================================================================

    BRONZE_RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", 30))
    SILVER_RETENTION_DAYS = int(os.getenv("SILVER_RETENTION_DAYS", 60))
    GOLD_RETENTION_DAYS = int(os.getenv("GOLD_RETENTION_DAYS", 365))

    # ============================================================================
    # Batch et timing
    # ============================================================================

    BATCH_INTERVAL_HOURS = 2  # Toutes les 2 heures, comme spécifié dans le kata
    BATCH_INTERVAL_MINUTES = BATCH_INTERVAL_HOURS * 60

    COLLECTION_TIMEOUT_MINUTES = 30  # Timeout pour une collecte de zone

    STREAMING_CHECKPOINT_INTERVAL_SECONDS = 300  # 5 minutes entre checkpoints

    # ============================================================================
    # Configuration Spark
    # ============================================================================

    SPARK_APP_NAME = "flight-radar-etl-pipeline"
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    # Tuning pour Spark Core Batch
    SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", 200))
    SPARK_MAX_PARTITIONS_PER_EXECUTOR = 5
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", 4))
    SPARK_EXECUTOR_INSTANCES = int(os.getenv("SPARK_EXECUTOR_INSTANCES", 4))

    # Optimisations
    SPARK_ADAPTIVE_EXECUTION_ENABLED = True
    SPARK_COALESCE_SHUFFLE_PARTITIONS_MIN_PARTITION_NUM = 1

    # ============================================================================
    # Logging
    # ============================================================================

    LOG_PATH = f"{DATALAKE_ROOT}/_logs"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Format de logs JSON pour parsing
    LOG_FORMAT = "json"  # ou "text"

    # ============================================================================
    # API FlightRadarAPI
    # ============================================================================

    API_TIMEOUT_SECONDS = 30
    # Concurrence réduite (anti-429) : moins d'appels simultanés.
    API_MAX_WORKERS_PARALLEL = int(os.getenv("API_MAX_WORKERS_PARALLEL", 3))
    API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", 3))  # Retries sur échec transitoire (feed)

    # Backoff interne de la librairie (RetryPolicy) — couvre tous les appels (dont 429).
    API_RETRY_MAX_ATTEMPTS = int(os.getenv("API_RETRY_MAX_ATTEMPTS", 4))
    API_RETRY_BASE_DELAY = float(os.getenv("API_RETRY_BASE_DELAY", 5.0))
    API_RETRY_MAX_DELAY = float(os.getenv("API_RETRY_MAX_DELAY", 60.0))

    # Authentification FR24 (quota plus élevé). Secrets via env, jamais en dur/loggués.
    FR24_EMAIL = os.getenv("FR24_EMAIL", "")
    FR24_PASSWORD = os.getenv("FR24_PASSWORD", "")

    # Enrichissement par vol (get_flight_details) : DÉSACTIVÉ par défaut — l'enrichissement
    # vient désormais des dimensions bulk (get_airports/get_airlines) jointes en Spark.
    API_ENRICH_DETAILS = os.getenv("API_ENRICH_DETAILS", "false").lower() == "true"

    # Cache des dimensions de référence (jours) : get_airports n'est rechargé
    # que si le cache Silver est plus vieux que ce seuil.
    DIM_CACHE_MAX_AGE_DAYS = int(os.getenv("DIM_CACHE_MAX_AGE_DAYS", 7))

    # Source de la dimension aéroports : "static" (jeu OpenFlights local, fiable, 0 appel,
    # recommandé) ou "api" (get_airports — fragile/lent + 429 en anonyme).
    DIM_AIRPORTS_SOURCE = os.getenv("DIM_AIRPORTS_SOURCE", "static").lower()
    DIM_AIRPORTS_STATIC_PATH = os.getenv(
        "DIM_AIRPORTS_STATIC_PATH",
        str(Path(__file__).parent.parent / "data" / "airports.dat"),
    )

    # Pays pour la dimension aéroports (source "api" uniquement). "ALL" = tous (lent ~30 min,
    # peu fiable à 228) ;
    # par défaut un sous-ensemble à fort trafic (rapide + fiable, couvre l'essentiel).
    # Override via env : liste de noms d'enum Countries séparés par des virgules, ou "ALL".
    DIM_AIRPORTS_COUNTRIES = os.getenv("DIM_AIRPORTS_COUNTRIES", "").strip() or ",".join([
        "UNITED_STATES", "UNITED_KINGDOM", "FRANCE", "GERMANY", "SPAIN", "ITALY",
        "NETHERLANDS", "SWITZERLAND", "IRELAND", "PORTUGAL", "BELGIUM", "AUSTRIA",
        "SWEDEN", "NORWAY", "DENMARK", "FINLAND", "POLAND", "GREECE", "TURKEY",
        "RUSSIA", "UKRAINE", "CZECHIA", "HUNGARY", "ROMANIA", "CROATIA",
        "CHINA", "JAPAN", "SOUTH_KOREA", "INDIA", "INDONESIA", "THAILAND",
        "MALAYSIA", "SINGAPORE", "VIETNAM", "PHILIPPINES", "HONG_KONG", "TAIWAN",
        "UNITED_ARAB_EMIRATES", "SAUDI_ARABIA", "QATAR", "ISRAEL", "PAKISTAN",
        "AUSTRALIA", "NEW_ZEALAND", "CANADA", "MEXICO", "BRAZIL", "ARGENTINA",
        "CHILE", "COLOMBIA", "PERU", "SOUTH_AFRICA", "EGYPT", "MOROCCO",
        "ETHIOPIA", "KENYA", "NIGERIA",
    ])

    FLIGHTS_BATCH_SIZE_LIMIT = 1500  # Limite de vols retournés par get_flights() sans bounds

    # Zones à collecter. L'appel global est plafonné à 1500 vols ; en itérant les zones
    # top-level (bounds), chaque appel monte jusqu'à 5000 vols -> couverture bien plus large.
    COLLECTION_ZONES = [
        "europe", "northamerica", "southamerica", "asia",
        "africa", "oceania", "atlantic", "maldives", "northatlantic",
    ]

    # ============================================================================
    # Qualité des données
    # ============================================================================

    # Seuil d'alerte : si % de vols valides < ce seuil, générer une alerte
    ALERT_THRESHOLD_PCT_VALID = 70

    # Webhook optionnel pour notifier les alertes (Slack/Teams/etc.). Vide = désactivé.
    ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

    # Sauvegarder les rapports de qualité JSON ?
    SAVE_QUALITY_REPORTS = True

    # ============================================================================
    # Transformation Silver/Gold
    # ============================================================================

    # Charger Silver + Gold après Bronze ? (Étape 4+)
    LOAD_SILVER_GOLD = os.getenv("LOAD_SILVER_GOLD", "false").lower() == "true"

    # ============================================================================
    # Méthodes d'accès
    # ============================================================================

    @classmethod
    def get_bronze_flights_path(cls):
        """Chemin complet des vols bruts."""
        return f"{cls.BRONZE_PATH}/flights_raw"

    @classmethod
    def get_bronze_checkpoints_path(cls):
        """Chemin des checkpoints Spark (tolerance failures)."""
        return f"{cls.BRONZE_PATH}/_checkpoints"

    @classmethod
    def get_silver_fact_flights_path(cls):
        """Chemin de la table fact_flights nettoyée."""
        return f"{cls.SILVER_PATH}/fact_flights"

    @classmethod
    def get_silver_dim_path(cls, dim_name: str):
        """Chemin d'une table dimension (snapshot courant)."""
        # dim_name : "dim_airlines", "dim_airports", etc.
        return f"{cls.SILVER_PATH}/{dim_name}/_current"

    @classmethod
    def get_gold_kpi_path(cls, kpi_name: str):
        """Chemin d'une table KPI (partitionnée par date/heure)."""
        # kpi_name : "kpi_airline_volumes", "kpi_continental_regional", etc.
        return f"{cls.GOLD_PATH}/{kpi_name}"

    @classmethod
    def get_log_path(cls):
        """Chemin des logs."""
        return cls.LOG_PATH

    @classmethod
    def get_quality_report_path(cls, date_str: str, hour: int):
        """Chemin d'un rapport de qualité JSON (kpi_date=YYYY-MM-DD, kpi_hour=HH)."""
        return f"{cls.GOLD_PATH}/_quality_logs/{date_str}/{hour:02d}"

    @classmethod
    def as_dict(cls):
        """Exporter la configuration en dict pour logging."""
        return {
            "datalake_root": cls.DATALAKE_ROOT,
            "bronze_path": cls.BRONZE_PATH,
            "silver_path": cls.SILVER_PATH,
            "gold_path": cls.GOLD_PATH,
            "retention_days": {
                "bronze": cls.BRONZE_RETENTION_DAYS,
                "silver": cls.SILVER_RETENTION_DAYS,
                "gold": cls.GOLD_RETENTION_DAYS,
            },
            "batch_interval_hours": cls.BATCH_INTERVAL_HOURS,
            "spark_master": cls.SPARK_MASTER,
            "spark_shuffle_partitions": cls.SPARK_SHUFFLE_PARTITIONS,
            "log_level": cls.LOG_LEVEL,
        }

    @classmethod
    def validate(cls):
        """Valider la configuration (afficher warnings/errors si nécessaire)."""
        import logging
        log = logging.getLogger(__name__)

        # Vérifier que le chemin racine est accessible (création si nécessaire)
        try:
            Path(cls.DATALAKE_ROOT).mkdir(parents=True, exist_ok=True)
            log.info(f"Datalake root: {cls.DATALAKE_ROOT}")
        except Exception as e:
            log.error(f"Impossible de créer/accéder au datalake root: {e}")
            raise

        # Log la configuration
        log.info("Configuration du datalake:")
        for key, value in cls.as_dict().items():
            log.info(f"  {key}: {value}")


# ============================================================================
# Constantes pour partitionnement
# ============================================================================

PARTITION_COLUMNS_BRONZE = ["tech_year", "tech_month", "tech_day", "tech_hour"]
PARTITION_COLUMNS_SILVER = ["tech_year", "tech_month", "tech_day", "tech_hour"]
PARTITION_COLUMNS_GOLD_KPI = ["kpi_date", "kpi_hour"]  # Format différent pour Gold

PARTITION_DATE_FORMAT = "%Y-%m-%d"  # YYYY-MM-DD
PARTITION_TIME_FORMAT = "%H"  # HH (00-23)

# Exemple de chemin complet :
# datalake/bronze/flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-21/tech_hour=14/
# datalake/gold/kpi_airline_volumes/kpi_date=2026-06-21/kpi_hour=14/


# ============================================================================
# Cas d'usage en code
# ============================================================================

if __name__ == "__main__":
    # Test configuration
    print("=== Configuration du datalake ===")
    DatalakeConfig.validate()
    print(f"\nBronze flights path: {DatalakeConfig.get_bronze_flights_path()}")
    print(f"Silver fact flights path: {DatalakeConfig.get_silver_fact_flights_path()}")
    print(f"Silver airlines dim path: {DatalakeConfig.get_silver_dim_path('dim_airlines')}")
    print(f"Gold airline volumes KPI path: {DatalakeConfig.get_gold_kpi_path('kpi_airline_volumes')}")
