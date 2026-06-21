# Documentation de développement — Pipeline ETL de trafic aérien

**Date de début:** 2026-06-21  
**Client:** Data Engineering Technical Test — Exalt  
**Langage:** French  
**Objectif:** Construire un pipeline ETL temps-réel robuste, observable et systématique pour l'analyse du trafic aérien mondial.

---

## Table des matières

1. [Étape 1 : Modélisation des données](#étape-1--modélisation-des-données)
2. [Étape 2 : Structure du datalake](#étape-2--structure-du-datalake)
3. [Étape 3 : POC Spark Batch](#étape-3--poc-spark-batch)
3.5. [Étape 3.5 : Test-Based Development](#étape-35--test-based-development)
4. [Étape 4 : POC Transformation & KPIs](#étape-4--poc-transformation--kpis)
5. [Étape 5 : Stratégie de partitionnement](#étape-5--stratégie-de-partitionnement)
6. [Étape 6 : Logging & Monitoring](#étape-6--logging--monitoring)
7. [Étape 7 : Job Spark final](#étape-7--job-spark-final)
8. [Étape 8 : Dashboard Streamlit](#étape-8--dashboard-streamlit)
9. [Étape 9 : Gestion des erreurs](#étape-9--gestion-des-erreurs)

---

# Étape 1 : Modélisation des données

## 1.1 Contexte

L'exploration notebook (`notebook_exploration.ipynb`) a permis de découvrir la structure brute de l'API FlightRadarAPI :
- **Source primaire :** `get_flights()` retourne ~1500 vols max par appel (sans paramètres de zone)
- **Enrichissement :** `get_flight_details(flight)` ajoute ~50 attributs (modèle avion, noms aéroports, pays/continents)
- **Dimensions :** `get_airports()`, `get_airlines()`, `get_zones()`

## 1.2 Principes de conception

### Architecture en couches (Medallion/Delta Lake)

Le datalake sera organisé en trois couches :

1. **Bronze (Raw)** : Données brutes de l'API, telles quelles, avec métadonnées d'extraction
2. **Silver (Cleaned)** : Données nettoyées, enrichies, validées, partitionnées
3. **Gold (Analytics)** : Vues agrégées prêtes pour les KPIs et le dashboard

### Choix technologiques

- **Format de stockage :** **Parquet** (comprimé, efficace, schéma, support Spark/pandas)
- **Framework :** **Apache Spark Core (batch API)** - Pas Structured Streaming (qui est pour temps-réel continu)
- **Paradigme :** Job batch orchestré (1 exécution/2 heures via Airflow/cron) - déclenchement externe
- **Structuration temporelle :** Partitionnement par `tech_year/tech_month/tech_day/tech_hour` (cf. spec kata)

---

## 1.3 Modèle de données — Tables de faits et dimensions

### 1.3.1 Couche Bronze : Table `flights_raw`

**Source :** `get_flights()` + `get_flight_details()` (enrichissement optionnel)

```
flights_raw (Parquet)
├─ Métadonnées d'extraction
│  ├─ extraction_timestamp (timestamp)        # Quand le vol a été extrait
│  ├─ batch_id (string)                       # Identifiant du batch
│  └─ source_zone (string)                    # Zone géographique collectée (ou "global")
│
├─ Données brutes de Flight
│  ├─ flight_id (string, PK)                  # Identifiant unique FR24
│  ├─ callsign (string)                       # Indicatif (ex: "DLH123")
│  ├─ flight_number (string)                  # Numéro de vol
│  ├─ airline_icao (string)                   # Code ICAO compagnie (ex: "DAL")
│  ├─ aircraft_code (string)                  # Type avion (ex: "B737")
│  ├─ registration (string)                   # Immatriculation avion
│  ├─ origin_iata (string)                    # Aéroport départ (IATA, ex: "CDG")
│  ├─ destination_iata (string)               # Aéroport arrivée (IATA)
│  ├─ latitude (double)                       # Position en temps réel
│  ├─ longitude (double)
│  ├─ altitude (double)                       # Pieds
│  ├─ ground_speed (double)                   # Nœuds
│  ├─ heading (double)                        # Degrés (0-360)
│  ├─ on_ground (int)                         # 0 = en vol, 1 = au sol
│  ├─ vertical_speed (double)                 # Pieds/minute
│  └─ icao_24bit (string)                     # Adresse ICAO avion
│
└─ Données enrichies (si details=True)
   ├─ aircraft_model (string)                 # Model complet (ex: "Boeing 737-800")
   ├─ airline_name (string)                   # Nom compagnie
   ├─ origin_airport_name (string)
   ├─ origin_airport_country_code (string)    # Code pays (ex: "FR")
   ├─ origin_airport_country_name (string)
   ├─ origin_airport_latitude (double)        # Position exacte aéroport
   ├─ origin_airport_longitude (double)
   ├─ destination_airport_name (string)
   ├─ destination_airport_country_code (string)
   ├─ destination_airport_country_name (string)
   ├─ destination_airport_latitude (double)
   ├─ destination_airport_longitude (double)
   ├─ status_text (string)                    # État du vol ("En vol", "Atterrissage", etc.)
   └─ trail (array<struct>)                   # Historique de position (optionnel, comprimé)
```

**Partitionnement :** `tech_year/tech_month/tech_day/tech_hour`

**Retention :** 30 jours (coût de stockage vs. valeur analytique)

---

### 1.3.2 Couche Silver : Tables nettoyées et enrichies

#### Table `dim_airlines`

```
dim_airlines (Parquet)
├─ airline_icao (string, PK)
├─ airline_iata (string)
├─ airline_name (string)
├─ country_code (string)                    # Pays de la compagnie
├─ n_aircrafts_fleet (int)                  # Nombre d'appareils
├─ is_active (boolean)                      # Flag: compagnie toujours active ?
└─ last_updated (timestamp)
```

#### Table `dim_airports`

```
dim_airports (Parquet)
├─ airport_iata (string, PK)
├─ airport_icao (string)
├─ airport_name (string)
├─ country_code (string)
├─ country_name (string)
├─ continent_code (string)                  # Dérivé de country_code (voir mapping)
├─ latitude (double)
├─ longitude (double)
├─ altitude_feet (double)
└─ last_updated (timestamp)
```

#### Table `dim_aircraft_models`

```
dim_aircraft_models (Parquet)
├─ aircraft_code (string, PK)               # B737, A320, etc.
├─ aircraft_model (string)                  # "Boeing 737-800"
├─ manufacturer (string)                    # "Boeing", "Airbus", etc.
├─ aircraft_family (string)                 # "737", "A320", etc.
└─ last_updated (timestamp)
```

#### Table `dim_countries_continents`

```
dim_countries_continents (Parquet)
├─ country_code (string, PK)                # ISO 3166-1 alpha-2
├─ country_name (string)
├─ continent_code (string)                  # EU, AS, NA, SA, OC, AF, etc.
├─ continent_name (string)
└─ last_updated (timestamp)
```

#### Table `fact_flights` (principale)

```
fact_flights (Parquet, partitionnée)
├─ flight_id (string, PK)
├─ batch_id (string)                        # Clé batch pour traçabilité
├─ extraction_timestamp (timestamp)         # Quand extrait
├─ 
├─ --- Dimensions
├─ airline_icao (string, FK → dim_airlines)
├─ origin_airport_iata (string, FK → dim_airports)
├─ destination_airport_iata (string, FK → dim_airports)
├─ aircraft_code (string, FK → dim_aircraft_models)
├─ 
├─ --- Mesures (état du vol)
├─ callsign (string)
├─ flight_number (string)
├─ registration (string)
├─ latitude (double)
├─ longitude (double)
├─ altitude_feet (double)
├─ ground_speed_knots (double)
├─ heading_degrees (double)
├─ vertical_speed_fpm (double)
├─ on_ground (int)                         # 0 ou 1
├─ 
├─ --- Données calculées
├─ distance_nm (double)                    # Distance origin → destination (haversine)
├─ data_quality_flags (string)             # "MISSING_DESTINATION,INCOMPLETE_DETAILS"
└─ is_valid (boolean)                      # True si on_ground=0, route complète, codes valides
```

**Partitionnement :** `tech_year/tech_month/tech_day/tech_hour`

---

### 1.3.3 Couche Gold : Tables agrégées pour les KPIs

#### Table `kpi_airline_volumes`

```
kpi_airline_volumes (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)                          # 0-23
├─ airline_icao (string)
├─ airline_name (string)
├─ active_flights_count (int)              # on_ground=0
├─ rank (int)                              # Rang par volume
└─ computed_at (timestamp)
```

#### Table `kpi_continental_regional`

```
kpi_continental_regional (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ continent_code (string)
├─ airline_icao (string)
├─ airline_name (string)
├─ regional_flights_count (int)            # continent_orig = continent_dest
├─ rank (int)
└─ computed_at (timestamp)
```

#### Table `kpi_longest_flights`

```
kpi_longest_flights (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ flight_id (string)
├─ callsign (string)
├─ airline_name (string)
├─ origin_airport_name (string)
├─ destination_airport_name (string)
├─ distance_nm (double)
├─ current_latitude (double)               # Position actuelle
├─ current_longitude (double)
├─ current_altitude_feet (double)
└─ computed_at (timestamp)
```

#### Table `kpi_continental_avg_distance`

```
kpi_continental_avg_distance (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ continent_code (string)
├─ continent_name (string)
├─ avg_distance_nm (double)
├─ min_distance_nm (double)
├─ max_distance_nm (double)
├─ flight_count (int)
└─ computed_at (timestamp)
```

#### Table `kpi_aircraft_manufacturers`

```
kpi_aircraft_manufacturers (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ manufacturer (string)                   # "Boeing", "Airbus", etc.
├─ active_flights_count (int)              # on_ground=0
├─ rank (int)
└─ computed_at (timestamp)
```

#### Table `kpi_airline_aircraft_models`

```
kpi_airline_aircraft_models (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ airline_icao (string)
├─ airline_country_code (string)
├─ airline_name (string)
├─ rank (int)                              # 1, 2, 3 (top 3)
├─ aircraft_code (string)
├─ aircraft_model (string)
├─ usage_count (int)                       # Nombre de vols
└─ computed_at (timestamp)
```

#### Table `kpi_airport_imbalance` (Bonus)

```
kpi_airport_imbalance (Parquet)
├─ kpi_date (date)
├─ kpi_hour (int)
├─ airport_iata (string)
├─ airport_name (string)
├─ country_name (string)
├─ outgoing_flights (int)                  # Vol partant
├─ incoming_flights (int)                  # Vol arrivant
├─ imbalance (int)                         # outgoing - incoming
├─ imbalance_abs (int)                     # Valeur absolue
└─ computed_at (timestamp)
```

---

## 1.4 Transformations clés (Bronze → Silver → Gold)

### Bronze → Silver

1. **Déduplication :** Certains vols peuvent être capturés deux fois dans un batch → garder le plus récent
2. **Nettoyage :**
   - Rejeter les vols sans `airline_icao` OU sans route complète (origin + destination) → marquer en `data_quality_flags`
   - Filtrer `on_ground=0` pour les indicateurs "vols en cours"
3. **Enrichissement :**
   - Mapper `country_code` → `continent_code` via la table `dim_countries_continents`
   - Extraire `manufacturer` depuis `aircraft_code` (B/A/E pour Boeing/Airbus/Embraer) ou lookup table
   - Calculer distance haversine (origin lat/lon → destination lat/lon)
4. **Jointures :**
   - Joindre `fact_flights` avec `dim_airlines`, `dim_airports`, `dim_aircraft_models`
   - Propager les noms complets pour analyse directe

### Silver → Gold

1. **Agrégations** : Group by `airline_icao`, `continent_code`, `aircraft_model`, etc.
2. **Ranking** : Appliquer dense_rank() pour les top-N
3. **Calculs** : avg/min/max distance, counts, imbalances
4. **Timestamping** : Ajouter `computed_at` pour traçabilité

---

## 1.5 Justifications des choix

### Pourquoi Parquet ?

- **Comprimé natif** → réduit le coût de stockage (~5x vs CSV)
- **Schéma** → garantit la cohérence
- **Columnar** → agrégations rapides (ne lit que les colonnes utilisées)
- **Compatible Spark & pandas** → flexibilité downstream

### Pourquoi partitionnement temporel (année/mois/jour/heure) ?

- **Spec kata** : `tech_year=2023/tech_month=2023-07/tech_day=2023-07-16`
- **Prune** : Les requêtes analytiques filtrent souvent par date → partition pruning économise I/O
- **Retention** : Facile de supprimer les anciennes données (supprimer les dossiers jour/heure)
- **Ordre chrono** : Aligné avec la collecte temps-réel (batch toutes les 2h)

### Pourquoi une couche Silver distinct ?

- **Séparation concerns** : Bronze = "on range tout", Silver = "on garantit la qualité"
- **Réutilisabilité** : Plusieurs pipelines Gold peuvent consommer Silver sans refaire le nettoyage
- **Audit trail** : Les erreurs de transformation en Gold ne cassent pas Silver

### Continent vs. Pays ?

- L'API retourne le **pays**, pas le continent.
- Le kata demande des agrégations par **continent** → table de lookup `dim_countries_continents` obligatoire
- Gestion du cas "aéroport multi-pays" (ex: Bâle, aéroport trinational) → choisir le pays dominant ou utiliser une logique métier

---

## 1.6 Validation du modèle contre les KPIs

| KPI | Tables nécessaires | Champs clés |
|-----|-------------------|-------------|
| Compagnie +vols en cours | `fact_flights`, `dim_airlines` | `airline_icao`, `on_ground` |
| Compagnie +régionale/continent | `fact_flights`, `dim_airports`, `dim_countries_continents` | `airline_icao`, `continent_orig`, `continent_dest` |
| Vol trajet +long | `fact_flights`, `dim_aircraft_models` | `distance_nm` (calculé) |
| Avg distance/continent | `fact_flights`, `dim_countries_continents` | `distance_nm`, `continent_code` |
| Constructeur +actif | `fact_flights`, `dim_aircraft_models` | `manufacturer`, `on_ground` |
| Top 3 modèles/pays compagnie | `dim_airlines`, `fact_flights`, `dim_aircraft_models` | `airline_icao`, `country_code` (via airline), `aircraft_code` |
| Bonus: aéroport imbalance | `fact_flights`, `dim_airports` | `origin_iata`, `destination_iata` |

✅ **Tous les KPIs peuvent être résolus avec le modèle proposé.**

---

## 1.7 Fichiers à implémenter

- `schemas.py` : Classes Spark StructType pour chaque table (Bronze, Silver, Gold)
- `data_quality.py` : Fonctions de validation et de flagging
- `transformations.py` : Fonctions de nettoyage, enrichissement, calcul de métriques
- `README_modele.md` : Justifications détaillées (pour le client)

---

**Status :** ✅ Modélisation complète et validée

### Fichiers implémentés pour l'étape 1

1. **`src/schemas.py`** (350 lignes)
   - Définitions complètes des StructType Spark pour toutes les tables
   - 5 tables Bronze, 5 tables Silver, 7 tables Gold
   - Dictionnaire de référence `SCHEMAS` pour accès facile en runtime

2. **`src/data_quality.py`** (180 lignes)
   - `validate_and_flag_flights()` : 8 catégories de flags (missing, invalid, inconsistent)
   - `check_missing_enrichment()` : flags pour données enrichies manquantes
   - `profile_data_quality()` : profil complet + logging automatique

3. **`README_modele.md`** (400 lignes)
   - Justifications complètes des choix (Spark, Parquet, Medallion, timing)
   - Star schema avec mappages KPI
   - Stratégie de rétention, scalabilité, résilience

**Prochaine étape :** Structure du datalake (Étape 2)

---

# Étape 2 : Structure du datalake

## 2.1 Objectif

Créer l'arborescence du datalake avec :
- Structure de répertoires (bronze/, silver/, gold/)
- Configuration des chemins et de la rétention
- Script d'initialisation (création répertoires, tables vides)
- Fonction de nettoyage/purge (suppression anciennes partitions)

## 2.2 Arborescence proposée

```
datalake/
├── bronze/                           # Données brutes de l'API
│   ├── flights_raw/                  # Table principale
│   │   ├── tech_year=2026/
│   │   │   ├── tech_month=2026-06/
│   │   │   │   ├── tech_day=2026-06-21/
│   │   │   │   │   ├── tech_hour=14/
│   │   │   │   │   │   ├── flights_20260621_140000_batch001.parquet
│   │   │   │   │   │   └── flights_20260621_140200_batch002.parquet
│   │   │   │   │   └── tech_hour=16/
│   │   │   │   │       └── flights_20260621_160000_batch003.parquet
│   │   │   │   └── tech_day=2026-06-22/
│   │   │   │       └── ...
│   │   │   └── tech_month=2026-07/
│   │   │       └── ...
│   │   └── tech_year=2025/
│   │       └── ...
│   └── _logs/                       # Logs d'exécution (par batch)
│       └── flights_streaming/
│
├── silver/                           # Données nettoyées et enrichies
│   ├── fact_flights/
│   │   ├── tech_year=2026/
│   │   │   └── tech_month=2026-06/
│   │   │       └── tech_day=2026-06-21/
│   │   │           └── tech_hour=14/
│   │   │               └── fact_flights_2026-06-21-14.parquet
│   │   └── ...
│   ├── dim_airlines/
│   │   └── _current/                 # SCD Type 2 ou snapshot (pas partitionné)
│   │       └── airlines_20260621.parquet
│   ├── dim_airports/
│   │   └── _current/
│   │       └── airports_20260621.parquet
│   ├── dim_aircraft_models/
│   │   └── _current/
│   │       └── aircraft_20260621.parquet
│   ├── dim_countries_continents/
│   │   └── _current/
│   │       └── countries_20260621.parquet
│   └── _quality_logs/                # Rapports de qualité (optionnel)
│       └── 2026-06-21/
│           └── quality_profile_14.json
│
└── gold/                             # Tables agrégées et KPIs
    ├── kpi_airline_volumes/
    │   ├── kpi_date=2026-06-21/
    │   │   ├── kpi_hour=14/
    │   │   │   └── kpi_airline_volumes_2026-06-21-14.parquet
    │   │   └── kpi_hour=16/
    │   │       └── kpi_airline_volumes_2026-06-21-16.parquet
    │   └── ...
    ├── kpi_continental_regional/
    │   └── ...
    ├── kpi_longest_flights/
    │   └── ...
    ├── kpi_continental_avg_distance/
    │   └── ...
    ├── kpi_aircraft_manufacturers/
    │   └── ...
    ├── kpi_airline_aircraft_models/
    │   └── ...
    ├── kpi_airport_imbalance/
    │   └── ...
    └── _metadata/                    # Metadata (schémas, timestamps computation)
        └── last_run_2026-06-21_16.json
```

## 2.3 Configuration et paramètres

**Fichier `config/datalake_config.py`** :

```python
import os
from pathlib import Path

class DatalakeConfig:
    """Configuration centralisée du datalake."""

    # Racine du datalake
    DATALAKE_ROOT = os.getenv("DATALAKE_ROOT", "/mnt/datalake")

    # Couches
    BRONZE_PATH = f"{DATALAKE_ROOT}/bronze"
    SILVER_PATH = f"{DATALAKE_ROOT}/silver"
    GOLD_PATH = f"{DATALAKE_ROOT}/gold"

    # Retention (jours)
    BRONZE_RETENTION_DAYS = 30
    SILVER_RETENTION_DAYS = 60
    GOLD_RETENTION_DAYS = 365

    # Batch et timing
    BATCH_INTERVAL_HOURS = 2
    COLLECTION_TIMEOUT_MINUTES = 30

    # Spark
    SPARK_APP_NAME = "flight-radar-pipeline"
    SPARK_SHUFFLE_PARTITIONS = 200

    # Logging
    LOG_PATH = f"{DATALAKE_ROOT}/_logs"
    LOG_LEVEL = "INFO"

    @classmethod
    def get_bronze_flights_path(cls):
        return f"{cls.BRONZE_PATH}/flights_raw"

    @classmethod
    def get_silver_fact_flights_path(cls):
        return f"{cls.SILVER_PATH}/fact_flights"

    @classmethod
    def get_silver_dim_path(cls, dim_name):
        return f"{cls.SILVER_PATH}/{dim_name}/_current"

    @classmethod
    def get_gold_kpi_path(cls, kpi_name):
        return f"{cls.GOLD_PATH}/{kpi_name}"

    # ... autres méthodes
```

## 2.4 Fichiers d'implémentation

### Fichiers implémentés pour l'étape 2

1. **`config/datalake_config.py`** (260 lignes)
   - Classe `DatalakeConfig` centralisée
   - Tous les chemins, rétention, paramètres Spark en un seul endroit
   - Méthodes pour accéder aux chemins (Bronze, Silver, Gold, logs)
   - Validation et logging de la configuration

2. **`src/datalake_utils.py`** (250 lignes)
   - `get_partition_values()` : extraire year/month/day/hour d'un timestamp
   - `build_partition_path()` : construire chemins partitionnés (Bronze/Silver)
   - `build_partition_path_gold_kpi()` : chemins Gold (kpi_date/kpi_hour)
   - `parse_partition_path()` : parser un chemin pour retrouver table + partitions
   - `cleanup_old_partitions()` : supprimer partitions anciennes (avec dry-run)
   - `list_partitions()` : lister toutes les partitions d'une table
   - `estimate_storage_usage()` : profil du stockage par couche

3. **`scripts/init_datalake.py`** (300 lignes)
   - Initialisation complète : crée tous les répertoires et sous-structures
   - Crée des partitions exemples avec `_SUCCESS` markers
   - Écrit documentation des schémas (markdown par table)
   - Génère fichier `.env.example` et log d'initialisation
   - Entièrement idempotent (safe d'exécuter plusieurs fois)

4. **`scripts/purge_old_partitions.py`** (280 lignes)
   - Purge partitions selon rétention configurée
   - Modes : dry-run (défaut), ou --execute pour vraie suppression
   - Affiche résumé partitions + calendrier rétention
   - Estime espace libéré
   - ATTENTION : destructif (design avec safeguards)

**Status :** ✅ Structure du datalake complète et testée

**Prochaine étape :** POC Spark Batch (Étape 3)

---

# Étape 3 : POC Spark Batch

## 3.1 Objectif

Créer un premier pipeline Spark Core batch qui :
- Collecte des données de l'API FlightRadarAPI par batch (2h)
- Les stocke en Bronze brutes
- Démontre que la collecte, partitionnement et write fonctionnent
- Fournit les bases pour les étapes de transformation (Silver/Gold)

## 3.2 Architecture du streaming

**Pattern :** Micro-batch toutes les 2h

```
API FlightRadarAPI
       |
       v
[ExtractFlights]  (2h interval)
       |
       v
[LoadBronze]      (write Parquet partitionné)
       |
       v
datalake/bronze/flights_raw/tech_year=.../...
```

### Fichiers implémentés pour l'étape 3

1. **`src/flight_extraction.py`** (220 lignes)
   - Classe `FlightExtractor` encapsule l'API FlightRadarAPI
   - `get_flights_for_zone()` : collecte par zone avec gestion d'erreurs
   - `flights_to_dicts()` : conversion Flight objects → dicts plats
   - `flights_to_spark_df()` : création DataFrame Spark avec schéma forcé
   - `collect_and_convert()` : orchestration multi-zone + union
   - `extract_flights_batch()` : fonction de haut niveau pour le job

2. **`src/batch_job.py`** (320 lignes)
   - `create_spark_session()` : configuration Spark complète
   - `run_batch()` : orchestration complète d'un batch (extract → validate → load)
   - Phases : Extraction → Validation → Profilage → Partitionnement → Chargement Bronze
   - Gestion d'erreurs fault-tolerant (logs, pas d'arrêt du job)
   - Sauvegarde des rapports de qualité JSON
   - CLI avec args: `--zones`, `--dry-run`, `--single-batch`, `--verbose`

3. **`README_quickstart.md`** (270 lignes)
   - Guide complet pour démarrer le POC
   - Instructions étape-par-étape : init → run batch → vérifier données
   - Troubleshooting courant
   - Architecture visuelle du flux

**Status :** ✅ POC Spark Batch opérationnel

## Test du POC

Le job peut être testé immédiatement :

```bash
# 1. Initialiser le datalake
python scripts/init_datalake.py

# 2. Lancer le POC (1 batch)
python src/streaming_job.py --single-batch --verbose

# 3. Vérifier les données écrites
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Check').getOrCreate()
df = spark.read.parquet('datalake/bronze/flights_raw')
print(f'Flights: {df.count()}')
df.select('callsign', 'is_valid').show(5)
"
```

---

# Étape 3.5 : Test-Based Development

## 3.5.1 Objectif

Mettre en place une suite de tests **équilibrée** (unit + integration + E2E) avant de continuer avec les Étapes 4-9. Approche : test-based development (TBD) sans over-engineering. ~28 tests, couvrant les chemins critiques.

## 3.5.2 Philosophie et scope

**Principe :** Tester juste ce qu'il faut pour être sûr que tout fonctionne, sans excès.

**Couverture :**
- **Unit tests (~20)** : Composants individuels (schemas, data_quality, flight_extraction, datalake_utils)
- **Integration tests (~5)** : Workflows batch (extract → validate → load avec mock API)
- **E2E tests (~3)** : Cycle complet avec l'API réelle (marqués @slow, optionnels)
- **Total : ~28 tests** (léger, manageable, rapide < 5 min)

## 3.5.3 Structure des tests

```
tests/
├── conftest.py                      # Fixtures partagées
├── unit/
│   ├── test_schemas.py              # ~8 tests : validation schémas Spark
│   ├── test_data_quality.py         # ~8 tests : flags + is_valid logic
│   ├── test_flight_extraction.py    # ~3 tests : extraction + mock API
│   └── test_datalake_utils.py       # ~4 tests : partitionnement + cleanup
├── integration/
│   └── test_batch_job.py            # ~5 tests : workflows batch complets
└── e2e/
    └── test_e2e_batch.py            # ~3 tests : full cycle (API réelle)
```

### Fichiers de configuration et documentation

- **`pytest.ini`** : Configuration pytest (markers, logging, output)
- **`TESTS.md`** : Documentation complète de la suite
- **`TESTING_PLAN.md`** : Plan détaillé des tests
- **`QUICKTEST.md`** : Quick start guide
- **`run_tests.ps1`** / **`run_tests.sh`** : Scripts lanceurs (Windows/Linux)

## 3.5.4 Unit Tests

### `test_schemas.py` (~8 tests)

**Objectif :** Valider que les schémas Spark sont corrects

```python
# Tests
- test_schema_exists() : StructType existe et n'est pas None
- test_schema_has_required_columns() : colonnes obligatoires présentes
- test_schema_field_types() : types des colonnes corrects
- test_schema_can_create_dataframe() : création DataFrame possible
- test_airlines_schema_exists() : dim_airlines OK
- test_airports_schema_exists() : dim_airports OK
- test_fact_flights_schema_exists() : fact_flights OK
- test_schemas_dict_populated() : SCHEMAS registry rempli
```

### `test_data_quality.py` (~8 tests)

**Objectif :** Valider la logique de flagging et is_valid

```python
# Tests
- test_valid_flight_no_flags() : vol valide → aucun flag
- test_missing_origin_flag() : vol sans origin → flag MISSING_ORIGIN
- test_invalid_altitude_flag() : altitude négative → flag INVALID_ALTITUDE
- test_is_valid_logic() : is_valid = True ssi aucun flag
- test_profile_returns_dict() : profil retourne dict valide
- test_profile_counts_valid_invalid() : profil compte vols valid/invalid
```

### `test_flight_extraction.py` (~3 tests)

**Objectif :** Valider extraction et conversion (avec API mockée)

```python
# Tests
- test_extractor_initialization() : extracteur s'initialise correctement
- test_flights_to_dicts_conversion() : Flight objects → dicts
- test_extract_flights_batch_with_mock_api() : création DataFrame OK
- test_empty_flights_list() : gestion liste vide
```

### `test_datalake_utils.py` (~4 tests)

**Objectif :** Valider partitionnement et cleanup

```python
# Tests
- test_get_partition_values() : extraction valeurs partition OK
- test_partition_values_format() : format correct (year, month, day, hour)
- test_build_partition_path() : construction chemin partitionné OK
- test_parse_partition_path() : parsing chemin OK
- test_cleanup_dry_run() : dry-run ne supprime pas
```

## 3.5.5 Integration Tests

### `test_batch_job.py` (~5 tests)

**Objectif :** Valider le workflow batch complet

```python
# Tests
- test_spark_session_creation() : session Spark créée correctement
- test_batch_job_with_mock_api() : extraction → validation → load OK
- test_batch_job_empty_api_response() : API vide → graceful handling
- test_batch_job_logging() : logs créés correctement
- test_batch_continues_on_partial_failure() : données mixtes → fault-tolerant

# Validations
- Batch doit terminer sans crash (success = True/False)
- Bronze Parquet doit être écrit si succès
- Logs + rapports JSON créés
```

## 3.5.6 E2E Tests

### `test_e2e_batch.py` (~3 tests, @slow)

**Objectif :** Valider le cycle complet avec API réelle

```python
# Tests
- test_batch_job_full_cycle() : API réelle → Spark → Parquet
- test_batch_job_creates_quality_reports() : rapports de qualité générés
- test_batch_job_idempotent() : batch rejouable 2x = même résultat

# Marqué @pytest.mark.slow (à exécuter séparément)
# Nécessite accès Internet + API FlightRadarAPI
```

## 3.5.7 Fixtures partagées (conftest.py)

```python
@pytest.fixture(scope="session")
def spark_session():
    """Session Spark pour tous les tests."""
    # Local[2], 1g driver memory
    # Configurée pour tests

@pytest.fixture(scope="function")
def temp_datalake(tmp_path):
    """Datalake temporaire par test."""
    # Bronze, Silver, Gold dirs créés
    # Isolé par test (cleanup auto)

@pytest.fixture
def sample_flight_dict():
    """Vol valide (dictionnaire)."""
    # CDG → ORY, altitude 10000 ft, etc.

@pytest.fixture
def sample_flight_dict_invalid():
    """Vol invalide (données manquantes)."""
    # Manque airline, origin ; altitude négatif

@pytest.fixture
def sample_flights_dataframe(spark_session, sample_flight_dict):
    """DataFrame Spark avec 1 vol."""
```

## 3.5.8 Exécution

### Option 1 : Scripts lanceurs

```powershell
# Windows PowerShell
.\run_tests.ps1              # Unit + Integration (rapide, ~2 min)
.\run_tests.ps1 -Mode unit   # Unit only (~30 sec)
.\run_tests.ps1 -Mode e2e    # With API (slow, ~10 min)
.\run_tests.ps1 -Mode coverage # Rapport coverage
```

```bash
# Linux/Mac
bash run_tests.sh             # Default
bash run_tests.sh unit        # Unit only
bash run_tests.sh e2e         # With API
```

### Option 2 : pytest directe

```bash
# Default (unit + integration, pas slow)
pytest tests/ -v -m "not slow"

# Unit only
pytest tests/unit/ -v

# Integration
pytest tests/integration/ -v

# E2E (avec API)
pytest tests/e2e/ -v -m slow

# All
pytest tests/ -v

# Coverage
pytest tests/unit/ --cov=src --cov-report=html
```

## 3.5.9 Résultats attendus

Après exécution : `pytest tests/ -v -m "not slow"`

```
tests/unit/test_schemas.py::TestFlightsRawSchema::test_schema_exists PASSED
tests/unit/test_schemas.py::TestFlightsRawSchema::test_schema_has_required_columns PASSED
tests/unit/test_data_quality.py::TestDataQualityFlags::test_valid_flight_no_flags PASSED
...
tests/integration/test_batch_job.py::TestBatchJobIntegration::test_spark_session_creation PASSED
...

========================= 25 passed, 3 skipped in 2.15s =========================
```

**Résultat attendu :**
- ✅ 25 PASSED (unit + integration)
- ⏭️ 3 SKIPPED (E2E marked @slow)
- ❌ 0 FAILED

## 3.5.10 Artefacts livrés

**Fichiers créés :**
- `tests/` (3 sous-dossiers, 7 fichiers test, ~1000 lignes code test)
- `pytest.ini` (configuration)
- `TESTS.md` (documentation complète)
- `TESTING_PLAN.md` (plan détaillé)
- `QUICKTEST.md` (quick start)
- `run_tests.ps1` / `run_tests.sh` (lanceurs)

**Dependencies :**
- pytest==7.4.0
- pytest-mock==3.12.0
- pyspark==3.5.0+ (pour Spark local sessions)

**Status :** ✅ Suite de tests en place, équilibrée, prête pour validation

---

**Prochaines étapes :** 

- **Étape 4** : POC Transformation & KPIs (Silver + Gold layer)
- **Étape 5** : Stratégie de partitionnement (optimisation)
- **Étape 6** : Logging & Monitoring
- **Étape 7** : Job Spark final (boucle infinie vs. cron)
- **Étape 8** : Dashboard Streamlit
- **Étape 9** : Gestion des erreurs (fault-tolerance "loud")

---

## Résumé global (Étapes 1-3.5 complétées)

| Étape | Titre | Fichiers | Status |
|-------|-------|----------|--------|
| 1 | Modélisation des données | `src/schemas.py`, `src/data_quality.py`, `README_modele.md` | ✅ |
| 2 | Structure du datalake | `config/datalake_config.py`, `src/datalake_utils.py`, `scripts/init_datalake.py`, `scripts/purge_old_partitions.py` | ✅ |
| 3 | POC Spark Batch | `src/flight_extraction.py`, `src/batch_job.py`, `README_quickstart.md` | ✅ |
| 3.5 | Test-Based Development | `tests/` (~7 fichiers), `pytest.ini`, `TESTS.md`, `TESTING_PLAN.md`, `run_tests.ps1/sh` | ✅ |
| 4 | POC Transformation & KPIs | À implémenter | 🔲 |
| 5 | Stratégie de partitionnement | À implémenter | 🔲 |
| 6 | Logging & Monitoring | À implémenter | 🔲 |
| 7 | Job Spark final | À implémenter | 🔲 |
| 8 | Dashboard Streamlit | À implémenter | 🔲 |
| 9 | Fault-tolerance & gestion erreurs | À implémenter | 🔲 |

**Artefacts clés livrés :**
- ✅ Modèle de données complet (star schema avec fact + 4 dims + 7 KPIs)
- ✅ Configuration centralisée (DatalakeConfig)
- ✅ Scripts d'admin (init, purge) opérationnels
- ✅ POC fonctionnel (extract → Bronze)
- ✅ Documentation client (README_modele, README_quickstart)
- ✅ Schémas Spark + data quality checks
- ✅ Suite de tests équilibrée (~28 tests : unit + integration + E2E)
- ✅ Journal développement complet (documentation_dev.md)

**Prochaine priorité :** Étape 4 (Transformation Silver + KPIs Gold)

---

# Conclusion — Statut et prochaines étapes

## Résumé de ce qui a été livré (Étapes 1-3.5)

### Code et implémentation

**Fichiers principaux (Étapes 1-3) :**
- `src/schemas.py` (350 lignes) : Schémas Spark pour 12 tables
- `src/data_quality.py` (180 lignes) : Validation + flagging qualité
- `config/datalake_config.py` (260 lignes) : Configuration centralisée
- `src/datalake_utils.py` (250 lignes) : Utilitaires partitionnement + cleanup
- `scripts/init_datalake.py` (300 lignes) : Initialisation datalake
- `scripts/purge_old_partitions.py` (280 lignes) : Purge par rétention
- `src/flight_extraction.py` (220 lignes) : Extraction API
- `src/batch_job.py` (320 lignes) : Job Spark Core Batch principal

**Fichiers tests (Étape 3.5) :**
- `tests/conftest.py` (~100 lignes) : Fixtures partagées
- `tests/unit/test_schemas.py` (~120 lignes) : 8 tests schémas
- `tests/unit/test_data_quality.py` (~130 lignes) : 8 tests qualité
- `tests/unit/test_flight_extraction.py` (~100 lignes) : 3 tests extraction
- `tests/unit/test_datalake_utils.py` (~120 lignes) : 4 tests utils
- `tests/integration/test_batch_job.py` (~130 lignes) : 5 tests batch
- `tests/e2e/test_e2e_batch.py` (~80 lignes) : 3 tests E2E

**Total : ~2100 lignes de code production + ~750 lignes de tests**

### Documentation

**Principale :**
- `README_modele.md` (400 lignes) : Justifications modèle + mappages KPI
- `README_quickstart.md` (270 lignes) : Guide démarrage rapide
- `documentation_dev.md` (ce fichier, ~1100 lignes) : Journal complet development
- `README.md` (320 lignes) : Vue d'ensemble projet
- Inline docstrings : tous les modules documentés

**Tests (Étape 3.5) :**
- `TESTS.md` (~300 lignes) : Documentation complète de la suite
- `TESTING_PLAN.md` (~400 lignes) : Plan détaillé avec exemples
- `QUICKTEST.md` (~100 lignes) : Quick start tests
- `pytest.ini` : Configuration pytest
- `run_tests.ps1` / `run_tests.sh` : Scripts lanceurs

### Artefacts client

1. **Modèle de données complet** ✅
   - Star schema validé contre tous les KPIs
   - 1 table fact + 4 dimensions + 7 KPIs
   - 8 flags de qualité + `is_valid` boolean

2. **Infrastructure as Code** ✅
   - Configuration centralisée (source unique de vérité)
   - Scripts d'initialisation idempotents
   - Scripts de nettoyage avec dry-run

3. **POC opérationnel** ✅
   - Cycle complet testé (API → Spark → Parquet)
   - Validation + profil de qualité
   - Logs structurés + rapports JSON

4. **Documentation française** ✅
   - 3 README différents (modèle, quickstart, global)
   - Journal développement complet
   - Justifications de chaque choix

---

## Installation et premier test

### POC (Étapes 1-3)

```bash
# 1. Cloner le repository et cd
cd /path/to/test_tecnico_exalt

# 2. Installer dépendances
pip install -r requirements.txt

# 3. Initialiser le datalake
python scripts/init_datalake.py --verbose

# 4. Exécuter le POC
python src/batch_job.py --single-batch --verbose

# 5. Vérifier les données
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('datalake/bronze/flights_raw')
print(f'Flights: {df.count()}')
print(f'Valid: {df.filter(\"is_valid=True\").count()}')
"
```

**Durée estimée :** ~5 minutes (2-3 min API + 1-2 min Spark + logs)

### Tests (Étape 3.5)

```bash
# Option 1 : Scripts lanceurs
.\run_tests.ps1              # Windows : unit + integration (~2 min)
bash run_tests.sh            # Linux/Mac : unit + integration (~2 min)

# Option 2 : pytest directe
pytest tests/ -v -m "not slow"  # Unit + Integration (rapide)
pytest tests/unit/ -v           # Unit only (~30 sec)
pytest tests/e2e/ -v -m slow    # E2E avec API (lent, ~10 min)

# Avec coverage report
pytest tests/unit/ --cov=src --cov-report=html
```

**Durée estimée :** ~2-5 minutes (selon mode)

---

## Roadmap — Ce qui reste à faire

### Étape 4 : Transformation Silver + KPIs (2-3 jours)

**Fichiers à créer :**
- `src/transformations.py` : Fonctions de cleaning (normalisation, dedup, enrichissement)
- `src/silver_loader.py` : Load Silver (fact + dims) depuis Bronze
- `src/gold_loader.py` : Load Gold (7 KPIs) depuis Silver
- `src/continent_mapping.py` : Table lookup pays → continent (JSON ou CSV)

**Étapes :**
1. Parser Bronze, faire les joins (fact + dims)
2. Calculer distance haversine, continent, etc.
3. Créer les 7 tables Gold avec agrégations
4. Valider chaque KPI contre données manuelles

**Validation :** Comparer les résultats avec les données du notebook d'exploration

### Étape 5 : Optimisation + Partitioning (1 jour)

**Analyse :**
- Profiler les requêtes Gold les plus lentes
- Analyser les patterns d'accès (quels KPIs, quels filtres)
- Ajouter des indices ou partitions secondaires si besoin

**Tuning Spark :**
- Ajuster `SPARK_SHUFFLE_PARTITIONS`
- Évaluer bucketing vs. partitioning

### Étape 6 : Logging & Monitoring (1-2 jours)

**Implémentation :**
- Ajouter logs structurés (JSON format)
- Métriques Prometheus (counts, latencies, error rates)
- Dashboard Grafana pour le monitoring interne

**Alertes :**
- Si % valid < 70% → alerte
- Si latence batch > 30 min → alerte
- Si API timeout > 5% → alerte

### Étape 7 : Job final + Scheduling (1 jour)

**Choix :**
- Boucle infinie Spark + cron (recommandé pour cette taille)
- Ou Spark Streaming continu (si infrastructure disponible)

**Checkpoint management :**
- Spark checkpoints pour fault recovery
- Versioning des states

### Étape 8 : Dashboard Streamlit (2 jours)

**Interface :**
- 7 visualisations (une par KPI)
- Filtres : date range, compagnie, continent, etc.
- Tables détaillées + graphiques
- Auto-refresh toutes les 5 min

**Déploiement :**
- Streamlit sur port 8501
- Accès local (phase 2 : AWS Streamlit Community Cloud)

### Étape 9 : Fault-tolerance & gestion erreurs (1 jour)

**Policy :**
- Chaque erreur → flag explicite dans les données
- Logs détaillés + traces complètes
- Pas d'arrêt du job pour corner cases

**Gestion :**
- Timeout API → retry 3x + abandon gracieux
- Données corrompues → marquer `is_valid=False` + flag
- Job crash → logs+ notification

### Phase 3 : Déploiement (2-4 semaines)

**Airflow orchestration (optionnel) :** Scheduler les jobs
**AWS :** S3 datalake, EC2/ECS workers, RDS metadata, Athena queries
**Monitoring cloud :** CloudWatch logs, CloudTrail audit

---

## Points de décision restants

1. **Enrichissement API ?**
   - POC : `enrich=False` (pas d'appel `get_flight_details`)
   - Étape 4 : `enrich=True` OU enrichissement asynchrone après (coûteux)
   - Impact : Silver table va de 200 cols (enrichies) à ~50 cols (brutes)

2. **Continent vs. Pays ?**
   - Décidé : utiliser table `dim_countries_continents` (pays ISO 3166-1 → continent)
   - Mapping statique (pas de maintenance)

3. **Boucle infinie vs. Cron ?**
   - Recommandé : Cron (simpler, observable, logs par batch)
   - Spark Streaming continu : plus complexe, benefits unclear pour 2h intervals

4. **Dashboard interne vs. Externe ?**
   - POC : Streamlit local (accès interne)
   - Phase 2 : Streamlit Community Cloud OU Tableau OU AWS Quicksight

5. **Airflow ?**
   - ONHOLD (non spécifié dans kata)
   - À ajouter si orchestration multi-jobs requis

---

## Checklist pour continuer (Étape 4)

- [ ] Créer `src/transformations.py` avec fonctions de cleaning
- [ ] Implémenter Silver loader (fact_flights + 4 dims)
- [ ] Créer table `dim_countries_continents` (pays → continent)
- [ ] Implémenter Gold loader (7 KPIs)
- [ ] Tester chaque KPI manuellement
- [ ] Créer tests unitaires (pytest)
- [ ] Documenter transformations

---

## Resources utilisées

- **API :** FlightRadarAPI v1.5.1 (pip)
- **Computing :** Apache Spark 3.5.0
- **Storage :** Filesystem local (Parquet)
- **Monitoring :** Logs Python native
- **Documentation :** Markdown

---

## Conclusion

✅ **Étapes 1-3 complétées et testées.**

Le POC est opérationnel et démontre :
1. Que l'API peut être collectée fiablement
2. Que Spark peut traiter et stocker les données
3. Que la qualité peut être validée et flaggée
4. Que l'architecture Medallion 3 couches fonctionne

**Prochaine priorité :** Étape 4 (2-3 jours) pour avoir les KPIs calculés.
**Puis :** Streamlit dashboard (Étape 8) pour montrer les résultats au client.

Le code est production-ready pour le POC (pas à 100% pour production AWS, voir phase 3).
