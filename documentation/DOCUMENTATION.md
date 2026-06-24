# Documentation — Pipeline ETL Trafic Aérien Mondial

> Documentation technique complète et unique du projet. Le README à la racine fournit
> une entrée rapide ; ce fichier détaille tout.

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Modèle de données](#2-modèle-de-données)
3. [Datalake & partitionnement](#3-datalake--partitionnement)
4. [Pipeline ETL](#4-pipeline-etl)
5. [Collecte API & anti-quota](#5-collecte-api--anti-quota)
6. [Exécution & exploitation](#6-exécution--exploitation)
7. [Monitoring & Dashboard](#7-monitoring--dashboard)
8. [Tests](#8-tests)
9. [Fault-tolerance & qualité des données](#9-fault-tolerance--qualité-des-données)
10. [Historique de développement](#10-historique-de-développement)
11. [Statistiques & livrables](#11-statistiques--livrables)
12. [Annexes](#12-annexes)

---

## 1. Vue d'ensemble

Pipeline **ETL batch** qui collecte le trafic aérien mondial via l'API **FlightRadar24**,
le nettoie/enrichit dans une **architecture Medallion** (Bronze → Silver → Gold) avec
**Apache Spark Core (batch)**, calcule **7 KPIs** et les expose dans un **dashboard Streamlit**.
Le job est conçu pour être **orchestré toutes les 2 heures** (Apache **Airflow** — cf. §6 ; ou
cron / Task Scheduler) — ce n'est pas du streaming continu.

### Les 7 KPIs
1. **Compagnie** avec le plus de vols en cours.
2. Par **continent**, compagnie la plus active en **vols régionaux** (continent origine == destination).
3. **Vol en cours** au trajet le **plus long** (distance haversine).
4. Par **continent**, **distance de vol moyenne**.
5. **Constructeur** d'avions avec le plus de vols actifs.
6. Par **pays de compagnie**, **top 3** des modèles d'avion en usage.
7. **(Bonus)** Aéroport au plus grand **écart départs/arrivées**.

> Les **KPI de distance (3 & 4)** ne considèrent que les vols **point-à-point**
> (`origin_iata != destination_iata`) : les vols circulaires (même aéroport départ/arrivée —
> patrouille, entraînement, tour de piste) ont une distance grand-cercle nulle et sont écartés de
> ces agrégats (ils restent comptés dans les autres KPI et en Silver).

### Architecture

```
API FlightRadar24 (temps réel)
        │  job toutes les 2 h (orchestré par Airflow — cf. §6)
        ▼
┌─────────────────────────────┐
│ BRONZE — données brutes      │  flights_raw (Parquet)
│ partition: year/month/day/h  │
└─────────────────────────────┘
        │  nettoyage + enrichissement (jointure dimensions)
        ▼
┌─────────────────────────────┐
│ SILVER — nettoyé & enrichi   │  fact_flights + 4 dimensions
│ partition: year/month/day/h  │  dim_airports / dim_airlines /
└─────────────────────────────┘  dim_aircraft_models / dim_countries_continents
        │  agrégations
        ▼
┌─────────────────────────────┐
│ GOLD — 7 tables KPI           │  kpi_* (Parquet)
│ partition: year/month/day/h  │
└─────────────────────────────┘
        │
        ▼   Dashboard Streamlit + métriques JSON + alerting
```

### Choix technologiques

| Choix | Justification |
|---|---|
| **Spark Core batch** | Scalabilité, fault-tolerance ; job ponctuel toutes les 2 h (pas streaming continu) |
| **Medallion 3 couches** | Traçabilité, découplage, résilience (Bronze rejouable) |
| **Parquet + partition temporelle** | Columnar compressé, schéma fort, partition pruning, rétention facile |
| **Star schema** | Réutilisabilité, audit, séparation fait/dimensions |
| **Orchestration Airflow** | DAG toutes les 2 h via DockerOperator ; pas de boucle interne (cron/Task Scheduler en repli) |
| **Postgres + LocalExecutor** | Évite le verrou SQLite sous la charge scheduler ; déploiement minimal (2 conteneurs) mais fiable |
| **Enrichissement dimensions bulk** | Anti-quota/anti-rate-limit : 1 appel groupé au lieu de N (cf. §5) |
| **Jeu aéroports statique OpenFlights** | Fiabilité, zéro quota API, reproductible ; auto-téléchargé/rafraîchi (TTL configurable) |
| **Idempotence (overwrite + dédup)** | Re-run de la même heure = remplacement, pas d'empilement ; validé par test de double run |
| **Socket Docker via `docker-socket-proxy`** | Airflow non-root, sans montage du socket : proxy filtrant (réseau interne, API minimale) qui neutralise la faille `docker.sock` |

---

## 2. Modèle de données

Star schema : **1 table de faits** (`fact_flights`) + **4 dimensions** + **7 tables KPI** (Gold).
Le schéma Bronze (`flights_raw`) est figé dans `src/schemas.py` (seul schéma **appliqué**) ; les
tables Silver/Gold sont construites dynamiquement (`dimension_loader`, `transformations`).

### Bronze — `flights_raw`
Données brutes de l'API + métadonnées d'extraction. Champs clés : `extraction_timestamp`,
`batch_id`, `source_zone`, `flight_id` (PK), `callsign`, `airline_icao/iata`, `aircraft_code`,
`origin_iata`, `destination_iata`, position (`latitude/longitude/altitude/ground_speed/heading/
on_ground/vertical_speed`), + champs d'enrichissement (remplis en Silver) : pays/coords des
aéroports, `airline_name`, `aircraft_model`.

### Silver — fait + dimensions
- **`fact_flights`** : vols dédupliqués (1 par `flight_id`, le plus récent), enrichis
  (continent, distance haversine, constructeur) + flags qualité + `is_valid`.
- **`dim_airports`** : `airport_iata` (PK), pays (nom + code ISO), continent, lat/lon.
- **`dim_airlines`** : `airline_icao` (PK), iata, nom.
- **`dim_aircraft_models`** : `aircraft_code` (PK), modèle, constructeur.
- **`dim_countries_continents`** : `country_code` (PK), nom, continent.

Les dimensions sont écrites en **snapshot `_current`** (overwrite, non partitionnées) — ce sont
des **référentiels**, pas des données événementielles.

### Gold — 7 tables KPI
`kpi_airline_volumes`, `kpi_continental_regional`, `kpi_longest_flight`,
`kpi_continental_avg_distance`, `kpi_aircraft_manufacturers`, `kpi_airline_aircraft_top3`,
`kpi_airport_imbalance`. Chacune inclut `computed_at` (traçabilité). Logique dans
`src/transformations.py`.

### Qualité — 8 flags + `is_valid`
`validate_and_flag_flights` (`src/data_quality.py`) pose des flags (colonne
`data_quality_flags`) sans jamais échouer :
`MISSING_ORIGIN`, `MISSING_DESTINATION`, `MISSING_AIRLINE`, `MISSING_AIRCRAFT_CODE`,
`MISSING_POSITION`, `INVALID_ALTITUDE` (0–50000 ft), `INVALID_GROUND_SPEED` (0–600 kn),
`INCONSISTENT_POSITION` (lat/lon hors limites). `is_valid = True` ssi vol **en vol**
(`on_ground=0`), origine/destination/compagnie présents et **aucun flag** → utilisable par les KPIs.

### Données de référence (`src/reference_data.py`)
- `COUNTRY_TO_CONTINENT` : code ISO alpha-2 → continent.
- `COUNTRY_NAME_TO_CODE` : nom de pays (FR24/OpenFlights) → code ISO (clé normalisée tolérant
  « United States » / « united-states »).
- `AIRCRAFT_PREFIX_TO_MANUFACTURER` : préfixe ICAO d'avion → constructeur (ex. `A`→Airbus,
  `B`→Boeing, `BCS`→Airbus, `E1/E2`→Embraer…), évalué du préfixe le plus long au plus court.
- Helpers d'expressions Spark : `continent_code_expr`, `country_code_from_name_expr`,
  `manufacturer_expr`.

---

## 3. Datalake & partitionnement

Configuration centralisée dans `config/pipeline_config.py` (`PipelineConfig`). Racine via
`DATALAKE_ROOT` (défaut `./datalake`).

```
datalake/
├── bronze/
│   └── flights_raw/tech_year=2026/tech_month=2026-06/tech_day=2026-06-22/tech_hour=14/*.parquet
├── silver/
│   ├── fact_flights/tech_year=2026/tech_month=2026-06/tech_day=2026-06-22/tech_hour=14/*.parquet
│   ├── dim_airports/_current/*.parquet
│   ├── dim_airlines/_current/*.parquet
│   ├── dim_aircraft_models/_current/*.parquet
│   └── dim_countries_continents/_current/*.parquet
├── gold/
│   └── kpi_*/tech_year=2026/tech_month=2026-06/tech_day=2026-06-23/tech_hour=14/*.parquet
└── _logs/   (logs, métriques JSON, rapports qualité, alertes)
```

### Nomenclature horodatée (requis)
- **Bronze, Silver & Gold** : `tech_year / tech_month / tech_day / tech_hour` (**jusqu'à l'heure**).

Le job tournant 12×/jour, les **données ET les résultats** sont retrouvables pour un couple
**(Date, Heure)** : chaque run écrit sa propre partition horaire. C'est aussi **idempotent** — re-run
de la même heure = overwrite de cette partition (les autres heures sont conservées). Les dimensions
restent en snapshot `_current` (référentiels, non partitionnés).

Constantes : `PARTITION_COLUMNS_BRONZE` / `PARTITION_COLUMNS_SILVER` / `PARTITION_COLUMNS_GOLD`
(les 4 niveaux `tech_*`). L'**inférence de type des partitions est désactivée**
(`spark.sql.sources.partitionColumnTypeInference.enabled=false`) pour que les `tech_*` restent
des **labels string** cohérents entre couches (sinon Spark inférerait `tech_year`→int,
`tech_day`→date).

### Rétention & nettoyage
`BRONZE_RETENTION_DAYS=30`, `SILVER=60`, `GOLD=365`. Nettoyage par
`scripts/purge_old_partitions.py` (dry-run par défaut) qui s'appuie sur
`datalake_utils.cleanup_old_partitions` (os.walk, compatible Python < 3.12).
Le tuning Spark (mémoire, shuffle) se règle via `PipelineConfig`/variables d'env (§6).

---

## 4. Pipeline ETL

Point d'entrée **unique** : `src/batch_job.py::run_batch` (le wrapper CLI `scripts/run_job.py`
ne fait que l'appeler). Phases :

1. **Extraction** (`src/flight_extraction.py`) — collecte des vols sur les **zones**
   configurées (`COLLECTION_ZONES`) avec `bounds`, **dédup cross-zones** par `flight_id`.
2. **Validation & flagging** (`data_quality.validate_and_flag_flights`) — 8 flags + `is_valid`.
3. **Profil qualité + analyse** — `profile_data_quality`, comptage en vol/au sol, cardinalités
   des dimensions ; rapport JSON qualité.
4. **Partitionnement temporel** — colonnes `tech_*` via `get_partition_values`.
5. **Écriture Bronze** (Parquet partitionné).
6. **Silver + Gold** (`src/silver_gold_loader.py`, si `--with-silver-gold`) :
   - chargement des **dimensions** de référence (bulk + cache, cf. §5) ;
   - **enrichissement par jointure** `enrich_with_dimensions` (pays/coords/airline_name) ;
   - `clean_and_enrich_bronze` (dédup, continent, distance, constructeur) → `fact_flights` ;
   - écriture des 4 dimensions ; calcul + écriture des **7 KPIs**.
7. **Métriques + alerting** — `JobMetrics` JSON + évaluation des alertes (cf. §7, §9).

Le job est **fault-tolerant** : toute exception → log + métriques sauvegardées + retour `False`,
sans interrompre brutalement.

---

## 5. Collecte API & anti-quota

Librairie **FlightRadarAPI** (curl_cffi). Extraction dans `src/flight_extraction.py`,
dimensions de référence dans `src/dimension_loader.py`.

### Dépasser le cap de 1500 vols
L'appel global non borné est **plafonné à 1500 vols**. En passant un `bounds` (zone), la limite
de la librairie monte à **5000/appel**. `COLLECTION_ZONES` itère les **9 zones top-level**
(europe, northamerica, southamerica, asia, africa, oceania, atlantic, maldives, northatlantic) →
plusieurs milliers de vols (≈ 7600 observés), dédupliqués par `flight_id`.

### Enrichissement par dimensions (au lieu de `get_flight_details` par vol)
Plutôt que ~1 appel détaillé **par vol** (des milliers d'appels → 429), on charge des
**dimensions de référence** jointes au fait en Spark :
- **`dim_airlines`** : `get_airlines()` — **1 appel** (~2000 compagnies).
- **`dim_airports`** : source configurable via **`DIM_AIRPORTS_SOURCE`** :
  - `static` (**défaut, recommandé**) : jeu **OpenFlights** local `data/airports.dat` (~6000
    aéroports avec IATA/lat/lon/pays) — **zéro appel API, zéro quota**, fiable.
    **Auto-géré** par `dimension_loader.ensure_airports_dataset()` : téléchargé s'il est absent,
    rafraîchi si plus vieux que `DIM_AIRPORTS_MAX_AGE_DAYS` (14 j), sinon rien. URL et TTL
    surchargeables (`DIM_AIRPORTS_STATIC_URL`, `DIM_AIRPORTS_MAX_AGE_DAYS`). En cas d'échec
    réseau, on garde le fichier existant (**fallback hors-ligne**). → aucun jeu figé dans l'image.
  - `api` : `get_airports()` (1 appel/pays, ~228) — idéal en principe mais **lent (~30 min)** et
    **429 en anonyme**.
- **Cache** : les dimensions sont relues du cache Silver si plus récentes que
  `DIM_CACHE_MAX_AGE_DAYS` (7 j) → 0 appel sur les runs suivants.

`enrich_with_dimensions` (`src/transformations.py`) remplit pays/coords (origine+destination) et
`airline_name` ; `clean_and_enrich_bronze` dérive ensuite continent/distance/constructeur.

### Résilience au rate-limit (HTTP 429)
- **Login** (`FR24_EMAIL`/`FR24_PASSWORD`) → quota plus élevé. ⚠️ Un compte FR24 créé « via
  Google » doit avoir **un mot de passe** défini pour que `login` fonctionne ; sinon le pipeline
  continue en **anonyme**.
- **`RetryPolicy`** (backoff exponentiel) passée au constructeur → réessaie les erreurs
  transitoires (dont 429) sur tous les appels internes.
- **`max_workers=3`** : concurrence réduite.
- Garde externe `retry_with_backoff` sur l'appel feed ; l'enrichissement par vol éventuel est
  **best-effort** (un échec n'annule pas la zone).

---

## 6. Exécution & exploitation

### Exécution avec Docker (recommandé, zéro prérequis)
Une **image unique** (`Dockerfile`) embarque JDK 17 + Python 3.11 + dépendances + code. En
conteneur Linux, **winutils/HADOOP_HOME ne s'appliquent pas** (prérequis Windows uniquement). Le
`docker-compose.yml` définit deux services principaux partageant le volume `flight_datalake` (le
**profil `airflow`** ajoute `postgres` + `airflow`, cf. plus bas) :

| Service | Rôle |
|---|---|
| `etl` | exécute `run_job.py --with-silver-gold` (un batch complet) puis sort |
| `dashboard` | sert Streamlit sur `http://localhost:8501` (toujours up) |

```bash
docker compose up                 # build + lance ETL et dashboard
docker compose run --rm etl       # relancer un batch à la demande
docker compose down               # arrêter (le volume datalake persiste)
docker compose down -v            # arrêter + supprimer les données (run vierge)
```

- **Pas de `.env` requis** ; `cp .env.example .env` pour des identifiants FR24
  (`FR24_EMAIL/PASSWORD`) ou ajuster `SPARK_DRIVER_MEMORY` (allouer ≥ 4 Go à Docker Desktop).
- Le jeu OpenFlights est **téléchargé une fois** sous `datalake/_reference/airports.dat` (volume),
  puis rafraîchi à 14 j (cf. §5) — rien n'est figé dans l'image.
- L'orchestration **Airflow réutilise cette même image** (sous-section dédiée plus bas).

### Prérequis Windows (Spark/Parquet) — exécution native
L'écriture Parquet via Spark exige `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x). **Il suffit de
les placer dans un dossier `bin\`** parmi les emplacements auto-détectés :
`%USERPROFILE%\hadoop\bin` (recommandé), `<projet>\vendor\hadoop\bin`, `C:\hadoop\bin` ou
`C:\winutils\bin`. `create_spark_session` appelle `_ensure_hadoop_home()` qui **trouve winutils
et pose `HADOOP_HOME` automatiquement** au démarrage — plus besoin de définir la variable à la
main dans chaque terminal.

```powershell
# (optionnel) login FR24 pour un quota plus élevé :
$env:FR24_EMAIL="<email>"; $env:FR24_PASSWORD="<mot_de_passe_FR24>"
python scripts\run_job.py --with-silver-gold
```

Si winutils est dans un emplacement non standard, on peut toujours forcer manuellement
`$env:HADOOP_HOME="<chemin>"` (respecté en priorité). `create_spark_session` ajoute aussi
`spark.driver.extraLibraryPath` vers `%HADOOP_HOME%\bin` (chargement de `hadoop.dll`). Sous
Linux, aucun de ces prérequis n'est nécessaire.

### Commandes principales
```bash
python scripts/init_datalake.py              # initialiser l'arborescence
python scripts/run_job.py --with-silver-gold # 1 batch complet (Bronze → Silver → Gold)
python scripts/run_job.py --zones europe asia --verbose
streamlit run dashboard.py                   # dashboard (http://localhost:8501)
python scripts/purge_old_partitions.py --dry-run --verbose
```

### Scheduling (toutes les 2 h)
- **Airflow (recommandé)** : cf. ci-dessous — c'est le mécanisme d'ordonnancement du projet.
- Alternative sans Airflow : un cron (`0 */2 * * *`) ou une tâche planifiée Windows appelant
  `python scripts/run_job.py --with-silver-gold`.

### Orchestration Airflow (`airflow/`, profil Docker)
**Pattern production** : Airflow **orchestre**, il n'exécute pas Spark en interne. Le DAG
`flight_etl_pipeline` déclenche le batch dans un conteneur `flight-etl` **isolé** via
**`DockerOperator`** — équivalent local du `KubernetesPodOperator` visé en prod (l'image Airflow
reste légère : ni Java ni pyspark). Déploiement lean mais fiable : `airflow standalone` +
**Postgres** (LocalExecutor) — 2 conteneurs (SQLite seul verrouille sous la charge scheduler).

```bash
docker compose build
docker compose --profile airflow up -d        # Airflow :8080 + dashboard :8501
docker compose exec airflow airflow dags trigger flight_etl_pipeline   # run à la demande
```

- **DAG** : `schedule="0 */2 * * *"`, `catchup=False`, `max_active_runs=1`, `retries=1`. Deux
  tâches : **`run_etl`** (DockerOperator → `run_job.py --with-silver-gold`, monte le volume
  `flight_datalake`) puis **`check_outcome`** (lit le dernier `*_metrics.json`, échoue si
  `status != success` ou erreurs).
- **Volume partagé `flight_datalake`** entre `etl`, `dashboard`, le conteneur job et `airflow` →
  le **dashboard (:8501)** visualise chaque run planifié (« Statut d'exécution ») et les KPIs.
- Accès au socket Docker **sécurisé** : Airflow ne monte plus le socket brut et tourne en
  utilisateur non-root. Il pilote le `DockerOperator` via un **proxy filtrant**
  (`docker-socket-proxy`, réseau interne, aucun port publié) qui n'expose que les endpoints API
  nécessaires (`CONTAINERS`/`IMAGES`/`POST`) ; le socket n'est monté qu'en lecture seule dans ce
  proxy minimal. En prod K8s ce point disparaît. Détails dans `airflow/dags/flight_etl_dag.py`.
- **Cible prod** : remplacer `DockerOperator` par `KubernetesPodOperator` (même structure de DAG).

### Configuration (`config/pipeline_config.py`, surchargeable par env)
Chemins datalake, rétention, paramètres Spark, API (timeout, `API_MAX_WORKERS_PARALLEL`,
`API_MAX_RETRIES`, `API_RETRY_*`), `API_ENRICH_DETAILS`, `FR24_EMAIL/PASSWORD`,
`DIM_AIRPORTS_SOURCE`, `DIM_CACHE_MAX_AGE_DAYS`, `COLLECTION_ZONES`, `LOAD_SILVER_GOLD`,
`ALERT_THRESHOLD_PCT_VALID`. Voir l'annexe §12 pour les variables d'env.

### Troubleshooting
| Symptôme | Cause / Solution |
|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` (crash Phase 5, écriture Bronze) | winutils introuvable → **auto-détecté** si placé dans un dossier standard (`%USERPROFILE%\hadoop\bin`…). Sinon, installer winutils + définir `HADOOP_HOME` manuellement. |
| `UnsatisfiedLinkError: NativeIO$Windows.access0` | `hadoop.dll` non chargé → définir `HADOOP_HOME` + PATH (cf. ci-dessus) |
| « Python worker failed to connect back » | Alias Python Windows → **corrigé automatiquement** : `create_spark_session` force `PYSPARK_PYTHON=sys.executable`. (Surchargeable manuellement si besoin.) |
| `CANNOT_ACCEPT_OBJECT_IN_TYPE DoubleType … int` | géré : coercion de types dans `flights_to_dicts` |
| `HTTP Error 429` | quota API → login FR24, `DIM_AIRPORTS_SOURCE=static`, attendre le reset |
| KPIs continentaux vides | dimensions aéroports non chargées → vérifier `data/airports.dat` / source |

---

## 7. Monitoring & Dashboard

### Métriques (`src/job_metrics.py`)
`JobMetrics` collecte par batch : extraction (lignes, durée), validation (valides/invalides, %),
analyse (en vol/au sol), cardinalités des dimensions, lignes par KPI, erreurs/avertissements,
durée totale, statut. Sauvegarde JSON dans `datalake/_logs/` (lu par le dashboard).

### Dashboard Streamlit (`dashboard.py`, 4 pages)
```bash
streamlit run dashboard.py    # http://localhost:8501  (pandas/pyarrow, sans Spark)
```
- **Statut d'exécution** — détecte un run **en cours / terminé / interrompu** en analysant le
  dernier `datalake/_logs/batch_job_*.log` (helpers `_latest_batch_log`/`_analyze_run`) :
  barre de progression par phase (1→6), durée, checklist des phases, **résultat** (statut,
  erreurs, durée) et 50 dernières lignes du journal. **Auto-refresh 5 s** tant que le run tourne.
- **KPIs (Gold)** — **valeurs métier** des 7 KPIs lues dans `datalake/gold/kpi_*` (helper
  `_read_kpi`, pandas) : compagnie la plus active, top régional/continent (+ bar chart), vol le
  plus long, distance moyenne/continent (+ bar chart), constructeur, top-3 modèles/pays
  (sélecteur), déséquilibre aéroport. Garde-fous si Gold absent.
- **Last Execution** — métriques d'exécution du dernier batch.
- **Execution History** — historique, trends, export CSV.

### Alerting (`src/alerting.py`)
Règles sur les métriques finalisées : erreurs → **CRITICAL** ; qualité < seuil ; durée > SLA ;
extraction vide → **WARNING**. Journalisées et écrites en `datalake/_logs/alerts/`.

---

## 8. Tests

Suite **pytest** (~80 tests) : `unit/`, `integration/`, `e2e/`. Markers : `slow`, `e2e`
(désélectionnés par défaut). Fixtures dans `tests/conftest.py` (session Spark, `temp_datalake`
qui redirige tous les chemins dérivés, `make_mock_flight`, `parquet_write_supported`).

```bash
pytest -m "not slow and not e2e" -q     # suite rapide (unit + integration mockée)
pytest tests/unit -q                     # unitaires seuls
```

### Couverture
- **Unit** : schémas, qualité (8 flags paramétrés), extraction (API mockée), datalake_utils,
  transformations (7 KPIs sur valeurs connues, dimensions, enrichissement), reference_data,
  dimension_loader (cache, parseur OpenFlights), fault-tolerance (retries + alerting).
- **Integration** : `run_batch` (chemin nominal), `run_full_etl` Bronze→Silver→Gold, round-trip
  Parquet partitionné (anti-régression `tech_*`), dédup/idempotence, conformité `tech_day`.
- **E2E** (`@slow @e2e`) : cycle complet avec l'API réelle.

### Note Windows
La fixture `spark_session` **auto-détecte** `HADOOP_HOME`/winutils (même mécanisme que le pipeline),
donc la suite tourne **intégralement** sous Windows : **~79 passés, 0 skipped** (1 e2e désélectionné).
Les 7 tests qui écrivent du Parquet ne se *skip* (proprement) **que si winutils est introuvable**
(machine sans winutils) — filet de sécurité, pas un échec.

---

## 9. Fault-tolerance & qualité des données

Philosophie **« loud but not breaking »** : on ne **masque** jamais un problème, mais on
n'**interrompt** pas le batch.
- **Données douteuses** → flags `data_quality_flags` + `is_valid=False` (pas d'exception).
- **Échec API transitoire** → `retry_with_backoff` + `RetryPolicy` (backoff, 429) ; après
  épuisement, la zone renvoie `[]` (best-effort, le batch continue).
- **Silver/Gold en erreur** → warning, le Bronze reste écrit.
- **Toute exception du batch** → log détaillé (`exc_info`), métriques finalisées + alerte,
  retour `False`.
- **Alerting** : erreurs/qualité/SLA/extraction vide → fichier + log + webhook optionnel.

---

## 10. Historique de développement

Le projet a été construit itérativement (détail des décisions ci-dessous résumé) :

| # | Étape | Apport principal |
|---|---|---|
| 1 | Modélisation | schémas Spark, star schema, 8 flags qualité |
| 2 | Structure datalake | `PipelineConfig`, partitions, scripts admin |
| 3 | POC Spark batch | extraction API → Bronze |
| 3.5 | Test-Based Dev | suite pytest (unit/integration/e2e) |
| 4 | Transformation Silver+Gold | `transformations.py`, 7 KPIs |
| 5 | Optimisation partitionnement | outillage d'analyse (retiré en §14 pour alléger) |
| 6 | Logging & monitoring | `JobMetrics` + dashboard Streamlit |
| 7 | Job final + scheduling | job unifié `run_batch` + cron/Task Scheduler |
| 8 | Revue de code & corrections | unification du job, bugs critiques, `reference_data` |
| 9 | Fault-tolerance avancée | retries backoff + alerting |
| 10 | Exécution réelle | infra Windows, `enrich`, dimensions, contraintes API |
| 11 | Anti-quota | login, backoff 429, dims bulk + cache, source aéroports static |
| 12 | Dashboard KPIs (Gold) | page valeurs métier (pandas/pyarrow) |
| 13 | Partitionnement → tech_hour | Silver & Gold partitionnés **jusqu'à l'heure** : résultats retrouvables par (Date, Heure), idempotents par heure |
| 14 | Allègement du projet | retrait tooling non câblé, code mort, schémas KPI périmés, `requirements` slim |
| 15 | Portabilité & auto-config | `PYSPARK_PYTHON`/`HADOOP_HOME` auto-détectés ; onglet dashboard « Statut d'exécution » (suivi des runs en direct) |
| 16 | Jeu de référence auto-géré | `ensure_airports_dataset` : téléchargé si absent, rafraîchi à 14 j (mtime), fallback hors-ligne |
| 17 | Conteneurisation Docker | image unique `flight-etl` + `docker compose` (etl + dashboard), volume `flight_datalake`, « first run » sans Java/winutils |
| 18 | Correctif KPI distance | exclusion des vols circulaires (origine = destination, 0 km) des KPI 3 & 4 |
| 19 | Orchestration Airflow | DAG toutes les 2 h via **DockerOperator** (Postgres + LocalExecutor) ; tâche `check_outcome` |

### Corrections notables (hardening)
- **Job unifié** : suppression d'un duplicata `streaming_job.py` ; toute la logique dans
  `batch_job.run_batch` ; signatures alignées.
- **Partitionnement Bronze** : valeurs correctes via `get_partition_values` (auparavant cast
  epoch/string corrompu).
- **Infra Windows** : winutils/`HADOOP_HOME` + `PYSPARK_PYTHON` désormais **auto-détectés** dans
  `create_spark_session` (plus de réglage manuel), `spark.driver.extraLibraryPath`, coercion
  int→double, logging UTF-8, `.cache()` (counts non recalculés). *(Docker évite ces prérequis.)*
- **API** : `get_airports` peu fiable/lent et 429 en anonyme → **source statique OpenFlights**
  par défaut (flag `DIM_AIRPORTS_SOURCE`).
- **Inférence de partition** désactivée pour des `tech_*` cohérents (string) entre couches.

---

## 11. Statistiques & livrables

- **Code** : modules `src/` (schemas, data_quality, flight_extraction, batch_job,
  transformations, silver_gold_loader, dimension_loader, reference_data, job_metrics, alerting,
  datalake_utils), `config/` (pipeline_config), `scripts/`
  (run_job, init_datalake, purge_old_partitions), `dashboard.py`.
- **Conteneurisation & orchestration** : `Dockerfile` + `docker-compose.yml` (etl + dashboard +
  profil airflow), `.dockerignore`, `.env.example` ; `airflow/` (Dockerfile + `dags/flight_etl_dag.py`).
- **Données de référence** : `data/airports.dat` (OpenFlights, ~7700 aéroports) — **auto-géré**
  (`ensure_airports_dataset` : téléchargement/refresh à 14 j, fallback hors-ligne). Non commité
  (`data/` est dans `.gitignore`) : la 1re exécution part toujours d'un dépôt vierge.
- **Tests** : ~80 (unit + integration + e2e).
- **Pipeline vérifié de bout en bout** (run anonyme) : 9 zones → Bronze ≈ 7600 vols → Silver
  (fact + 4 dims) → **7/7 KPIs Gold peuplés**. Exemples : vol le plus long **SIN→JFK ≈ 15 340 km**,
  top régional EU **Ryanair**, constructeur **Airbus**, déséquilibre **ICN (+73)**.

---

## 12. Annexes

### Variables d'environnement principales
| Variable | Rôle | Défaut |
|---|---|---|
| `DATALAKE_ROOT` | racine du datalake | `./datalake` |
| `LOAD_SILVER_GOLD` | activer Silver/Gold (sinon `--with-silver-gold`) | `false` |
| `SPARK_MASTER` | maître Spark | `local[*]` |
| `SPARK_DRIVER_MEMORY` | mémoire driver (seul levier mémoire en local) | `2g` |
| `SPARK_SHUFFLE_PARTITIONS` | partitions de shuffle (bas car petite donnée ; AQE coalesce) | `16` |
| `COLLECTION_ZONES` (config) | zones collectées | 9 zones top-level |
| `API_ENRICH_DETAILS` | enrichissement par vol (déconseillé) | `false` |
| `API_MAX_WORKERS_PARALLEL` | concurrence API | `3` |
| `API_MAX_RETRIES` / `API_RETRY_*` | retries/backoff | 3 / 4·5s·60s |
| `FR24_EMAIL` / `FR24_PASSWORD` | login FR24 (quota) | vide (anonyme) |
| `DIM_AIRPORTS_SOURCE` | `static` (OpenFlights) ou `api` | `static` |
| `DIM_AIRPORTS_STATIC_PATH` | chemin du jeu aéroports | `data/airports.dat` |
| `DIM_AIRPORTS_STATIC_URL` | source de téléchargement du jeu | OpenFlights (GitHub) |
| `DIM_AIRPORTS_MAX_AGE_DAYS` | TTL avant rafraîchissement du jeu | `14` |
| `DIM_CACHE_MAX_AGE_DAYS` | fraîcheur du cache dimensions | `7` |
| `ALERT_THRESHOLD_PCT_VALID` | seuil d'alerte qualité | `70` |
| `HADOOP_HOME`, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` | prérequis Spark Windows | — |

### Références conservées
- **`documentation/JOURNAL_DEV.md`** — journal de développement : synthèse chronologique des
  étapes du projet (le *pourquoi* de chaque décision), reconstruite depuis l'historique git.
- **`premiere_exploration/notebook_exploration.ipynb`** — exploration interactive de l'API
  FlightRadar24 (méthodologie, champs, observations de qualité).
