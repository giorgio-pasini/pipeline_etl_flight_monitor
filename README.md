# Pipeline ETL — Trafic Aérien Mondial ✈️

Pipeline **ETL batch** (Apache Spark) qui collecte le trafic aérien mondial via l'API
**FlightRadar24**, l'enrichit dans une architecture **Medallion** (Bronze → Silver → Gold),
calcule **7 KPIs** et les expose dans un **dashboard Streamlit**. Orchestré **toutes les 2 heures**
par **Apache Airflow** (conteneurisé via Docker).

**Les 7 KPIs** : (1) compagnie la plus active · (2) top compagnie régionale par continent ·
(3) vol en cours le plus long · (4) distance moyenne par continent · (5) constructeur le plus
actif · (6) top 3 modèles d'avion par pays · (7, bonus) aéroport au plus grand écart
départs/arrivées.

```
API FlightRadar24 ──► BRONZE (brut) ──► SILVER (fact_flights + 4 dimensions) ──► GOLD (7 KPIs) ──► Dashboard
                       partition year/month/day[/hour]
```

## Architecture

Quatre rôles séparés ; tout communique par un **volume partagé** (`flight_datalake`) :

```
   ┌──────────────┐  déclenche toutes les 2 h (DockerOperator)
   │   Airflow    │ ─────────────────────────────┐
   │   :8080      │  ordonnancement & supervision │
   └──────────────┘                               ▼
                                        ┌────────────────────┐
   API FlightRadar24 ──────────────────►│  flight-etl (job)  │  run_batch (Spark)
                                        │  conteneur isolé   │
                                        └─────────┬──────────┘
                                                  │ écrit Parquet + métriques JSON
                                                  ▼
                       volume   ┌──────────────────────────────────────┐
                     flight_    │  BRONZE → SILVER → GOLD  + _logs/      │
                     datalake   │                          _reference/  │
                                └───────────────────┬───────────────────┘
                                                    │ lit (pandas/pyarrow)
                                                    ▼
                                          ┌────────────────────┐
                                          │  Dashboard         │  KPIs + statut des runs
                                          │  Streamlit :8501   │
                                          └────────────────────┘
```

| Rôle | Composant | Détail |
|---|---|---|
| **Ordonnancement** | Airflow (DAG `flight_etl_pipeline`) | toutes les 2 h, retries, supervision ; déclenche le job via DockerOperator |
| **Exécution** | conteneur `flight-etl` | `run_batch` : extraction → validation → Bronze → Silver → Gold (Spark local) |
| **Stockage** | volume `flight_datalake` | Medallion Parquet partitionné + `_logs/` (métriques) + `_reference/` (jeu aéroports) |
| **Visualisation** | dashboard Streamlit | KPIs (Gold) + suivi des runs en direct, lit le même volume |

> En production, le `DockerOperator` se remplace par un `KubernetesPodOperator` (même DAG) — voir
> [DOCUMENTATION.md §6](documentation/DOCUMENTATION.md#6-exécution--exploitation).

## Structure du projet

```
├── src/                      # cœur du pipeline
│   ├── batch_job.py          #   point d'entrée unique (run_batch : 7 phases)
│   ├── flight_extraction.py  #   collecte API (zones, dédup, login/retry)
│   ├── dimension_loader.py   #   dimensions + jeu aéroports auto-géré (download/refresh)
│   ├── transformations.py    #   nettoyage, enrichissement, 7 KPIs
│   ├── silver_gold_loader.py #   orchestration Bronze → Silver → Gold
│   ├── data_quality.py       #   8 flags qualité + is_valid
│   ├── schemas.py            #   schémas Spark (Bronze/Silver/Gold)
│   ├── reference_data.py     #   pays→continent, avion→constructeur
│   ├── job_metrics.py        #   métriques d'exécution (JSON)
│   ├── alerting.py           #   alertes (fichier/log/webhook)
│   └── datalake_utils.py     #   partitions, rétention/cleanup
├── config/                   # datalake_config.py (config centralisée)
├── scripts/                  # run_job.py, init_datalake.py, purge_old_partitions.py
├── airflow/                  # orchestration : Dockerfile + dags/flight_etl_dag.py (toutes les 2 h)
├── Dockerfile                # image flight-etl (JDK 17 + Python 3.11 + deps + code)
├── docker-compose.yml        # services etl + dashboard (+ profil airflow : postgres + airflow)
├── .dockerignore             # (et .env.example pour surcharges/secrets)
├── tests/                    # unit / integration / e2e (~95 tests)
├── data/airports.dat         # référentiel aéroports OpenFlights (auto-téléchargé/rafraîchi)
├── datalake/                 # bronze/ silver/ gold/ _logs/  (généré)
├── dashboard.py              # dashboard Streamlit (statut run + KPIs + métriques)
├── documentation/
│   └── DOCUMENTATION.md       # 📖 documentation technique complète
├── plan_de_implementation.md # cahier des charges original
└── premiere_exploration/     # notes de découverte de l'API
```

## 🐳 Démarrage rapide avec Docker (recommandé)

Aucun prérequis hormis **Docker Desktop** — ni Java, ni Python, ni winutils à installer. Une
seule commande :

```bash
docker compose up        # build l'image, lance le batch ETL + le dashboard
```

- Dashboard : **http://localhost:8501** (suivez le run en direct via l'onglet « Statut d'exécution »).
- Le service `etl` exécute un batch complet (Bronze → Silver → Gold) puis sort ; les données
  persistent dans le volume `datalake`.
- Relancer un batch plus tard : `docker compose run --rm etl`.
- (Optionnel) `cp .env.example .env` pour des identifiants FR24 ou ajuster la mémoire Spark.

## 🗓️ Orchestration Airflow (toutes les 2 h)

Airflow **orchestre** le pipeline (il ne l'exécute pas en interne) : le DAG déclenche le batch dans
un conteneur `flight-etl` isolé via **DockerOperator** (équivalent local du `KubernetesPodOperator`
de prod), toutes les 2 h.

*important* : le moteur docker desktop doit etre actif

```bash
docker compose build                         # image flight-etl
docker compose --profile airflow up -d       # Airflow (:8080) + dashboard (:8501)
```

- **Airflow** : http://localhost:8080 — identifiants **`admin` / `admin`**. DAG
  `flight_etl_pipeline`, planifié `0 */2 * * *`.
- **Dashboard** : http://localhost:8501 — visualise chaque run planifié (onglet « Statut
  d'exécution ») et les KPIs rafraîchis (onglet « KPIs (Gold) »).
- Déclencher un run à la demande :
  `docker compose exec airflow airflow dags trigger flight_etl_pipeline`.

> Airflow = ordonnancement & supervision · dashboard = visualisation · `flight-etl` = exécution.

## Démarrage rapide (natif, sans Docker)

```bash
pip install -r requirements.txt                   # Python 3.9–3.11 + un JDK (8/11/17) requis
python scripts/init_datalake.py
python scripts/run_job.py --with-silver-gold     # 1 batch : Bronze → Silver → Gold
streamlit run dashboard.py                        # KPIs : http://localhost:8501
pytest -m "not slow and not e2e" -q               # tests
```

> **Windows** — l'écriture Parquet exige `winutils.exe`/`hadoop.dll` : placez-les dans
> `%USERPROFILE%\hadoop\bin` et `HADOOP_HOME` est **auto-détecté** (comme `PYSPARK_PYTHON`).
> Voir [DOCUMENTATION.md § 6](documentation/DOCUMENTATION.md#6-exécution--exploitation).
> *(Docker évite ce prérequis entièrement.)*

## Commandes utiles (interaction)

| Action | Commande |
|---|---|
| Démo : 1 batch + dashboard | `docker compose up` |
| Mode orchestré : Airflow + dashboard | `docker compose --profile airflow up -d` |
| Relancer un batch à la main | `docker compose run --rm etl` |
| Déclencher le DAG immédiatement | `docker compose exec airflow airflow dags trigger flight_etl_pipeline` |
| Mot de passe admin Airflow | `docker compose logs airflow \| grep -i password` |
| Suivre les logs d'un service | `docker compose logs -f etl` (ou `airflow`, `dashboard`) |
| Reconstruire après modif du code | `docker compose build` |
| Arrêter (données conservées) | `docker compose --profile airflow down` |
| Tout réinitialiser (efface les volumes) | `docker compose --profile airflow down -v` |

**Interfaces** : dashboard → http://localhost:8501 · Airflow → http://localhost:8080

## Documentation

Toute la documentation technique est dans **[documentation/DOCUMENTATION.md](documentation/DOCUMENTATION.md)** :

| Pour… | Section |
|---|---|
| Comprendre l'architecture & les choix | [§1 Vue d'ensemble](documentation/DOCUMENTATION.md#1-vue-densemble) |
| Le modèle de données & les KPIs | [§2 Modèle de données](documentation/DOCUMENTATION.md#2-modèle-de-données) |
| Le datalake & le partitionnement | [§3 Datalake & partitionnement](documentation/DOCUMENTATION.md#3-datalake--partitionnement) |
| Le déroulé du pipeline | [§4 Pipeline ETL](documentation/DOCUMENTATION.md#4-pipeline-etl) |
| L'API & la gestion du quota | [§5 Collecte API & anti-quota](documentation/DOCUMENTATION.md#5-collecte-api--anti-quota) |
| Lancer / planifier / configurer | [§6 Exécution & exploitation](documentation/DOCUMENTATION.md#6-exécution--exploitation) |
| Le dashboard & le monitoring | [§7 Monitoring & Dashboard](documentation/DOCUMENTATION.md#7-monitoring--dashboard) |
| Les tests | [§8 Tests](documentation/DOCUMENTATION.md#8-tests) |

Le requis original est dans [plan_de_implementation.md](plan_de_implementation.md).
