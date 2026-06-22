# Pipeline ETL — Trafic Aérien Mondial ✈️

Pipeline **ETL batch** (Apache Spark) qui collecte le trafic aérien mondial via l'API
**FlightRadar24**, l'enrichit dans une architecture **Medallion** (Bronze → Silver → Gold),
calcule **7 KPIs** et les expose dans un **dashboard Streamlit**. Conçu pour être orchestré
**toutes les 2 heures** par un scheduler externe.

**Les 7 KPIs** : (1) compagnie la plus active · (2) top compagnie régionale par continent ·
(3) vol en cours le plus long · (4) distance moyenne par continent · (5) constructeur le plus
actif · (6) top 3 modèles d'avion par pays · (7, bonus) aéroport au plus grand écart
départs/arrivées.

```
API FlightRadar24 ──► BRONZE (brut) ──► SILVER (fact_flights + 4 dimensions) ──► GOLD (7 KPIs) ──► Dashboard
                       partition year/month/day[/hour]
```

## Structure du projet

```
├── src/                      # cœur du pipeline
│   ├── batch_job.py          #   point d'entrée unique (run_batch : 7 phases)
│   ├── flight_extraction.py  #   collecte API (zones, dédup, login/retry)
│   ├── dimension_loader.py   #   dimensions de référence (bulk/static + cache)
│   ├── transformations.py    #   nettoyage, enrichissement, 7 KPIs
│   ├── silver_gold_loader.py #   orchestration Bronze → Silver → Gold
│   ├── data_quality.py       #   8 flags qualité + is_valid
│   ├── schemas.py            #   schémas Spark (Bronze/Silver/Gold)
│   ├── reference_data.py     #   pays→continent, avion→constructeur
│   ├── job_metrics.py        #   métriques d'exécution (JSON)
│   ├── alerting.py           #   alertes (fichier/log/webhook)
│   └── datalake_utils.py, partitioning_optimizer.py
├── config/                   # datalake_config.py (config centralisée), spark_tuning.py
├── scripts/                  # run_job.py, init_datalake.py, purge_old_partitions.py,
│                             #   profile_partitions.py, schedule_job.sh/ps1
├── tests/                    # unit / integration / e2e (~85 tests)
├── data/airports.dat         # référentiel aéroports (OpenFlights)
├── datalake/                 # bronze/ silver/ gold/ _logs/  (généré)
├── dashboard.py              # dashboard Streamlit (4 pages)
├── documentation/
│   └── DOCUMENTATION.md       # 📖 documentation technique complète
├── plan_de_implementation.md # cahier des charges original
└── premiere_exploration/     # notes de découverte de l'API
```

## Démarrage rapide

```bash
pip install -r requirements.txt
python scripts/init_datalake.py
python scripts/run_job.py --with-silver-gold     # 1 batch : Bronze → Silver → Gold
streamlit run dashboard.py                        # KPIs : http://localhost:8501
pytest -m "not slow and not e2e" -q               # tests
```

> **Windows** — l'écriture Parquet exige `winutils.exe`/`hadoop.dll` et quelques variables
> d'environnement (`HADOOP_HOME`, `PYSPARK_PYTHON`). Voir
> [DOCUMENTATION.md § 6](documentation/DOCUMENTATION.md#6-exécution--exploitation).

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
