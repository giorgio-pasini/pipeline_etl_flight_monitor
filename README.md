# Pipeline ETL Batch — Trafic aérien mondial

**Client :** Exalt (Technical Assessment)  
**Langue :** Français  
**Statut :** 3 étapes complétées, POC opérationnel  
**Date :** 2026-06-21

---

## 🎯 Objectif

Construire un **pipeline ETL batch** (exécution toutes les 2 heures) pour collecter, nettoyer et analyser le trafic aérien mondial via l'API **FlightRadarAPI**. Le pipeline génère 7 indicateurs clés (KPIs) prédéfinis et les expose via un dashboard.

**Indicateurs obligatoires :**
1. La compagnie avec le + de vols en cours
2. Par continent, la compagnie la + active en vols régionaux (continent orig == continent dest)
3. Le vol en cours au trajet le + long
4. Par continent, longueur de vol moyenne
5. Le constructeur d'avions avec le + de vols actifs
6. Par pays de compagnie, top 3 des modèles d'avion en usage
7. **Bonus :** Aéroport au + grand écart départs/arrivées

---

## 📊 Architecture haute niveau

```
API FlightRadar24 (temps-réel)
        ↓
   [Batch 2h]
        ↓
┌─────────────────────────────┐
│  BRONZE (Données brutes)    │
│  - flights_raw (Parquet)    │
│  - Partitionné              │
│    tech_year/month/day/hour │
└─────────────────────────────┘
        ↓
   [Nettoyage + Enrichissement]
        ↓
┌─────────────────────────────┐
│ SILVER (Données nettoyées)  │
│  - fact_flights             │
│  - dim_airlines             │
│  - dim_airports             │
│  - dim_aircraft_models      │
│  - dim_countries_continents │
└─────────────────────────────┘
        ↓
   [Agrégations + KPIs]
        ↓
┌─────────────────────────────┐
│ GOLD (Indicateurs)          │
│  - kpi_airline_volumes      │
│  - kpi_continental_regional │
│  - kpi_longest_flights      │
│  - kpi_continental_avg_...  │
│  - kpi_aircraft_manufacturers
│  - kpi_airline_aircraft_... │
│  - kpi_airport_imbalance    │
└─────────────────────────────┘
        ↓
   [Dashboard Streamlit]
        ↓
   📊 Visualisations temps-réel
```

---

## 🏗️ Structure du projet

```
.
├── README.md                          # Ce fichier
├── README_modele.md                   # Justifications modèle de données
├── README_quickstart.md                # Guide de démarrage rapide
├── plan_de_implementation.md           # Plan général
├── requirements.txt                    # Dépendances Python
│
├── notebook_exploration.ipynb          # Exploration initiale de l'API
│
├── config/
│   └── datalake_config.py             # Configuration centralisée (chemins, rétention, etc.)
│
├── src/
│   ├── __init__.py
│   ├── schemas.py                     # Définitions des schémas Spark
│   ├── data_quality.py                # Validation + flagging qualité
│   ├── datalake_utils.py              # Utilitaires (partitionnement, nettoyage)
│   ├── flight_extraction.py           # Extraction depuis l'API
│   ├── batch_job.py                   # Job principal Spark Core Batch
│   └── transformations.py             # Transformations Silver → Gold (à venir)
│
├── scripts/
│   ├── __init__.py
│   ├── init_datalake.py              # Initialiser la structure du datalake
│   └── purge_old_partitions.py       # Nettoyer les anciennes partitions
│
├── datalake/                          # Dtalake local (créé par init_datalake.py)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── _logs/
│
└── documentation/
    └── documentation_dev.md           # Journal développement complet (FR)
```

---

## ✅ Étapes complétées

### Étape 1 : Modélisation des données ✅

**Deliverables :**
- **`src/schemas.py`** : 12 schémas Spark (Bronze, Silver, Gold)
- **`src/data_quality.py`** : 8 types de flags de qualité, validation progressive
- **`README_modele.md`** : Justifications complètes des choix architecturaux

**Highlights :**
- Star schema (1 table fact + 4 dimensions + 7 KPIs)
- 8 flags de qualité (MISSING_ORIGIN, INVALID_ALTITUDE, etc.)
- Traçabilité complète via `data_quality_flags` et `is_valid`

### Étape 2 : Structure du datalake ✅

**Deliverables :**
- **`config/datalake_config.py`** : Configuration centralisée (chemins, rétention, Spark config)
- **`src/datalake_utils.py`** : Utilitaires partitionnement, cleanup, estimation stockage
- **`scripts/init_datalake.py`** : Initialisation complète (répertoires, exemples, docs)
- **`scripts/purge_old_partitions.py`** : Nettoyage par rétention (dry-run + execution)

**Highlights :**
- Partitionnement temporel spec kata : `tech_year/tech_month/tech_day/tech_hour`
- Rétention configurable : Bronze 30j, Silver 60j, Gold 365j
- Scripts idempotents et avec safeguards

### Étape 3 : POC Spark Batch ✅

**Deliverables :**
- **`src/flight_extraction.py`** : Classe `FlightExtractor` (collecte, conversion DataFrame)
- **`src/batch_job.py`** : Job Spark Core batch (extract → validate → load Bronze)
- **`README_quickstart.md`** : Guide démarrage rapide (français)

**Highlights :**
- Cycle complet testé : API → Spark DataFrame → Parquet Bronze
- Validation et flagging en place
- Rapports de qualité sauvegardés
- Fault-tolerance : les erreurs n'arrêtent pas le job
- **À orchestrer toutes les 2 heures** (via Airflow/cron, pas streaming continu)

---

## 🚀 Démarrage rapide

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Initialiser le datalake
python scripts/init_datalake.py

# 3. Lancer le POC (1 batch)
python src/batch_job.py --single-batch --verbose

# 4. Vérifier les données
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Check').getOrCreate()
df = spark.read.parquet('datalake/bronze/flights_raw')
print(f'Vols collectés : {df.count()}')
df.select('callsign', 'airline_icao', 'on_ground', 'is_valid').show(3)
"
```

**Pour plus de détails :** voir [README_quickstart.md](README_quickstart.md)

---

## 🔄 Prochaines étapes (Phase 2)

### Étape 4 : Transformation & KPIs
- Créer la couche Silver (nettoyage + enrichissement)
- Créer les 7 tables Gold avec les KPIs
- Valider chaque KPI contre les données

### Étape 5 : Stratégie de partitionnement
- Analyser les patterns d'accès (quels KPIs demandent quelles colonnes)
- Optimiser les partitions secondaires
- Mesurer les performances

### Étape 6 : Logging & Monitoring
- Intégration Prometheus / Grafana
- Alertes (ex: si % valid < 70%)
- Dashboard de monitoring interne

### Étape 7 : Job Spark final
- Boucle infinie vs. cron (recommandé : cron toutes les 2h)
- Gestion checkpoint Spark
- Retry logic

### Étape 8 : Dashboard Streamlit
- Visualisation des 7 KPIs
- Filtres (date, compagnie, continent, etc.)
- Graphiques temps-réel

### Étape 9 : Fault-tolerance & erreurs
- Policy "loud but not breaking" : logs détaillés, données flaggées
- Gestion gracieuse des timeouts API
- Alertes Slack/email en cas d'anomalie

### Phase 3 (Futur)
- **Orchestration Airflow** : scheduling automatique
- **Déploiement AWS** : S3 datalake, EC2/ECS workers, RDS metadata
- **ML** : prédiction retards, anomaly detection

---

## 📋 Choix technologiques justifiés

| Choix | Justification | Alternatives rejetées |
|-------|---------------|------------------------|
| **Apache Spark Core (batch)** | Scalabilité, fault-tolerance, job toutes les 2h (pas streaming continu) | Pandas (pas fault-tolerant), Flink (overkill pour cette taille) |
| **Orchestration externe** | Job batch = 1 exécution/2h → scheduler (Airflow, cron) | Structured Streaming (pour temps-réel continu, pas applicable ici) |
| **Parquet** | Compression, columnar, schéma fort, écosystème large | CSV (zéro compression), AVRO (moins répandu), Delta (MVP trop lourd) |
| **Partitionnement temporel** | Spec kata, partition pruning, rétention facile | Aucun (scans slow), partitionnement par zone (mélange concerns) |
| **Star Schema** | Réutilisabilité, audit trail, séparation concerns | Dénormalisé (anomalies), snowflake (complexité pour cette taille) |
| **Medallion 3 couches** | Traçabilité, découplage, résilience | 1 couche (trop simple), 4+ couches (over-engineering) |

Voir [README_modele.md](README_modele.md) pour justifications complètes.

---

## 🔍 Qualité des données

Le pipeline inclut :
- **8 types de flags** : MISSING_ORIGIN, INVALID_ALTITUDE, INCONSISTENT_POSITION, etc.
- **Colonne `is_valid`** : synthèse "ce vol est utilisable pour KPIs"
- **Profil de qualité** : rapports JSON avec % valid, on_ground, flags top
- **Fault-tolerance** : les erreurs ne cassent pas le job, juste flaggées

Exemple :
```
Vols collectés : 1500
Vols valides (is_valid=True) : 1178 (78.5%)
Au sol (on_ground=1) : 142 (9.5%)

Flags les plus courants :
  MISSING_DESTINATION : 346
  MISSING_ORIGIN : 42
  INVALID_ALTITUDE : 8
```

---

## 📚 Documentation

| Fichier | Contenu |
|---------|---------|
| [README_modele.md](README_modele.md) | Détails modèle de données + justifications |
| [README_quickstart.md](README_quickstart.md) | Guide démarrage (init → run → check) |
| [documentation/documentation_dev.md](documentation/documentation_dev.md) | Journal développement complet (FR) |
| [plan_de_implementation.md](plan_de_implementation.md) | Plan général du kata |
| [notebook_exploration.ipynb](notebook_exploration.ipynb) | Exploration interactive de l'API |

---

## 🛠️ Configuration

Configuration centralisée dans `config/datalake_config.py` :

```python
# Exemple : surcharger via env vars
export DATALAKE_ROOT=/data/my_datalake
export BRONZE_RETENTION_DAYS=30
export LOG_LEVEL=DEBUG
export SPARK_EXECUTOR_MEMORY=4g

python src/batch_job.py --single-batch
```

Ou fichier `.env` :
```bash
source .env
python src/streaming_job.py --single-batch
```

---

## 🔐 Sécurité & Résilience

- **Fault-tolerant** : erreurs API/Spark loggées mais n'arrêtent pas le job
- **Validation** : schémas forcés, nullability checked, bounds vérifiés
- **Audit trail** : `batch_id`, `extraction_timestamp`, `data_quality_flags` dans toutes les données
- **Rétention** : anciennes partitions supprimées automatiquement (script purge)
- **Idempotence** : chaque batch peut être rejouable (write mode "append")

---

## 📊 Volumes estimés

**Par batch (2 heures) :**
- ~1500 vols collectés (sans zone)
- Bronze (comprimé) : ~5-10 MB
- Silver (après nettoyage) : ~8-15 MB
- Gold (agrégations) : ~0.5-1 MB
- **Total : ~25 MB/batch**

**Par mois :** ~18 GB  
**Par an (archivé) :** ~220 GB (très petit, trivial à stocker/traiter)

---

## 🎓 Apprentissages & Points clés

1. **API FlightRadarAPI** : ~1500 vols max par appel global → collector par zone pour coverage
2. **Pays vs. Continent** : API donne pays → mapping statique vers continent obligatoire
3. **Enrichissement coûteux** : `get_flight_details()` = 1 req/vol → paralléliser avec Spark
4. **Quality flags** : ne JAMAIS échouer silencieusement → flagger explicitement
5. **Partitionnement** : temporel (année/mois/jour/heure) prune automatiquement

---

## 📞 Support

En cas de question :
1. Consulter la doc pertinente (README_modele, README_quickstart)
2. Vérifier les logs dans `datalake/_logs/`
3. Relire [documentation_dev.md](documentation/documentation_dev.md)
4. Tester avec `notebook_exploration.ipynb`

---

## 📄 Licences

- **FlightRadarAPI** : Open source (BeautifulSoup, curl_cffi dependencies)
- **Apache Spark** : Apache License 2.0
- **Code du projet** : Fourni à titre d'exemple pour Exalt

---

**Dernière mise à jour :** 2026-06-21  
**Version du pipeline :** 0.1.0 (POC)  
**Auteur :** Data Engineering Team
