# Quickstart — Pipeline ETL Trafic Aérien

Ce document décrit comment démarrer le pipeline en mode POC (Proof of Concept).

---

## Prérequis

- Python 3.9+
- Apache Spark 3.3+ (avec PySpark)
- `FlightRadarAPI` v1.5.1 (déjà installé)
- Pandas, NumPy (via requirements.txt)

```bash
# Installation des dépendances
pip install -r requirements.txt
```

---

## 1. Initialiser le datalake

L'initialisation crée la structure complète (bronze, silver, gold) et prépare les répertoires.

```bash
# Initialisation avec paramètres par défaut
python scripts/init_datalake.py

# Ou spécifier un chemin personnalisé
python scripts/init_datalake.py --datalake-root /data/my_datalake --verbose
```

**Résultat :** Arborescence complète créée, fichiers `.env.example` et logs d'initialisation générés.

---

## 2. Exécuter le POC Streaming (1 batch)

Le job POC collecte les vols une fois, les valide, et les écrit en Bronze.

```bash
# POC simple : collecte global, mode dry-run (validation seulement, pas d'écriture)
python src/batch_job.py --single-batch --dry-run --verbose

# POC complet : collecte + écriture en Bronze
python src/batch_job.py --single-batch

# POC multi-zones (si vouloir tester plusieurs régions)
python src/batch_job.py --zones global europe asia --single-batch

# POC avec datalake personnalisé
python src/batch_job.py --datalake-root /data/my_datalake --single-batch
```

**Que se passe-t-il :**

1. **Extraction** : Appelle `get_flights()` de l'API (retourne ~1500 vols)
2. **Validation** : Ajoute des flags de qualité (`MISSING_ORIGIN`, etc.) et calcule `is_valid`
3. **Profil** : Affiche stats de qualité (% valid, on_ground, missing fields)
4. **Partitionnement** : Ajoute colonnes `tech_year`, `tech_month`, `tech_day`, `tech_hour`
5. **Chargement** : Écrit en Parquet dans `datalake/bronze/flights_raw/tech_year=.../...`

**Logs :** Affichage console + fichier dans `datalake/_logs/streaming_job_*.log`

---

## 3. Vérifier les données écrites

Après un batch réussi, explorer les données :

```python
# Lire les données Bronze écrites
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataExplore").getOrCreate()

df = spark.read.parquet("datalake/bronze/flights_raw")
print(f"Total flights: {df.count()}")
print(f"Columns: {df.columns}")
print("\nSample data:")
df.select("callsign", "airline_icao", "origin_iata", "destination_iata", "on_ground", "is_valid").show(5)

# Profil de qualité
print("\nQuality flags:")
df.groupBy("data_quality_flags").count().show()

print(f"\nValid flights: {df.filter('is_valid == True').count()}")
```

---

## 4. Nettoyer les anciennes partitions (Rétention)

Une fois que plusieurs batches ont accumulé des données, nettoyer selon la rétention :

```bash
# Afficher ce qui serait supprimé (dry-run)
python scripts/purge_old_partitions.py --all-layers --dry-run --verbose

# Estimer l'espace disque utilisé
python scripts/purge_old_partitions.py --estimate-only

# Vraie suppression (ATTENTION : destructif)
python scripts/purge_old_partitions.py --all-layers --execute --verbose
```

---

## 5. Architecture du POC

```
Extraction (1 batch)
  |
  v
[1500 vols API] ---(FlightRadarAPI.get_flights)---> Flight objects
  |
  v
Validation & Flagging
  |
  v
[DataFrame Spark] + [Quality stats]
  |
  v
Partitioning
  |
  v
[Bronze Parquet] datalake/bronze/flights_raw/tech_year=.../
```

---

## 6. Prochaines étapes (Phase 2+)

Après validation du POC :

1. **Silver Layer** : Transformer + enrichir les données
   - Joindre avec `dim_airlines`, `dim_airports`
   - Calculer distances, continents
   - Créer `fact_flights` partitionnée

2. **Gold Layer** : Agrégations et KPIs
   - `kpi_airline_volumes` (compagnie +vols)
   - `kpi_continental_regional` (vols régionaux)
   - ... (7 tables KPIs au total)

3. **Streamlit Dashboard** : Visualiser les KPIs temps-réel

4. **Logging & Monitoring** : Alertes, Prometheus metrics

5. **Airflow Orchestration** : Scheduler les jobs (vs. cron)

---

## Configuration (variables d'environnement)

Le fichier `config/datalake_config.py` fournit des défauts sensés.
Surcharger via env vars :

```bash
export DATALAKE_ROOT=/data/my_datalake
export BRONZE_RETENTION_DAYS=30
export LOG_LEVEL=DEBUG
export SPARK_SHUFFLE_PARTITIONS=100

python src/batch_job.py --single-batch
```

Ou via fichier `.env` (source avant lancement) :

```bash
# .env
DATALAKE_ROOT=/data/datalake
LOG_LEVEL=INFO
SPARK_EXECUTOR_MEMORY=4g

source .env
python src/batch_job.py --single-batch
```

---

## Troubleshooting

### "Aucun vol collecté"
- L'API peut retourner 0 vol si le serveur FR24 est en maintenance
- Attendre quelques minutes, puis réessayer
- Vérifier la connexion réseau

### "Erreur Spark : task failed"
- Vérifier les logs dans `datalake/_logs/`
- S'assurer que Spark peut écrire dans `DATALAKE_ROOT` (permissions)
- Augmenter `SPARK_EXECUTOR_MEMORY` si out-of-memory

### "Erreur: 'tech_year' column not found"
- Bug mineur du POC (les colonnes de partitionnement ne sont pas systématiquement présentes)
- Sera fixé en Étape 4 lors de la transformation Silver complète

### "Fichier `.env.example` non généré"
- L'initialisation a échoué (voir logs)
- Créer manuellement en copiant `config/datalake_config.py` en tant que template

---

## Documentation complète

- [Plan d'implémentation](plan_de_implementation.md)
- [Modèle de données](README_modele.md)
- [Documentation développement](documentation/documentation_dev.md)
- [Exploration API](notebook_exploration.ipynb)

---

## Support

En cas de problème :
1. Vérifier les logs (`datalake/_logs/`)
2. Relire la section Troubleshooting
3. Vérifier le plan d'implémentation pour comprendre l'architecture
4. Consulter le notebook d'exploration (`notebook_exploration.ipynb`) pour l'API
