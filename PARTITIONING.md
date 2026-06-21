# Partitioning Optimization — Étape 5

**Date:** 2026-06-21  
**Status:** ✅ Outils en place  
**Durée estimée:** 1 jour

---

## Objectif

Optimiser le partitionnement et la configuration Spark pour :
- ✅ Réduire le temps d'exécution des KPI queries
- ✅ Améliorer l'utilisation des ressources (CPU, mémoire)
- ✅ Équilibrer la distribution des données (éviter skew)
- ✅ Tuner la config Spark selon le workload

---

## 1. Analyse de partitionnement

### 1.1 Partition Skew Analysis

Détecte si les données sont mal distribuées entre partitions.

**Métrique clé :** `skew_ratio = max_rows / min_rows`
- **< 2x** : OK, distribution uniforme
- **2-3x** : À surveiller
- **> 3x** : PROBLÉMATIQUE, ajouter partitionnement secondaire

### 1.2 Taille des partitions

Estime la taille de chaque partition (en MB, GB).

**Recommandations :**
- **< 64 MB** : Trop petit, considérer coalesce
- **64 MB - 1 GB** : Optimal
- **> 1 GB** : Trop gros, ajouter partitionnement

### 1.3 Query Pattern Analysis

Analyse les colonnes utilisées par chaque KPI → recommande partitions.

**Patterns identifiés :**
```
KPI 1 (airline_volumes)        : airline_icao, on_ground
KPI 2 (continental_regional)   : origin_continent, airline_icao
KPI 3 (longest_flight)         : distance, origin/destination
KPI 4 (avg_distance)           : origin_continent, distance
...
```

---

## 2. Outils fournis

### 2.1 `PartitioningOptimizer` class

Analyse complète avec 4 méthodes :

```python
from src.partitioning_optimizer import PartitioningOptimizer

optimizer = PartitioningOptimizer(spark, config)

# Analyze distribution
skew = optimizer.analyze_partition_skew(df, ["tech_year", "tech_month"])

# Estimate sizes
sizes = optimizer.estimate_partition_sizes(df, ["tech_year", "tech_month"])

# Spark config recommendations
recs = optimizer.recommend_spark_config(df)

# Query patterns
patterns = optimizer.analyze_query_patterns()

# Full report
report = optimizer.generate_optimization_report(bronze_path)
```

### 2.2 Script `profile_partitions.py`

Profiler le datalake et générer un rapport.

```bash
# Profile Bronze layer
python scripts/profile_partitions.py --layer bronze

# Profile Silver layer
python scripts/profile_partitions.py --layer silver --verbose

# Profile Gold with custom output
python scripts/profile_partitions.py --layer gold --output /tmp/gold_profile.json
```

**Output :** JSON report avec analyses + recommandations

### 2.3 `SparkTuningProfiles` config

4 profils optimisés :

| Profil | Use case | Executors | Shuffle Partitions |
|--------|----------|-----------|-------------------|
| **POC** | Local, petit volume | 1 | 4 |
| **BATCH** | ETL batch (⭐ NÔTRE) | 8 | 150 |
| **ANALYTICS** | KPI queries | 5 | 100 |
| **PRODUCTION** | Cluster, gros volume | 10+ | 200 |

**Sélectionner un profil :**

```python
from config.spark_tuning import SparkTuningProfiles

profile = SparkTuningProfiles.get_profile("batch")  # ou "poc", "analytics", "production"

# Ou via env var
export SPARK_PROFILE=batch
```

---

## 3. Recommendations générales

### 3.1 Partitioning Strategy

**Recommandé :** Partitionnement multi-niveaux

```
Bronze/Silver:
  └─ tech_year=2026/
     └─ tech_month=2026-06/
        └─ tech_day=2026-06-21/
           └─ tech_hour=14/
           
Gold:
  └─ tech_year=2026/
     └─ tech_month=2026-06/  (coarser pour KPIs)
```

### 3.2 Secondary Partitioning

Si `skew_ratio > 3`, ajouter partitionnement secondaire :

```
Silver:
  └─ tech_year=2026/tech_month=06/
     └─ origin_continent=EU/  ← Secondary
        └─ on_ground=0/       ← Tertiary (très sélectif)
```

### 3.3 Broadcast Recommendations

**À broadcaster** (petites tables jointes souvent) :
- `dim_airlines` (~2K rows)
- `dim_airports` (~50K rows → peut être large)
- `dim_aircraft_models` (~3K rows)
- `dim_countries_continents` (~200 rows)

```python
from pyspark.sql.functions import broadcast

fact = spark.read.parquet(silver_fact_path)
airlines = spark.read.parquet(silver_dim_airlines_path)

joined = fact.join(broadcast(airlines), "airline_icao")
```

---

## 4. Performance Tuning Checklist

- [ ] Run `profile_partitions.py --layer bronze` → check skew_ratio
- [ ] Check if `skew_ratio > 3` → add secondary partitioning
- [ ] Analyze query patterns → match partitioning to KPI filters
- [ ] Select Spark profile → use "BATCH" for our ETL
- [ ] Run KPI queries with timing → measure baseline
- [ ] Apply recommendations → re-run queries
- [ ] Compare before/after → measure improvement %
- [ ] Document findings in optimization_report.json

---

## 5. Implementation Guide

### Step 1: Baseline Profile

```bash
python scripts/profile_partitions.py --layer bronze --output /tmp/baseline.json
```

**Check output :**
```json
{
  "skew_analysis": {
    "skew_ratio": 1.5,  // ✓ Good
    "partition_count": 365,
    "recommendation": "OK"
  },
  "size_analysis": {
    "total_size_mb_estimated": 5000,
    "avg_partition_size_mb": 13.7  // ✓ Good (< 1GB)
  },
  "spark_recommendations": {
    "recommended_shuffle_partitions": 150
  }
}
```

### Step 2: Apply Spark Tuning

```python
# In batch_job.py or config
export SPARK_PROFILE=batch

# spark_session.config("spark.sql.shuffle.partitions", "150")
# spark_session.config("spark.sql.adaptive.enabled", "true")
```

### Step 3: Profile KPI Queries

```python
from src.partitioning_optimizer import PartitioningOptimizer

optimizer = PartitioningOptimizer(spark, config)

# Time each KPI
for kpi_name, kpi_func in kpis.items():
    perf = optimizer.profile_query_performance(silver_df, kpi_name, kpi_func)
    print(f"{kpi_name}: {perf['elapsed_seconds']:.2f}s")
```

### Step 4: Apply Secondary Partitioning (if needed)

If `skew_ratio > 3`, add:

```python
df_repartitioned = df.repartition("tech_year", "tech_month", "origin_continent")
# Write with new partitioning
df_repartitioned.write.partitionBy(...).parquet(silver_path)
```

### Step 5: Measure Impact

```bash
# After optimization
python scripts/profile_partitions.py --layer silver --output /tmp/after.json

# Compare: skew_ratio, query times, partition sizes
```

---

## 6. Expected Results

**Before optimization :**
- Query time: 10-15s
- Partition skew: 2.5x
- Unused partitions scanned: yes

**After optimization :**
- Query time: 3-5s (3-5x faster)
- Partition skew: 1.2x
- Partition pruning: working
- Resource utilization: higher

---

## 7. Notes for Production

- **Monitor continuously** : skew can change as data grows
- **Repartition quarterly** : as data volume changes
- **Track query times** : set SLAs (KPI queries < 5s)
- **Auto-tuning** : Spark adaptive execution handles some of this
- **Document changes** : keep optimization_report.json in version control

---

## Files Created

✅ `src/partitioning_optimizer.py` (380 lines)
- `PartitioningOptimizer` class with 4 analysis methods
- `save_optimization_report()` helper

✅ `scripts/profile_partitions.py` (200 lines)
- CLI for profiling any layer
- Generates JSON report

✅ `config/spark_tuning.py` (150 lines)
- 4 profiles: POC, BATCH, ANALYTICS, PRODUCTION
- Partitioning + indexing + broadcast recommendations

---

## Next Steps

**Étape 6:** Logging & Monitoring (Prometheus/Grafana)
- Track partition skew metrics
- Alert if query time > SLA
- Monitor executor memory, CPU
