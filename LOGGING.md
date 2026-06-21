# Logging & Monitoring — Étape 6

**Date:** 2026-06-21  
**Status:** ✅ Système simple en place  
**Durée estimée:** 1 jour

---

## Objectif

Mettre en place un système de logging et de métriques simples pour :
- ✅ Logger les exécutions du job (librairie standard `logging`)
- ✅ Calculer les métriques de base (durée, validité, erreurs)
- ✅ Sauvegarder les métriques en JSON
- ✅ Visualiser via Streamlit dashboard

---

## 1. Logging Simple

### 1.1 Utilisation

```python
import logging

logger = logging.getLogger("pipeline")
logger.info("Job started")
logger.warning("Data quality < 70%")
logger.error("API timeout occurred")
```

Les logs sont écrits dans `datalake/_logs/pipeline.log`

---

## 2. Métriques du Job

### 2.1 Classe `JobMetrics`

Enregistre les métriques complètes du job ETL :

```python
from src.job_metrics import JobMetrics

batch_id = "20260621_141530"
metrics = JobMetrics(batch_id)

# Extraction
metrics.set_extraction(num_rows=1500, duration_seconds=45.2)

# Validation
metrics.set_validation(valid_rows=1178, invalid_rows=322)

# Analyse des données
metrics.set_analysis(on_ground_count=150, in_flight_count=1350)

# Dimensions
metrics.set_dimension("dim_airlines", unique_count=156)
metrics.set_dimension("dim_airports", unique_count=487)
metrics.set_dimension("dim_aircraft_models", unique_count=892)
metrics.set_dimension("dim_countries_continents", unique_count=198)

# Gold (KPIs)
metrics.set_gold(num_kpis=7, duration_seconds=12.5)
metrics.set_kpi_result("airline_volumes", num_rows=1)
metrics.set_kpi_result("continental_regional", num_rows=6)
metrics.set_kpi_result("longest_flight", num_rows=1)
metrics.set_kpi_result("continental_avg_distance", num_rows=6)
metrics.set_kpi_result("aircraft_manufacturers", num_rows=1)
metrics.set_kpi_result("airline_aircraft_top3", num_rows=15)
metrics.set_kpi_result("airport_imbalance", num_rows=1)

# Erreurs/warnings
metrics.add_error("api_timeout", "Timeout after 30s", phase="extraction")
metrics.add_warning("low_quality", "Data quality < 70%")

# Finaliser et sauvegarder
metrics.finalize()
metrics.save_to_json()
# → datalake/_logs/{batch_id}_metrics.json

# Afficher résumé en console
print(metrics.get_summary())
```

### 2.2 Structure des métriques

```json
{
  "batch_id": "20260621_141530",
  "start_time": "2026-06-21T14:15:30.123456",
  "end_time": "2026-06-21T14:17:15.654321",
  "total_duration_seconds": 105.5,
  "status": "success",
  "num_errors": 0,
  "num_warnings": 2,
  "extraction": {
    "rows": 1500,
    "duration_seconds": 45.2
  },
  "validation": {
    "valid_rows": 1178,
    "invalid_rows": 322,
    "pct_valid": 78.5
  },
  "analysis": {
    "on_ground_count": 150,
    "in_flight_count": 1350,
    "pct_in_flight": 90.0
  },
  "dimensions": {
    "dim_airlines": {"unique_count": 156},
    "dim_airports": {"unique_count": 487},
    "dim_aircraft_models": {"unique_count": 892},
    "dim_countries_continents": {"unique_count": 198}
  },
  "gold": {
    "kpis_computed": 7,
    "duration_seconds": 12.5,
    "kpi_airline_volumes": {"rows": 1},
    "kpi_continental_regional": {"rows": 6},
    "kpi_longest_flight": {"rows": 1},
    "kpi_continental_avg_distance": {"rows": 6},
    "kpi_aircraft_manufacturers": {"rows": 1},
    "kpi_airline_aircraft_top3": {"rows": 15},
    "kpi_airport_imbalance": {"rows": 1}
  },
  "errors": [],
  "warnings": [
    {
      "type": "low_quality",
      "message": "Data quality < 70%",
      "timestamp": "2026-06-21T14:17:00.000000"
    }
  ]
}
```

---

## 3. Dashboard Streamlit

### 3.1 Démarrage

```bash
pip install streamlit pandas
streamlit run dashboard.py
```

Accès : http://localhost:8501

### 3.2 Pages du dashboard

#### Page 1 : Last Execution

Affiche la dernière exécution :
- **KPIs** : Durée, Data Quality %, Erreurs, Avertissements
- **Extraction** : Nombre de vols, durée
- **Validation** : Rows valides/invalides, %
- **Gold** : KPIs calculés, durée
- **Errors/Warnings** : Détails des erreurs et avertissements

#### Page 2 : Execution History

Table historique de tous les batches :
- Batch ID, Status, Duration, Flights, Quality %, Errors, Warnings, Time
- **Charts** : Trend de durée, trend de qualité
- **Download** : Exporter en CSV

#### Page 3 : KPI Summary

Statistiques globales :
- Total batches, moyenne durée, moyenne qualité, total erreurs
- Distribution des status
- Distribution de qualité (min/max/trend)

---

## 4. Intégration dans batch_job.py

```python
from datetime import datetime
from src.job_metrics import JobMetrics
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('datalake/_logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("pipeline")

def run_batch(spark, config, zones=None):
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics = JobMetrics(batch_id)
    
    logger.info(f"Starting batch: {batch_id}")
    
    try:
        # Phase 1: Extraction
        start = time.time()
        flights_df, num_flights = extract_flights(...)
        extraction_duration = time.time() - start
        metrics.set_extraction(num_flights, extraction_duration)
        logger.info(f"Extracted {num_flights} flights")
        
        # Phase 2: Validation
        start = time.time()
        df_validated = validate_and_flag_flights(flights_df, ...)
        valid_count = df_validated.filter(col("is_valid")).count()
        invalid_count = num_flights - valid_count
        metrics.set_validation(valid_count, invalid_count)
        logger.info(f"Validation: {valid_count} valid, {invalid_count} invalid")
        
        # Analyse des données
        on_ground = df_validated.filter(col("on_ground") == 1).count()
        in_flight = valid_count - on_ground
        metrics.set_analysis(on_ground, in_flight)
        logger.info(f"Analysis: {in_flight} in flight, {on_ground} on ground")
        
        # Dimensions (compter uniques)
        unique_airlines = df_validated.select("airline_icao").distinct().count()
        unique_airports = df_validated.select("origin_iata", "destination_iata").rdd.flatMap(lambda x: x).distinct().count()
        unique_aircraft = df_validated.select("aircraft_code").distinct().count()
        
        metrics.set_dimension("dim_airlines", unique_airlines)
        metrics.set_dimension("dim_airports", unique_airports)
        metrics.set_dimension("dim_aircraft_models", unique_aircraft)
        metrics.set_dimension("dim_countries_continents", 195)  # ~195 pays dans le monde
        
        logger.info(f"Dimensions: {unique_airlines} airlines, {unique_airports} airports")
        
        # Phase 3: Load Bronze
        df_validated.write.parquet(bronze_path)
        logger.info(f"Loaded {num_flights} to Bronze")
        
        # Phase 4: Silver + Gold (optionnel)
        if config.LOAD_SILVER_GOLD:
            start = time.time()
            loader = SilverGoldLoader(spark, config)
            etl_result = loader.run_full_etl(bronze_path)
            gold_duration = time.time() - start
            
            metrics.set_gold(7, gold_duration)
            
            # Enregistrer les résultats de chaque KPI
            for kpi_name, kpi_df in etl_result['gold_kpis'].items():
                kpi_rows = kpi_df.count()
                metrics.set_kpi_result(kpi_name.replace("_", ""), kpi_rows)
            
            logger.info(f"Computed 7 KPIs in {gold_duration:.2f}s")
        
        # Finaliser
        metrics.finalize()
        metrics.save_to_json()
        
        # Afficher résumé
        print(metrics.get_summary())
        logger.info(f"✅ Batch completed: {metrics.metrics['status']}")
        
    except Exception as e:
        logger.error(f"Error in batch: {str(e)}", exc_info=True)
        metrics.add_error("batch_error", str(e))
        metrics.finalize()
        metrics.save_to_json()
        raise

    return True
```

---

## 5. Files created

✅ `src/job_metrics.py` (140 lignes)
- `JobMetrics` class pour enregistrer métriques simples
- `load_from_json()`, `load_all_metrics()` pour charger historique

✅ `dashboard.py` (320 lignes)
- Dashboard Streamlit multi-pages
- 3 pages : Last Execution, History, Summary
- Charts et tables interactives
- Export CSV

---

## 6. Directory structure

```
datalake/
  _logs/
    pipeline.log              # Logs généraux
    20260621_141530_metrics.json  # Métriques batch 1
    20260621_143000_metrics.json  # Métriques batch 2
    ...
```

---

## 7. Usage workflow

1. **Run batch job** :
   ```bash
   python scripts/run_batch.py
   ```

2. **Check logs** :
   ```bash
   tail -f datalake/_logs/pipeline.log
   ```

3. **Open dashboard** :
   ```bash
   streamlit run dashboard.py
   ```

4. **View metrics** :
   - Last execution details
   - History trends
   - Summary statistics

---

## 8. Expected output

**After batch execution :**

```
╔═══════════════════════════════════════════════════════╗
║ BATCH METRICS SUMMARY: 20260621_141530
╚═══════════════════════════════════════════════════════╝

📥 EXTRACTION:
  • Rows: 1500
  • Duration: 45.2s

✅ VALIDATION:
  • Valid: 1178
  • Invalid: 322
  • Valid %: 78.5%

📊 DATA ANALYSIS:
  • In Flight: 1350 (90.0%)
  • On Ground: 150

📋 DIMENSIONS:
  • Airlines: 156 unique
  • Airports: 487 unique
  • Aircraft Models: 892 unique
  • Countries: 198 unique

🎯 GOLD KPIs:
  • kpi_airline_volumes: 1 rows
  • kpi_continental_regional: 6 rows
  • kpi_longest_flight: 1 rows
  • kpi_continental_avg_distance: 6 rows
  • kpi_aircraft_manufacturers: 1 rows
  • kpi_airline_aircraft_top3: 15 rows
  • kpi_airport_imbalance: 1 rows

❌ ERRORS: 0
⚠️  WARNINGS: 0

⏱️  TOTAL DURATION: 125.3s
📊 STATUS: success
```

**In Streamlit Dashboard :**

*Last Execution page:*
- Duration: 125.3s
- Data Quality: 78.5%
- In Flight %: 90.0%
- Airlines: 156
- Airports: 487
- Aircraft Models: 892
- Countries: 198
- Errors: 0 ✅

*History page:*
- Table with: Batch ID, Status, Duration, Flights, Valid %, In Flight %, Airlines, Airports, Errors, Warnings
- Duration trend chart
- Data Quality trend chart

*Summary page:*
- Total Batches, Avg Duration, Avg Quality %, Avg In Flight %
- Avg Airlines, Avg Airports, Avg Aircraft, Avg Countries
- KPI Results table (avg rows per KPI)

---

**Next Steps:** Étape 7 (Job final + Scheduling)
