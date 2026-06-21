# Job Final & Scheduling — Étape 7

**Date:** 2026-06-21  
**Status:** ✅ Job final opérationnel + scheduling  
**Durée estimée:** 1 jour

---

## Objectif

Créer le job ETL final qui exécute toutes les phases du pipeline et le planifier pour exécution automatique toutes les 2 heures.

---

## 1. Job Final Complet

### 1.1 Script : `scripts/run_job.py`

Exécute le pipeline complet en une seule commande :

```bash
# Exécution simple (Bronze seulement)
python scripts/run_job.py

# Avec Silver + Gold
python scripts/run_job.py --with-silver-gold

# Avec zones spécifiques
python scripts/run_job.py --zones europe northamerica asia

# Verbose (debug)
python scripts/run_job.py --verbose --with-silver-gold
```

### 1.2 Phases exécutées

```
1️⃣  Phase 1: Extraction
    • Appel API FlightRadarAPI
    • Conversion en DataFrame Spark
    • Enregistrer: rows, duration

2️⃣  Phase 2: Validation + Data Quality
    • Validate et flag flights
    • Calculer valid/invalid rows %
    • Calculer on_ground vs in_flight
    • Compter unique airlines, airports, aircraft
    • Enregistrer: quality stats, dimensions

3️⃣  Phase 3: Load Bronze
    • Write to Parquet partitionné
    • Enregistrer: duration

4️⃣  Phase 4: Silver + Gold (optionnel)
    • Transform Silver (dedup, enrich)
    • Compute 7 KPIs Gold
    • Enregistrer: KPI rows

5️⃣  Phase 5: Finaliser + Métriques
    • Sauvegarder JSON metrics
    • Afficher résumé console
    • Log complet
```

### 1.3 Résultat

```
✅ ETL Job completed successfully
   Batch ID: 20260621_141530
   Duration: 125.1s
   Status: success
   Errors: 0
   Warnings: 0
```

**Fichiers générés :**
```
datalake/
  _logs/
    pipeline.log                      # Logs complets
    20260621_141530_metrics.json      # Métriques JSON
  bronze/
    flights_raw/
      tech_year=2026/
        tech_month=06/
          tech_day=21/
            tech_hour=14/
              *.parquet              # Données
```

---

## 2. Scheduling

### 2.1 Linux/macOS — Cron

```bash
# 1. Rendre script exécutable
chmod +x scripts/schedule_job.sh

# 2. Installer cron job (every 2 hours)
./scripts/schedule_job.sh install

# 3. Vérifier installation
./scripts/schedule_job.sh list
# Output: 0 0,2,4,6,8,10,12,14,16,18,20,22 * * * ...

# 4. Tester
./scripts/schedule_job.sh test

# 5. Supprimer (si nécessaire)
./scripts/schedule_job.sh remove
```

**Cron Pattern :** `0 0,2,4,6,8,10,12,14,16,18,20,22 * * *`
- Exécution à : 00:00, 02:00, 04:00, ..., 22:00 (every 2 hours)
- 12 exécutions par jour

**Logs :** `datalake/_logs/cron_schedule.log`

### 2.2 Windows — Task Scheduler

```powershell
# 1. Ouvrir PowerShell AS ADMINISTRATOR
#    (Right-click PowerShell → Run as administrator)

# 2. Installer Task Scheduler job
.\scripts\schedule_job.ps1 -Action install

# 3. Vérifier installation
.\scripts\schedule_job.ps1 -Action list

# 4. Tester
.\scripts\schedule_job.ps1 -Action test

# 5. Supprimer (si nécessaire)
.\scripts\schedule_job.ps1 -Action remove
```

**GUI :**
```
Win+R → taskschd.msc
  Task Scheduler Library
  └─ Exalt
     └─ ETL
        └─ ETL-Pipeline-Job (Status: Ready)
```

**Schedule :** Every 2 hours (00:00, 02:00, 04:00, ..., 22:00)  
**Logs :** `datalake\_logs\scheduler.log`

---

## 3. Configuration & Options

### 3.1 Avec ou sans Silver/Gold

```bash
# Bronze seulement (rapide)
python scripts/run_job.py
# Duration: ~50s

# Avec Silver + Gold (complet)
python scripts/run_job.py --with-silver-gold
# Duration: ~120s
```

**Recommandation :** Utiliser `--with-silver-gold` en scheduling normal.

### 3.2 Zones

```bash
# Global (default)
python scripts/run_job.py

# Zones spécifiques
python scripts/run_job.py --zones europe northamerica

# Toutes zones
python scripts/run_job.py --zones global europe northamerica asia
```

### 3.3 Retry & Error Handling

Le job est **fault-tolerant** :
- Erreur API → enregistre warning, continue
- Données invalides → flaggées, pas cassées
- Transformation Silver/Gold échoue → logs warning, continue

**Statuts possibles :**
- `success` : Pas d'erreurs
- `warning` : Erreurs non-bloquantes (données manquantes, API timeout, etc.)
- `error` : Erreur critique (pas possible actuellement)

---

## 4. Monitoring

### 4.1 Logs

```bash
# Real-time
tail -f datalake/_logs/pipeline.log

# Metrics JSON
cat datalake/_logs/20260621_141530_metrics.json | jq '.'

# Dashboard
streamlit run dashboard.py
# http://localhost:8501
```

### 4.2 Alertes manuelles

Vérifier les métriques après exécution :

```python
from src.job_metrics import JobMetrics

metrics_list = JobMetrics.load_all_metrics()
latest = metrics_list[0]

# SLA checks
print(f"Duration: {latest['total_duration_seconds']}s (SLA: < 600s)")
print(f"Quality: {latest['validation']['pct_valid']}% (SLA: >= 70%)")
print(f"Errors: {latest['num_errors']} (SLA: 0)")
```

---

## 5. Architecture Scheduling

### 5.1 Setup complet

```
┌─────────────────────────────────────┐
│   Cron / Task Scheduler             │
│   (Every 2 hours)                   │
└──────────────┬──────────────────────┘
               │
               ↓
    ┌──────────────────────┐
    │  scripts/run_job.py  │
    └──────────────────────┘
               │
      ┌────────┼────────┬────────┬────────┐
      ↓        ↓        ↓        ↓        ↓
    Extract Validate Bronze Silver Gold
               │
               ↓
      ┌──────────────────┐
      │   Metrics JSON   │
      │  + Logs + Spark  │
      └──────────────────┘
               │
               ↓
      ┌──────────────────┐
      │   Dashboard      │
      │   (Streamlit)    │
      └──────────────────┘
```

### 5.2 Workflow quotidien

```
Day 1, 00:00  → Job #1 (extraction, validation, Bronze)
Day 1, 02:00  → Job #2
...
Day 1, 22:00  → Job #12
Day 2, 00:00  → Job #13 (new day, new day partition)
...
```

**Partitions créées :**
```
tech_year=2026/tech_month=06/tech_day=21/tech_hour=00/
tech_year=2026/tech_month=06/tech_day=21/tech_hour=02/
...
tech_year=2026/tech_month=06/tech_day=22/tech_hour=00/  (new day)
```

---

## 6. Troubleshooting

### 6.1 Job ne s'exécute pas

**Linux/macOS (cron) :**
```bash
# Vérifier si cron tourne
ps aux | grep cron

# Vérifier les logs cron
tail -f /var/log/syslog | grep CRON

# Test manuel
python scripts/run_job.py --verbose
```

**Windows (Task Scheduler) :**
```powershell
# Voir logs
Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" | Where-Object {$_.Message -like "*ETL*"}

# Test manual
python scripts\run_job.py --verbose
```

### 6.2 API timeout

Le job continue même en cas de timeout :

```json
{
  "warnings": [
    {
      "type": "api_timeout",
      "message": "FlightRadarAPI timeout after 30s",
      "timestamp": "2026-06-21T14:15:30.000000"
    }
  ],
  "status": "warning"
}
```

Vérifier dans dashboard.

### 6.3 Data quality bas

Si `pct_valid < 70%` → warning enregistré :

```json
{
  "warnings": [
    {
      "type": "low_quality",
      "message": "Data quality 65.3% < 70% threshold",
      "timestamp": "2026-06-21T14:15:30.000000"
    }
  ]
}
```

Vérifier dashboard → Summary → Quality Distribution.

---

## 7. Files Created

✅ `scripts/run_job.py` (500 lignes)
- Job complet orchestrant toutes phases
- Logging détaillé
- Métriques
- Gestion d'erreurs

✅ `scripts/schedule_job.sh` (Linux/macOS)
- Installer/lister/supprimer cron jobs
- Test manual

✅ `scripts/schedule_job.ps1` (Windows)
- Installer/lister/supprimer Task Scheduler jobs
- Test manual

✅ `SCHEDULING.md` (Documentation)
- Guide complet
- Troubleshooting
- Architecture

---

## 8. Next Steps

**Étape 8:** Amélioration Dashboard (optionnel)
- KPI detailing page
- Export reports
- Real-time updates

**Étape 9:** Fault-tolerance avancée
- Alertes Slack/email
- Auto-retry logic
- Recovery mechanisms

---

## Quick Start

```bash
# 1. Test job (1 execution)
python scripts/run_job.py --with-silver-gold

# 2. View metrics
streamlit run dashboard.py

# 3. Install scheduler (Linux/macOS)
chmod +x scripts/schedule_job.sh && ./scripts/schedule_job.sh install

# 3. Install scheduler (Windows - as Admin)
.\scripts\schedule_job.ps1 -Action install

# 4. Verify installation
./scripts/schedule_job.sh list        # Linux/macOS
.\scripts\schedule_job.ps1 -List      # Windows
```

**Status :** ✅ Job opérationnel et prêt pour production
