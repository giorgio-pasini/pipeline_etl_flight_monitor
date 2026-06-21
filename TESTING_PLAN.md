# Plan de Test — ETL Trafic Aérien

**Date :** 2026-06-21  
**Status :** Suite de tests mise en place, prête pour validation  
**Approach :** Test-based development (TBD) — équilibré

---

## 📋 Résumé de la suite

### Structure
```
tests/
├── conftest.py                      # Fixtures (spark_session, temp_datalake, sample data)
├── unit/                            # 4 fichiers, ~20 tests
│   ├── test_schemas.py              # Validation schémas Spark ✅
│   ├── test_data_quality.py         # Flags et is_valid logic ✅
│   ├── test_flight_extraction.py    # Extraction + mock API ✅
│   └── test_datalake_utils.py       # Partitionnement, cleanup ✅
├── integration/                     # 1 fichier, ~5 tests
│   └── test_batch_job.py            # Workflow batch (mock API) ✅
└── e2e/                             # 1 fichier, ~3 tests
    └── test_e2e_batch.py            # Full cycle (API réelle, @slow) ✅
```

### Couverture
- **Unit tests :** 20 tests — composants critiques
- **Integration tests :** 5 tests — workflows batch
- **E2E tests :** 3 tests — cycle complet (marqués slow)
- **Total :** ~28 tests (léger, manageable)

---

## 🎯 Objectifs de chaque niveau

### Unit Tests (~20)
**Purpose :** Tester les composants individuels de manière isolée

| Test | Purpose | Mock ? |
|------|---------|--------|
| `test_schemas.py` | Validation StructType, colonnes, types | N/A |
| `test_data_quality.py` | Flags (MISSING_*, INVALID_*), is_valid | N/A |
| `test_flight_extraction.py` | Conversion Flight → dict → DataFrame | ✅ API |
| `test_datalake_utils.py` | Partitioning, cleanup, retention | ✅ Filesystem |

**Durée :** ~30 secondes  
**Dépendances :** Spark (local[2]), pytest, mock  
**CI/CD :** À chaque commit

---

### Integration Tests (~5)
**Purpose :** Tester les workflows batch (extract → validate → load)

| Test | Purpose | Mock ? |
|------|---------|--------|
| Session creation | Spark config | N/A |
| Batch with mock API | Extract 1 vol + flagging | ✅ API |
| Batch empty API | Extract 0 vols (graceful) | ✅ API |
| Batch + logging | Logs created | ✅ API |
| Batch fault-tolerance | Mixed valid/invalid | ✅ API |

**Durée :** ~1-2 minutes  
**Dépendances :** Spark, mock API, temp datalake  
**CI/CD :** À chaque commit (rapide)

---

### E2E Tests (~3)
**Purpose :** Tester le cycle complet avec l'API réelle

| Test | Purpose | Mock ? |
|------|---------|--------|
| Full cycle | API → Spark → Parquet Bronze | ❌ Real API |
| Quality reports | Rapports JSON générés | ❌ Real API |
| Idempotency | Run 2x = même résultat | ❌ Real API |

**Durée :** ~5-10 minutes (dépend API)  
**Dépendances :** API réelle, internet  
**CI/CD :** Nightly (slow)  
**Marqueur :** `@pytest.mark.slow`

---

## 🚀 Exécution

### Option 1 : PowerShell (Windows)
```powershell
# Unit + Integration (rapide)
.\run_tests.ps1

# Unit only
.\run_tests.ps1 -Mode unit

# With API (lent)
.\run_tests.ps1 -Mode e2e

# Coverage report
.\run_tests.ps1 -Mode coverage
```

### Option 2 : Bash (Linux/Mac)
```bash
# Unit + Integration
bash run_tests.sh

# Unit only
bash run_tests.sh unit

# With API
bash run_tests.sh e2e
```

### Option 3 : pytest directe
```bash
# Default (not slow)
pytest tests/ -v -m "not slow"

# Unit only
pytest tests/unit/ -v

# Integration
pytest tests/integration/ -v

# E2E (slow)
pytest tests/e2e/ -v -m slow

# All
pytest tests/ -v

# With coverage
pytest tests/unit/ --cov=src --cov-report=html
```

---

## ✅ Checklist de validation

### Installation
- [ ] `pip install -r requirements.txt`
- [ ] PySpark 3.5.0 OK
- [ ] pytest 7.4.0 OK
- [ ] pytest-mock OK

### Unit Tests
- [ ] `test_schemas.py` — tous passent
- [ ] `test_data_quality.py` — tous passent
- [ ] `test_flight_extraction.py` — tous passent (mock API)
- [ ] `test_datalake_utils.py` — tous passent

### Integration Tests
- [ ] `test_batch_job.py` — tous passent (mock API)
- [ ] Batch crée log files
- [ ] Batch gère gracieusement les erreurs

### E2E Tests (optionnel, slow)
- [ ] `test_e2e_batch.py` — peut être run avec `pytest tests/e2e/ -m slow`

---

## 🔍 Exemples de tests clés

### Unit : Validation schéma
```python
def test_schema_has_required_columns(self):
    """Les colonnes obligatoires doivent être présentes."""
    required = ['flight_id', 'callsign', 'airline_icao', ...]
    columns = [f.name for f in schema_flights_raw.fields]
    for col in required:
        assert col in columns
```

### Unit : Flagging de qualité
```python
def test_missing_origin_flag(self, spark_session, sample_flight_dict):
    """Un vol sans origine doit avoir le flag MISSING_ORIGIN."""
    sample_flight_dict['origin_iata'] = None
    df = spark_session.createDataFrame([sample_flight_dict.values()], ...)
    result = validate_and_flag_flights(df, logger=None)
    assert result.filter(col("data_quality_flags").isNotNull()).count() > 0
```

### Integration : Batch avec mock
```python
def test_batch_job_with_mock_api(self, spark_session, temp_datalake):
    """Tester extraction → validation → load."""
    with patch('src.flight_extraction.FlightRadar24API') as mock:
        mock_api.get_flights.return_value = [mock_flight]
        success = run_batch(spark_session, DatalakeConfig, logger)
        assert isinstance(success, bool)
```

### E2E : Cycle complet
```python
@pytest.mark.slow
def test_batch_job_full_cycle(self, temp_datalake):
    """Full cycle: API réelle → Spark → Parquet."""
    spark = create_spark_session(DatalakeConfig)
    success = run_batch(spark, DatalakeConfig, logger)
    assert isinstance(success, bool)
    spark.stop()
```

---

## 📊 Résultats attendus

Après exécution : `pytest tests/ -v -m "not slow"`

```
tests/unit/test_schemas.py::TestFlightsRawSchema::test_schema_exists PASSED
tests/unit/test_schemas.py::TestFlightsRawSchema::test_schema_has_required_columns PASSED
...
tests/integration/test_batch_job.py::TestBatchJobIntegration::test_spark_session_creation PASSED
...

========================= 25 passed, 3 skipped in 2.15s =========================
```

- ✅ 25 PASSED (unit + integration)
- ⏭️ 3 SKIPPED (E2E marked @slow)
- ❌ 0 FAILED

---

## 🔄 Prochaines itérations

Après validation des Étapes 1-3, ajouter des tests pour :

### Étape 4 (Silver + Gold)
```
tests/unit/test_transformations.py (~8 tests)
├── test_silver_loader.py (~6 tests)
├── test_gold_loader.py (~8 tests)
└── test_continent_mapping.py (~3 tests)
```

### Étape 6 (Logging)
```
tests/unit/test_prometheus_metrics.py (~5 tests)
```

### Étape 8 (Streamlit)
```
tests/unit/test_streamlit_dashboard.py (~6 tests)
```

---

## 🎓 Philosophy

✅ **Équilibré :** Unit + Integration + E2E, pas d'excès  
✅ **Mocké :** API mockée sauf pour E2E (pas de dépendances)  
✅ **Rapide :** Unit + Integration < 5 min (CI-friendly)  
✅ **Résilient :** Tests pour fault-tolerance et edge cases  
✅ **Évolutif :** Facile d'ajouter tests pour nouvelles phases  

---

## 📞 Support

**Question :** Pourquoi ~28 tests et pas plus ?  
**Réponse :** Couvrir les chemins critiques (schemas, flags, extraction, batch) sans over-engineering. Tests pour les vraies complexités, pas chaque ligne.

**Question :** Pourquoi mocker l'API ?  
**Réponse :** Unit + Integration doivent être rapides et fiables (pas dépendre d'internet). API réelle testée en E2E (slow).

**Question :** Comment ajouter un test ?  
**Réponse :** Créer `tests/unit/test_my_feature.py` avec fixtures de conftest.py, puis `pytest tests/unit/test_my_feature.py -v`.

---

**Status :** ✅ Suite prête pour validation  
**Prochaine étape :** Exécuter `pytest tests/ -v -m "not slow"` et continuer dev Étapes 4-9
