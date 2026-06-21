# Test Suite — Pipeline ETL Trafic Aérien

**Date :** 2026-06-21  
**Approche :** Équilibrée — Unit + Integration + E2E, sans over-engineering

---

## Vue d'ensemble

La suite de tests couvre les trois niveaux :

| Type | Focus | Fichiers | Count |
|------|-------|----------|-------|
| **Unit** | Composants individuels | `test_schemas.py`, `test_data_quality.py`, `test_flight_extraction.py`, `test_datalake_utils.py` | ~20 tests |
| **Integration** | Workflows (extraction → validation → load) | `test_batch_job.py` | ~5 tests |
| **E2E** | Cycle complet avec API réelle | `test_e2e_batch.py` | ~3 tests |

**Total :** ~28 tests (léger, manageable)

---

## Structure

```
tests/
├── conftest.py                      # Fixtures partagées
├── unit/
│   ├── test_schemas.py              # Validation des schémas Spark
│   ├── test_data_quality.py         # Flagging et is_valid logic
│   ├── test_flight_extraction.py    # Extraction + mock API
│   └── test_datalake_utils.py       # Partitionnement, cleanup
├── integration/
│   └── test_batch_job.py            # Batch complet (mock API)
└── e2e/
    └── test_e2e_batch.py            # Full cycle (API réelle)

pytest.ini                           # Configuration pytest
```

---

## Exécution

### Tous les tests unitaires (rapide, ~1-2 min)
```bash
pytest tests/unit/ -v
```

### Tests d'intégration (moyen, ~2-3 min)
```bash
pytest tests/integration/ -v
```

### Tests E2E complets (lent, ~5-10 min, nécessite l'API)
```bash
pytest tests/e2e/ -v -m slow
```

### Tous les tests (sauf E2E lents)
```bash
pytest tests/ -v -m "not slow"
```

### Coverage
```bash
pytest tests/unit/ --cov=src --cov-report=html
open htmlcov/index.html
```

---

## Unit Tests

### `test_schemas.py` (~8 tests)

**Objectif :** Valider que les schémas Spark sont corrects

- ✅ Schema existe
- ✅ Colonnes obligatoires présentes
- ✅ Types de champs corrects
- ✅ Création de DataFrame possible
- ✅ Schémas des dimensions (airlines, airports, fact)
- ✅ Registry de schémas (SCHEMAS dict)

**Exemple :**
```python
def test_schema_has_required_columns(self):
    """Le schéma doit contenir les colonnes obligatoires."""
    required_cols = ['flight_id', 'callsign', 'airline_icao', ...]
    schema_cols = [field.name for field in schema_flights_raw.fields]
    for col in required_cols:
        assert col in schema_cols
```

---

### `test_data_quality.py` (~8 tests)

**Objectif :** Valider la logique de flagging et is_valid

- ✅ Vol valide : aucun flag
- ✅ Vol sans origine : flag MISSING_ORIGIN
- ✅ Vol avec altitude invalide : flag INVALID_ALTITUDE
- ✅ is_valid = True seulement si aucun flag
- ✅ Profil de qualité retourne un dict
- ✅ Profile compte les vols valides/invalides

**Exemple :**
```python
def test_missing_origin_flag(self, spark_session, sample_flight_dict):
    """Un vol sans origine doit avoir le flag MISSING_ORIGIN."""
    sample_flight_dict['origin_iata'] = None
    df = spark_session.createDataFrame([sample_flight_dict.values()], ...)
    result = validate_and_flag_flights(df, logger=None)
    assert result.filter(col("data_quality_flags").isNotNull()).count() > 0
```

---

### `test_flight_extraction.py` (~3 tests)

**Objectif :** Valider l'extraction et conversion (avec API mockée)

- ✅ Extracteur s'initialise correctement
- ✅ Conversion Flight objects → dicts
- ✅ Création de DataFrame Spark
- ✅ Gestion de listes vides

**Exemple :**
```python
def test_flights_to_dicts_conversion(self):
    """Convertir Flight objects en dicts plats."""
    with patch('src.flight_extraction.FlightRadar24API'):
        extractor = FlightExtractor()
        mock_flight = Mock()
        mock_flight.callsign = "DLH123"
        dicts = extractor.flights_to_dicts([mock_flight])
        assert isinstance(dicts[0], dict)
```

---

### `test_datalake_utils.py` (~4 tests)

**Objectif :** Valider partitionnement et cleanup

- ✅ Extraction des valeurs de partition
- ✅ Format des valeurs (année, mois, jour, heure)
- ✅ Construction du chemin partitionné
- ✅ Parsing d'un chemin
- ✅ Cleanup en dry-run (ne supprime pas)
- ✅ Listing des partitions

**Exemple :**
```python
def test_partition_values_format(self):
    """Les valeurs de partition doivent avoir le bon format."""
    now = datetime(2026, 6, 21, 14, 30, 0)
    values = get_partition_values(now)
    assert values['tech_year'] == '2026'
    assert values['tech_day'] == '2026-06-21'
```

---

## Integration Tests

### `test_batch_job.py` (~5 tests)

**Objectif :** Valider le workflow batch complet

- ✅ Création de session Spark
- ✅ Batch job avec API mockée (complet)
- ✅ Batch job avec API vide (graceful)
- ✅ Batch job crée des logs
- ✅ Batch continue avec données mixtes (fault-tolerance)

**Exemple :**
```python
def test_batch_job_with_mock_api(self, spark_session, temp_datalake):
    """Tester le batch job complet avec API mockée."""
    with patch('src.flight_extraction.FlightRadar24API') as mock_api_class:
        mock_api = Mock()
        mock_api.get_flights.return_value = [mock_flight]  # 1 vol
        success = run_batch(spark_session, DatalakeConfig, logger, zones=["global"])
        assert isinstance(success, bool)
```

---

## End-to-End Tests

### `test_e2e_batch.py` (~3 tests)

**Objectif :** Valider le cycle complet avec l'API réelle

- ✅ Batch job complet : extraction réelle → validation → Bronze write
- ✅ Batch crée les rapports de qualité
- ✅ Batch est idempotent (peut être rejouée)

**Marqué avec `@pytest.mark.slow` — à exécuter séparément si besoin**

---

## Fixtures

### `conftest.py`

**Fixtures partagées :**

```python
@pytest.fixture(scope="session")
def spark_session():
    """Session Spark pour tous les tests."""
    # Local[2], 1g driver memory

@pytest.fixture(scope="function")
def temp_datalake(tmp_path):
    """Datalake temporaire par test."""
    # Bronze, Silver, Gold dirs créés

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

---

## Couverture

**Cible :** ~70-80% pour le code critique, pas 100%

| Module | Critique | Coverage |
|--------|----------|----------|
| `src/schemas.py` | ✅ Oui | ~90% |
| `src/data_quality.py` | ✅ Oui | ~85% |
| `src/flight_extraction.py` | ✅ Oui | ~75% |
| `src/datalake_utils.py` | ✅ Oui | ~80% |
| `src/batch_job.py` | ✅ Oui (integration) | ~70% |
| `config/datalake_config.py` | ❌ Config | ~0% |

**Pas testé :** Code de configuration, détails Spark, interfaces externes

---

## CI/CD

**Recommandation pour Airflow / GitHub Actions :**

```bash
# Pre-commit
pytest tests/unit/ -v -m "not slow"

# Pre-merge
pytest tests/ -v -m "not slow" --cov=src

# Nightly (avec E2E lents)
pytest tests/ -v
```

---

## Points clés

✅ **Équilibré** : 20 unit + 5 integration + 3 E2E = 28 tests (manageable)  
✅ **Mocké** : API mockée sauf pour E2E (pas de dépendances réelles)  
✅ **Isolé** : Chaque test utilise un datalake temporaire  
✅ **Rapide** : Unit + Integration < 5 min ; E2E ~5-10 min  
✅ **Résilient** : Tests pour fault-tolerance (données mixtes, API vide)  

---

## Prochaines étapes

1. **Étape 4** : Ajouter tests pour Silver + Gold transformations
2. **Étape 6** : Ajouter tests pour Prometheus metrics
3. **Étape 8** : Ajouter tests pour Streamlit dashboard

---

**Status :** ✅ Suite de tests en place et équilibrée  
**Prêt pour :** Développement des Étapes 4-9 avec confiance
