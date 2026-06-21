# Quick Test Guide

Exécuter les tests en 1 minute.

## Installation rapide
```bash
pip install -r requirements.txt
```

## Exécuter les tests
```bash
# Windows (PowerShell)
.\run_tests.ps1

# Windows (Command Prompt)
python -m pytest tests/ -v --tb=short -m "not slow"

# Linux/Mac
bash run_tests.sh
```

## Modes disponibles
```bash
# Unit tests only (~30 sec)
.\run_tests.ps1 -Mode unit

# Integration tests (~1 min)
.\run_tests.ps1 -Mode integration

# All (except slow E2E) (~2 min) ← default
.\run_tests.ps1

# With API (slow, ~10 min)
.\run_tests.ps1 -Mode e2e

# Coverage report
.\run_tests.ps1 -Mode coverage
```

## Comprendre les résultats

✅ **PASSED** = Test a réussi  
❌ **FAILED** = Erreur (à déboguer)  
⏭️ **SKIPPED** = Test ignoré (marqué comme slow, etc.)  

## Fichiers importants

- `pytest.ini` : Configuration pytest
- `tests/conftest.py` : Fixtures partagées
- `TESTS.md` : Documentation complète

## Ajouter de nouveaux tests

```python
# tests/unit/test_my_feature.py
def test_my_feature(spark_session):
    """Tester ma feature."""
    assert True
```

Puis :
```bash
python -m pytest tests/unit/test_my_feature.py -v
```
