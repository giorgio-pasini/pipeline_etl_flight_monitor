# Script pour exécuter les tests (Windows PowerShell)

param(
    [string]$Mode = "default"
)

Write-Host "======================================================================" -ForegroundColor Green
Write-Host "Test Suite — Pipeline ETL Trafic Aérien" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green

switch ($Mode) {
    "unit" {
        Write-Host "Running UNIT tests only..." -ForegroundColor Yellow
        & python -m pytest tests/unit/ -v --tb=short
    }
    "integration" {
        Write-Host "Running INTEGRATION tests..." -ForegroundColor Yellow
        & python -m pytest tests/integration/ -v --tb=short
    }
    "e2e" {
        Write-Host "Running E2E tests (with API, slow)..." -ForegroundColor Yellow
        & python -m pytest tests/e2e/ -v --tb=short -m slow
    }
    "all" {
        Write-Host "Running ALL tests..." -ForegroundColor Yellow
        & python -m pytest tests/ -v --tb=short
    }
    "coverage" {
        Write-Host "Running tests with coverage..." -ForegroundColor Yellow
        & python -m pytest tests/unit/ -v --cov=src --cov-report=html --tb=short
        Write-Host "Coverage report: htmlcov/index.html" -ForegroundColor Green
    }
    default {
        Write-Host "Running UNIT + INTEGRATION tests (default, no slow E2E)..." -ForegroundColor Yellow
        & python -m pytest tests/ -v --tb=short -m "not slow"
    }
}

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "✅ Test suite completed!" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
