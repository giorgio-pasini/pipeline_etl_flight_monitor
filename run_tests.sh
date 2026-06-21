#!/bin/bash
# Script pour exécuter les tests

set -e

echo "======================================================================"
echo "Test Suite — Pipeline ETL Trafic Aérien"
echo "======================================================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default: run unit + integration (not slow E2E)
TEST_MODE=${1:-"default"}

case $TEST_MODE in
  "unit")
    echo -e "${YELLOW}Running UNIT tests only...${NC}"
    pytest tests/unit/ -v --tb=short
    ;;
  "integration")
    echo -e "${YELLOW}Running INTEGRATION tests...${NC}"
    pytest tests/integration/ -v --tb=short
    ;;
  "e2e")
    echo -e "${YELLOW}Running E2E tests (with API, slow)...${NC}"
    pytest tests/e2e/ -v --tb=short -m slow
    ;;
  "all")
    echo -e "${YELLOW}Running ALL tests...${NC}"
    pytest tests/ -v --tb=short
    ;;
  "coverage")
    echo -e "${YELLOW}Running tests with coverage...${NC}"
    pytest tests/unit/ -v --cov=src --cov-report=html --tb=short
    echo -e "${GREEN}Coverage report: htmlcov/index.html${NC}"
    ;;
  "default"|*)
    echo -e "${YELLOW}Running UNIT + INTEGRATION tests (default, no slow E2E)...${NC}"
    pytest tests/ -v --tb=short -m "not slow"
    ;;
esac

echo ""
echo -e "${GREEN}======================================================================"
echo "✅ Test suite completed!"
echo "=====================================================================${NC}"
