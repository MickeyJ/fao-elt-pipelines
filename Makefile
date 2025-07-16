include .env
export

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#   	 Environment Variables
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-

# Check for environment and set activation command
ifdef VIRTUAL_ENV
    # Already in a virtual environment
    ACTIVATE = @echo "venv - $(VIRTUAL_ENV)" &&
    PYTHON = python
else ifdef CONDA_DEFAULT_ENV
    # Already in conda environment
    ACTIVATE = @echo "conda - $(CONDA_DEFAULT_ENV)" &&
    PYTHON = python
else ifeq ($(wildcard venv/Scripts/activate),venv/Scripts/activate)
    # Windows venv available
    ACTIVATE = @venv\Scripts\activate &&
    PYTHON = python
else ifeq ($(wildcard venv/bin/activate),venv/bin/activate)
    # Unix venv available
    ACTIVATE = @source venv/bin/activate &&
    PYTHON = python3
else
    # No environment found
    ACTIVATE = @echo "❌ No environment found. Run 'make venv' or activate conda." && exit 1 &&
    PYTHON = python
endif

.PHONY: all venv env-status install-init install install-update install-requirements \
pg-start pg-stop pg-shell pg-logs pipeline-full pipeline-quick validate-setup validate-pipeline \
test-dbt test-queries test-gold-layer dbt-run dbt-docs dbt-debug clean-cache reset-database \
reset-all help

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  			Python Environment
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
venv:
	@$(PYTHON) -m venv venv
	@echo "✅ Virtual environment created. Activate with:"
	@echo "   source venv/bin/activate  (macOS/Linux)"
	@echo "   venv\\Scripts\\activate     (Windows)"

env-status:
	@echo "=== Environment Status ==="
	$(ACTIVATE) echo "Python: $$(which $(PYTHON))"

# =-=-=--=-=-=-=-=-=-=
# Package Installation
# =-=-=--=-=-=-=-=-=-=
install-init:
	$(ACTIVATE) $(PYTHON) -m pip install pip-tools
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in --upgrade
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-update:
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in --upgrade
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-requirements:
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt


# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  	Docker / Postgres Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-

pg-start:
	docker-compose up -d 

pg-stop:
	docker compose down

pg-shell:
	docker exec -it fao_postgres psql -U $(LOCAL_DB_USER) -d $(LOCAL_DB_NAME)

pg-logs:
	docker-compose logs -f postgres

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Pipeline Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
pipeline-full:
	$(ACTIVATE) $(PYTHON) orchestration/elt_pipeline.py


# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Validation Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
validate-setup:
	$(ACTIVATE) $(PYTHON) scripts/validate_setup.py

validate-pipeline:
	$(ACTIVATE) $(PYTHON) verify_pipeline.py

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Test Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
test-dbt:
	cd dbt_project && dbt test --target dev

test-queries:
	docker exec -i fao_postgres psql -U $(LOCAL_DB_USER) -d $(LOCAL_DB_NAME) < sql/sample_queries.sql

test-gold-layer:
	docker exec fao_postgres psql -U $(LOCAL_DB_USER) -d $(LOCAL_DB_NAME) -c \
		"SELECT COUNT(*) as countries FROM gold.gold_country_metrics; \
		 SELECT COUNT(*) as commodities FROM gold.gold_price_production_analysis; \
		 SELECT COUNT(*) as regions FROM gold.gold_regional_summary;"

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		dbt Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
dbt-run:
	cd dbt_project && dbt run

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve

dbt-debug:
	cd dbt_project && dbt debug

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Cleanup/Reset Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
clean-cache:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	cd dbt_project && dbt clean

reset-database:
	docker exec fao_postgres psql -U $(LOCAL_DB_USER) -d $(LOCAL_DB_NAME) -c \
		"TRUNCATE bronze.raw_prices, bronze.raw_food_balance CASCADE;"
	@echo "✅ Database tables cleared"

reset-all: stop-postgres
	docker volume rm fao-elt-pipeline_postgres_data || true
	@echo "✅ Database volume removed"

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Database Access
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-


# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Help
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
help:
	@echo "FAO ELT Pipeline Commands"
	@echo "========================"
	@echo ""
	@echo "PIPELINES:"
	@echo "  make pipeline-full      - Run complete ELT pipeline"
	@echo "  make pipeline-quick     - Run pipeline with minimal data (1 page each)"
	@echo ""
	@echo "VALIDATIONS:"
	@echo "  make validate-setup     - Check if environment is properly configured"
	@echo "  make validate-pipeline  - Verify pipeline ran successfully"
	@echo ""
	@echo "TESTS:"
	@echo "  make test-dbt          - Run dbt data quality tests"
	@echo "  make test-queries      - Run SQL sample queries"
	@echo "  make test-gold-layer   - Quick check of gold layer tables"
	@echo ""
	@echo "DBT:"
	@echo "  make dbt-run           - Run dbt transformations only"
	@echo "  make dbt-docs          - Generate and serve dbt documentation"
	@echo "  make dbt-debug         - Debug dbt configuration"
	@echo ""
	@echo "CLEANUP/RESET:"
	@echo "  make clean-cache       - Remove Python cache and dbt artifacts"
	@echo "  make reset-database    - Clear all data from tables (keep schemas)"
	@echo "  make reset-all         - Stop postgres and delete volume (full reset)"
	@echo ""
	@echo "DATABASE ACCESS:"
	@echo "  make postgres-shell    - Open PostgreSQL interactive shell"
	@echo "  make postgres-logs     - View PostgreSQL container logs"
	@echo ""
	@echo "ENVIRONMENT:"
	@echo "  make use-local-db      - Switch to local database"
	@echo "  make use-remote-db     - Switch to remote database"
	@echo "  make start-postgres    - Start PostgreSQL container"
	@echo "  make stop-postgres     - Stop PostgreSQL container"

# Default target
all: help

