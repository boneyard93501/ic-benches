# ============================================================================
# ic-benches Makefile
# ============================================================================
SHELL := /bin/bash
PYTHON := uv run python
ANSIBLE := ansible-playbook -i ansible/inventory.yml
DATA_DIR := data/s3-bench
REPORTS_DIR := reports/metrics

# ------------------------------- HELP ---------------------------------------
.PHONY: help
help: ## Show this help
	@echo ""
	@echo "ic-benches — available commands"
	@echo "--------------------------------------------"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?##' Makefile | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# --------------------------- ENV / INSTALL -----------------------------------
.PHONY: setup install
setup: ## Install deps (uv) and project in editable mode
	uv sync
	uv pip install -e .

install: ## Alias for setup
	$(MAKE) setup

# ------------------------------ TESTS ----------------------------------------
.PHONY: test coverage
test: ## Run integration tests
	uv run pytest -v tests/integration

coverage: ## Generate coverage report (htmlcov/)
	uv run pytest --cov=src --cov-report=html

# ---------------------------- BENCHMARKING -----------------------------------
.PHONY: data bench quick
data: ## Generate or verify dataset (idempotent)
	$(PYTHON) scripts/data_gen.py --config config.toml

bench: ## Run full benchmark (uses config.toml + .env)
	uv run ic-bench --provider impossible_cloud --config config.toml --env .env

quick: ## Run fast dev/test benchmark (--quick)
	uv run ic-bench --provider impossible_cloud --config config.toml --env .env --quick

# ------------------------------ METRICS --------------------------------------
.PHONY: metrics visualize unpack visualize-all
metrics: ## Build consolidated CSV from NDJSON (writes in $(DATA_DIR))
	$(PYTHON) -m metrics

visualize: ## Generate PNG plots to $(REPORTS_DIR)
	$(PYTHON) scripts/visualize_metrics.py \
		--input $(DATA_DIR)/consolidated_metrics.csv \
		--outdir $(REPORTS_DIR)

unpack: ## Unpack latest metrics tarball from ./metrics to ./metrics/extracted
	mkdir -p metrics/extracted
	latest=$$(ls -1t metrics/metrics_*.tar.gz 2>/dev/null | head -n1); \
	[ -n "$$latest" ] || { echo "No metrics tarball in ./metrics/"; exit 1; }; \
	echo "Unpacking $$latest ..."; \
	tar -xzf "$$latest" -C metrics/extracted

visualize-all: unpack ## Unpack latest tarball and then generate PNG plots
	$(PYTHON) scripts/visualize_metrics.py \
		--input metrics/extracted/data/s3-bench/consolidated_metrics.csv \
		--outdir $(REPORTS_DIR)

# ------------------------------ ANSIBLE --------------------------------------
.PHONY: deploy deploy-quick collect collect-local collect-s3 test-connection
deploy: ## Copy repo, install deps, run tests + full benchmark on VM
	$(ANSIBLE) ansible/deploy.yml

deploy-quick: ## Copy repo, install deps, run tests + QUICK benchmark on VM
	$(ANSIBLE) ansible/deploy.yml -e run_quick=true

collect: ## Collect metrics using defaults in collect.yml vars
	$(ANSIBLE) ansible/collect.yml

collect-local: ## Fetch metrics tarball to ./metrics (local)
	$(ANSIBLE) ansible/collect.yml -e 'collect_mode=local local_dest=./metrics'

# Override S3_* from CLI: make collect-s3 S3_BUCKET=... S3_PREFIX=... S3_REGION=...
S3_BUCKET ?= your-bucket
S3_PREFIX ?= ic-benches/results
S3_REGION ?= us-east-1
collect-s3: ## Upload metrics tarball to S3 (uses S3_* vars)
	$(ANSIBLE) ansible/collect.yml \
		-e "collect_mode=s3 s3_bucket=$(S3_BUCKET) s3_prefix=$(S3_PREFIX) s3_region=$(S3_REGION)"

test-connection: ## Verify SSH + Python on inventory hosts
	$(ANSIBLE) ansible/test_connection.yml

# ------------------------------ CLEANUP --------------------------------------
.PHONY: clean purge
clean: ## Remove caches/build artifacts
	rm -rf src/__pycache__ scripts/__pycache__ tests/__pycache__
	rm -rf src/ic_benches.egg-info htmlcov

purge: clean ## Also remove datasets and reports
	rm -rf $(DATA_DIR)* reports/metrics metrics
