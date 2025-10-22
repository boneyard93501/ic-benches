# ============================================================================
# ic-benches Makefile
# ============================================================================
SHELL := /bin/bash
PYTHON := uv run python
ANSIBLE := ansible-playbook -i ansible/inventory.yml
DATA_DIR := data/s3-bench
REPORTS_DIR := reports/metrics

# ----------------------------------------------------------------------------
# Environment Setup
# ----------------------------------------------------------------------------
.PHONY: setup install
setup install:
	uv sync
	uv pip install -e .

# ----------------------------------------------------------------------------
# Tests & Coverage
# ----------------------------------------------------------------------------
.PHONY: test coverage
test:
	uv run pytest -v tests/integration

coverage:
	uv run pytest --cov=src --cov-report=html

# ----------------------------------------------------------------------------
# Benchmark
# ----------------------------------------------------------------------------
.PHONY: data bench quick
data:
	$(PYTHON) scripts/data_gen.py --config config.toml

bench:
	uv run ic-bench --provider impossible_cloud --config config.toml --env .env

quick:
	uv run ic-bench --provider impossible_cloud --config config.toml --env .env --quick

# ----------------------------------------------------------------------------
# Metrics
# ----------------------------------------------------------------------------
.PHONY: metrics visualize
metrics:
	$(PYTHON) -m metrics

visualize:
	$(PYTHON) scripts/visualize_metrics.py \
		--input $(DATA_DIR)/consolidated_metrics.csv \
		--outdir $(REPORTS_DIR)

# ----------------------------------------------------------------------------
# Ansible Automation
# ----------------------------------------------------------------------------
.PHONY: deploy collect collect-local collect-s3 test-connection
deploy:
	$(ANSIBLE) ansible/deploy.yml

collect:
	$(ANSIBLE) ansible/collect.yml

collect-local:
	$(ANSIBLE) ansible/collect.yml -e 'collect_mode=local local_dest=./metrics'

collect-s3:
	$(ANSIBLE) ansible/collect.yml \
		-e 'collect_mode=s3 s3_bucket=your-bucket s3_prefix=ic-benches/results s3_region=us-east-1'

test-connection:
	$(ANSIBLE) ansible/test_connection.yml

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------
.PHONY: clean purge
clean:
	rm -rf src/__pycache__ scripts/__pycache__ tests/__pycache__
	rm -rf src/ic_benches.egg-info htmlcov

purge: clean
	rm -rf $(DATA_DIR)* reports/metrics metrics

# ----------------------------------------------------------------------------
# Post-processing / Visualization
# ----------------------------------------------------------------------------
.PHONY: unpack visualize-all

# Unpack the latest metrics tarball to ./metrics/extracted
unpack:
	mkdir -p metrics/extracted
	latest=$$(ls -1t metrics/metrics_*.tar.gz | head -n1); \
	echo "Unpacking $$latest ..."; \
	tar -xzf $$latest -C metrics/extracted

# Unpack + visualize in one step
visualize-all: unpack
	$(PYTHON) scripts/visualize_metrics.py \
		--input metrics/extracted/data/s3-bench/consolidated_metrics.csv \
		--outdir reports/metrics
