SHELL := /bin/bash
.DEFAULT_GOAL := help

# Paths
ANSIBLE_INV := ansible/inventory.yml
DEPLOY_PLAY := ansible/deploy.yml
COLLECT_PLAY := ansible/collect.yml

# Where archives may land (support historical & fixed locations)
SEARCH_DIRS := metrics ansible/metrics
EXTRACT_DIR := metrics/extracted
DATA_DIR := $(EXTRACT_DIR)/data/s3-bench
REPORTS_DIR := reports/metrics

# Find newest archive across both search dirs
LATEST_ARCHIVE := $(shell ls -1t $(foreach d,$(SEARCH_DIRS),$(wildcard $(d)/metrics_*.tar.gz)) 2>/dev/null | head -n1)

.PHONY: help
help:
	@echo "ic-benches — available commands"
	@echo "--------------------------------------------"
	@echo "  deploy               Copy repo, install deps, run tests + full benchmark on VM"
	@echo "  deploy-quick         Same as deploy with smaller dataset"
	@echo "  collect-local        Fetch newest metrics tarball to ./metrics"
	@echo "  unpack               Extract newest tarball into $(EXTRACT_DIR)"
	@echo "  metrics              NDJSON → CSV in $(DATA_DIR)"
	@echo "  visualize            Charts → $(REPORTS_DIR)"
	@echo "  visualize-all        collect + unpack + metrics + visualize"

.PHONY: deploy
deploy:
	ansible-playbook -i $(ANSIBLE_INV) $(DEPLOY_PLAY)

.PHONY: deploy-quick
deploy-quick:
	ansible-playbook -i $(ANSIBLE_INV) $(DEPLOY_PLAY) -e run_quick=true

.PHONY: collect-local
collect-local:
	ansible-playbook -i $(ANSIBLE_INV) $(COLLECT_PLAY)
	@ls -1t metrics/metrics_*.tar.gz 2>/dev/null | head -n3 | sed 's/^/Fetched: /' || true

.PHONY: unpack extract
unpack extract:
	@if [ -z "$(LATEST_ARCHIVE)" ]; then echo "No metrics archives found in: $(SEARCH_DIRS)"; exit 1; fi
	@mkdir -p $(EXTRACT_DIR)
	tar -xzf "$(LATEST_ARCHIVE)" -C $(EXTRACT_DIR)
	@echo "Extracted: $(LATEST_ARCHIVE) -> $(EXTRACT_DIR)"
	@find $(DATA_DIR) -maxdepth 1 -type f -name '*.ndjson' -print | sed 's/^/NDJSON: /' || true

.PHONY: metrics
metrics: unpack
	uv run python src/metrics.py --data-path $(DATA_DIR)
	@echo "Consolidated CSV: $(DATA_DIR)/consolidated_metrics.csv"

.PHONY: visualize
visualize: metrics
	uv run python scripts/visualize_metrics.py \
	  --input $(DATA_DIR)/consolidated_metrics.csv \
	  --outdir $(REPORTS_DIR)
	@ls -1 $(REPORTS_DIR)/*.png | sed 's/^/Plot: /'

.PHONY: visualize-all
visualize-all: collect-local unpack metrics visualize

.PHONY: excel
excel: metrics
	uv run python scripts/export_excel.py --input metrics/extracted/data/s3-bench/consolidated_metrics.csv
