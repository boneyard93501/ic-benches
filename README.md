# ic-benches

Production-grade S3 benchmarking harness using the **MinIO CLI (`mc`)**, **Python**, and **Ansible** for deterministic S3 performance testing across **Impossible Cloud** and other S3-compatible providers.

---

## Overview

`ic-benches` benchmarks S3-compatible storage with deterministic datasets and full reproducibility.
It runs controlled PUT/GET/LIST/HEAD/DELETE cycles and produces NDJSON + CSV metrics linked to dataset manifests.

---

## Key Features

* Deterministic dataset generation (`scripts/data_gen.py`)
* Config-driven provider setup (`config.toml`)
* Auto-verification with manifest SHA-256 linkage
* Stable bucket reuse with run-scoped prefixes
* NDJSON + consolidated CSV metrics
* Integrated **Ansible deploy/collect** automation
* **Fast testing** via `--quick`
* **Visualization** via `scripts/visualize_metrics.py`
* **Makefile** orchestration for all tasks

---

## Quick Start

### 1. Install Dependencies

```bash
uv sync
make setup
```

### 2. Configure Environment

```bash
cp .env.template .env
# Edit credentials
IC_ACCESS_KEY=...
IC_SECRET_KEY=...
```

### 3. Configure Dataset and Provider

Edit `config.toml` (example for Impossible Cloud EU-East-1):

```toml
[dataset]
seed = 42
total_size_gb = 1
file_count = 10
min_file_size_mb = 100
max_file_size_mb = 100
directory_depth = 1
files_per_directory = 10
data_path = "./data/s3-bench"

[provider]
name = "impossible_cloud"
endpoint = "https://eu-east-1.storage.impossibleapi.net"
region = "eu-east-1"
bucket_prefix = "pl-01"
storage_class = "STANDARD"

[test]
iterations = 3
operations = ["PUT","GET","LIST","HEAD","DELETE"]
cleanup_after_run = true
warmup_operations = 2
verify_checksums = true
retry_attempts = 3
timeout_seconds = 300
```

---

## Makefile Commands

The Makefile automates all major operations.

### Environment & Tests

```bash
make setup         # install dependencies
make test           # run integration tests
make coverage       # generate coverage report (htmlcov/)
```

### Benchmark Lifecycle

```bash
make data           # generate dataset (idempotent)
make bench          # full benchmark run
make quick          # fast test run (--quick)
make metrics        # aggregate metrics into CSVs
make visualize      # generate PNG plots (reports/metrics)
```

### Ansible Automation

```bash
make deploy         # copy repo, install deps, run tests + benchmark
make collect-local  # fetch metrics locally (./metrics/)
make collect-s3     # upload metrics tarball to S3
make test-connection# verify SSH + Python connectivity
```

### Cleanup

```bash
make clean          # remove caches and build artifacts
make purge          # also delete datasets and reports
```

---

## Visualization Example

After running `make visualize`, you’ll find:

```
reports/metrics/
 ├── p95_latency_by_op.png
 ├── mbps_by_provider.png
 └── error_rate_by_provider_op.png
```

---

## Ansible Inventory Example

`ansible/inventory.yml`

```yaml
all:
  hosts:
    bench:
      ansible_host: 203.0.113.10
      ansible_user: ubuntu
      ansible_ssh_private_key_file: ~/.ssh/id_rsa
```

---

## Typical Runtime

| Mode      | Dataset | Iterations  | Time       |
| --------- | ------- | ----------- | ---------- |
| `--quick` | ~20 MB  | 1           | ~1–2 min   |
| Default   | ~1 GB   | 3 + warmups | ~20–30 min |

---

## Output Structure

```
data/s3-bench/
 ├── manifest.json
 ├── impossible_cloud.ndjson
 ├── metrics_impossible_cloud.csv
 └── consolidated_metrics.csv
reports/metrics/
 ├── p95_latency_by_op.png
 ├── mbps_by_provider.png
 └── error_rate_by_provider_op.png
```

