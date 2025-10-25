# ic-benches

**ic-benches** is a fully deterministic, multi‑provider S3 benchmarking harness built on **Python**, **MinIO’s mc client**, and **Ansible**. It is designed for enterprise‑grade reproducibility, reliability, and transparency, supporting IP and hostname endpoints, multiple provider configurations, and strict `.env`‑only namespaced credentials. The harness enables consistent dataset generation, stable test loops, NDJSON‑based result capture, and complete metric consolidation into CSV and visual reports.

---

## 1. Purpose and Overview

`ic-benches` provides deterministic, repeatable performance testing for S3‑compatible storage services. It removes variance from dataset creation, uses explicit configuration for every benchmark dimension, and automates deployment and collection through Ansible. The system integrates dataset generation, benchmark execution, metrics aggregation, and visualization into a single, reproducible lifecycle.

Each benchmark run:

1. Generates or reuses a deterministic dataset with manifest‑linked files.
2. Executes a parameterized test matrix over PUT, GET, LIST, HEAD, and DELETE operations.
3. Records timing, size, throughput, and exit status data in structured NDJSON.
4. Aggregates all results into per‑provider CSVs and a consolidated CSV summary.
5. Produces ready‑to‑use visualizations (latency, throughput, error rates) for reporting or CI pipelines.

The harness can target multiple S3 endpoints (e.g., Impossible Cloud, AWS S3, GCS, Akave) defined in a single configuration file with distinct namespaces and credentials.

---

## 2. Repository Structure

```
ansible/            # Infrastructure automation: deploy.yml, collect.yml, test_connection.yml
scripts/            # Helper tools: data_gen.py (dataset generation), visualize_metrics.py (plots)
src/                # Core implementation: cli.py, harness.py, credentials.py, metrics.py
config.toml         # Main configuration file (dataset, providers, test settings)
.env                # Namespaced environment credentials (exported automatically during deploy)
Makefile            # Complete automation entrypoint for local and remote runs
```

The project structure is intentionally flat and transparent, allowing reproducible builds and easy integration with CI/CD or research pipelines.

---

## 3. Configuration

Configuration is centralized in `config.toml` and separated into `[dataset]`, `[[providers]]`, and `[test]` sections. The file defines deterministic dataset parameters, multiple provider definitions, and benchmark control parameters.

### 3.1 Dataset Section

The dataset configuration controls the deterministic file generation process. Every file, directory, and checksum is fixed by the seed, ensuring reproducibility across runs.

```toml
[dataset]
seed = 42                     # master random seed for reproducibility
data_path = "./data/s3-bench" # root path for generated data and NDJSON output
```

Additional keys such as `total_size_gb`, `file_count`, and `size_distribution` can be supplied if extended dataset generation options are used.

### 3.2 Providers Section (namespaced)

Each provider definition describes one S3 endpoint to benchmark. Providers can be IP‑based or hostname‑based and must include an `id` and a unique `namespace`. The namespace determines which environment variable keys in `.env` will be read during deployment and execution.

```toml
[[providers]]
id = "ic-eu"                               # provider identifier used in CLI/Ansible
namespace = "IC_EU"                        # maps to .env variables (IC_EU_ACCESS_KEY / SECRET_KEY)
endpoint = "https://81.15.150.209"          # IP or hostname endpoint
region = "eu-east-1"                        # region or pseudo-region label
bucket = "pl-01"                            # target bucket name
insecure_ssl = true                          # disable TLS verification for IP endpoints
profile = ""                                # optional AWS shared credentials profile
```

**Environment variables (`.env`):**

```dotenv
IC_EU_ACCESS_KEY=75B3AE98BF4117041833
IC_EU_SECRET_KEY=df37b9ff6dca8462c2da8247be94d8cd5471a7df
# optional if required
IC_EU_SESSION_TOKEN=
```

If multiple providers are defined, each must use a distinct `namespace` (e.g., `IC_US`, `AWS_EAST`, `AKAVE_DEV`). The `.env` file should include matching credentials for each namespace.

### 3.3 Test Section

The test section defines the operational loop. Each operation type, iteration count, retry policy, and timeout is specified explicitly.

```toml
[test]
iterations = 3
operations = ["PUT", "GET", "LIST", "HEAD", "DELETE"]
cleanup_after_run = true
warmup_operations = 2
retry_attempts = 3
timeout_seconds = 300
```

* **iterations:** how many full loops to perform per operation.
* **operations:** sequence of S3 operations to benchmark.
* **cleanup_after_run:** whether to delete objects after completion.
* **warmup_operations:** number of non‑recorded dry runs to stabilize caches.
* **retry_attempts:** automatic retries per operation before marking failure.
* **timeout_seconds:** maximum time allowed for any operation.

---

## 4. Credential Model

`ic-benches` uses a strict, `.env`‑only credential model. No credentials are read from `config.toml`; instead, they are injected by the Ansible deploy process.

### Resolution order

1. **Explicit profile:** if `--profile` or `AWS_PROFILE` is provided, AWS shared credentials are loaded.
2. **Namespaced environment:** if the environment contains `<NAMESPACE>_ACCESS_KEY` and `<NAMESPACE>_SECRET_KEY`, they are used.
3. **AWS‑style fallback:** if no namespace match, falls back to `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

The `.env` file is copied to the remote system, loaded (`set -a; source .env; set +a`), and all variables exported for the benchmark step only. It is created with `0600` permissions and deleted when no longer needed.

---

## 5. Local Execution Workflow

A local run can be used for dry testing or development.

```bash
uv sync                     # install dependencies
pytest -v tests/integration # run integration tests
uv run ic-bench --config config.toml --provider ic-eu
```

The harness writes NDJSON to `data/s3-bench/ic-eu.ndjson` and metrics under the same directory. Local runs are ideal for debugging configuration or credentials before automation.

---

## 6. Remote Execution Workflow (Ansible)

The `make deploy` command invokes `ansible-playbook -i ansible/inventory.yml ansible/deploy.yml`. The playbook automates the full lifecycle:

1. Resolves absolute project root on the controller.
2. Ensures `/opt/ic-benches` exists on the remote VM and assigns ownership to the SSH user.
3. Rsyncs the repository from controller → remote (excluding `.git`, `.venv`, and build artifacts).
4. Installs prerequisites (`curl`, `tar`, `rsync`), `uv`, and MinIO `mc`.
5. Copies `config.toml` and `.env` to the remote path.
6. Runs `uv sync` and `pip install -e .` to set up the project.
7. Generates the dataset via `scripts/data_gen.py`.
8. Runs integration tests.
9. Executes the benchmark: `uv run ic-bench --config config.toml --provider <id>`.
10. Archives all resulting metrics under `.bench_archives/` with a timestamped filename.

Each benchmark step emits detailed logs to the Ansible console, and any failure preserves intermediate files for inspection.

---

## 7. CLI Interface

The command‑line entrypoint is defined in `pyproject.toml`:

```bash
uv run ic-bench --config config.toml --provider ic-eu
```

Options:

* `--config` path to configuration file.
* `--provider` provider id to benchmark.
* `--profile` AWS shared credentials profile (optional).
* `--quick` use reduced dataset and iterations for fast testing.

During execution:

1. The CLI loads the configuration and selects the specified provider.
2. The harness reads credentials from the active environment.
3. Each operation (PUT/GET/LIST/HEAD/DELETE) is executed in a deterministic loop.
4. Results are written in NDJSON line format.

Example NDJSON entry:

```json
{"provider":"ic-eu","op":"PUT","iteration":1,"duration_ms":8100.4,"bytes":104857600,"exit_code":0}
```

---

## 8. Metrics and Visualization Pipeline

The metrics subsystem consolidates NDJSON data into provider‑level and global summaries.

* **Raw NDJSON:** created by each benchmark run per provider.
* **Provider CSVs:** generated from NDJSON with descriptive statistics (p50/p95/p99/mean throughput/error rates).
* **Consolidated CSV:** merges all providers into one table (`consolidated_metrics.csv`).
* **Visualization:** `scripts/visualize_metrics.py` creates PNG plots:

  * `p95_latency_by_op.png`
  * `mbps_by_provider.png`
  * `error_rate_by_provider_op.png`

These outputs are stored under `reports/metrics/` for publication or further analysis.

---

## 9. Troubleshooting Guide

| Symptom                    | Cause                                     | Resolution                                                                                                 |
| -------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Missing credentials**    | Namespace mismatch or missing `.env` keys | Ensure `.env` contains `<NAMESPACE>_ACCESS_KEY` and `<NAMESPACE>_SECRET_KEY` matching provider `namespace` |
| **SSL handshake error**    | Using IP endpoints without proper cert    | Set `insecure_ssl = true` in provider block                                                                |
| **`mc` command not found** | Binary not installed or wrong path        | Ansible ensures `/usr/local/bin/mc` exists; verify PATH                                                    |
| **Manifest not found**     | Dataset missing or not generated          | Run `make data` or `scripts/data_gen.py --config config.toml` before deploy                                |
| **Permission denied**      | `.env` not readable by user               | Ensure local `.env` has proper permissions before sync                                                     |

---

## 10. Security and Compliance

* **Secrets isolation:** Credentials are stored only in `.env`, copied with `0600` mode, and removed after benchmark execution.
* **Transport security:** Set `insecure_ssl = false` for production endpoints with valid TLS certificates.
* **No embedded secrets:** All credentials are externalized; `config.toml` never stores keys.
* **Auditability:** Each benchmark run produces timestamped archives with deterministic manifests, ensuring verifiable reproducibility.

---

## 11. Multi‑Provider Operation

Multiple providers can be defined simultaneously. To run a specific provider:

```bash
make deploy RUN_PROVIDER=<id>
```

Or execute directly:

```bash
uv run ic-bench --config config.toml --provider <id>
```

Each run generates a separate NDJSON and metrics set per provider.

Example `.env` for two providers:

```dotenv
IC_EU_ACCESS_KEY=...
IC_EU_SECRET_KEY=...
IC_US_ACCESS_KEY=...
IC_US_SECRET_KEY=...
```

This allows parallel benchmarks or sequential runs for comparative performance analysis.

---

## 12. Makefile Targets

| Target               | Description                                    |
| -------------------- | ---------------------------------------------- |
| `make setup`         | Install dependencies and environment locally   |
| `make data`          | Generate or verify deterministic dataset       |
| `make test`          | Run integration tests                          |
| `make deploy`        | Execute full remote deployment and benchmark   |
| `make collect-local` | Fetch remote metrics archive into `./metrics/` |
| `make visualize`     | Generate latency/throughput/error plots        |
| `make clean`         | Remove caches and temporary files              |
| `make purge`         | Delete datasets and reports                    |

---

## 13. Example Outputs

```
data/s3-bench/
 ├── manifest.json
 ├── ic-eu.ndjson
 ├── metrics_ic-eu.csv
 └── consolidated_metrics.csv
reports/metrics/
 ├── p95_latency_by_op.png
 ├── mbps_by_provider.png
 └── error_rate_by_provider_op.png
```

---

## 14. Typical Runtime Characteristics

| Mode      | Dataset Size | Iterations  | Approx Duration |
| --------- | ------------ | ----------- | --------------- |
| `--quick` | ~20 MB       | 1           | ~1–2 minutes    |
| Default   | ~1 GB        | 3 + warmups | ~20–30 minutes  |

Benchmark timing depends on provider latency, parallelism, and dataset size but remains deterministic for a given configuration.

---

## 15. Version Notes

Version **1.4.4** introduces:

* Complete `.env`‑only namespace credential model (no `.secrets` files).
* Simplified, explicit credential precedence chain.
* Full workflow parity across local and remote execution.
* Enhanced metrics consistency and explicit cleanup.

---

