import os
import shutil
import json
from pathlib import Path
import pytest

from src.harness import S3Harness

MC = shutil.which("mc")

@pytest.mark.integration
@pytest.mark.skipif(MC is None, reason="mc CLI not available; real integration requires MinIO mc")
def test_harness_end_to_end(tmp_path: Path, monkeypatch):
    # Prepare .env with provider creds (replace with valid ones in CI)
    env_path = tmp_path / ".env"
    env_path.write_text("IC_ACCESS_KEY=changeme\nIC_SECRET_KEY=changeme\n")

    # Minimal config pointing to tmp data path
    cfg = tmp_path / "config.toml"
    data_dir = tmp_path / "data" / "s3-bench"
    data_dir.mkdir(parents=True)
    cfg.write_text(
        f"""
[dataset]
seed = 1
total_size_gb = 0.01
file_count = 1
size_distribution = "fixed"
min_file_size_mb = 1
max_file_size_mb = 1
directory_depth = 1
files_per_directory = 1
data_path = "{data_dir.as_posix()}"

[provider]
name = "impossible_cloud"
endpoint = "https://eu-central-2.impossibleapi.net"
region = "eu-central-2"
bucket_prefix = "ic-ci"
storage_class = "STANDARD"

[test]
iterations = 1
operations = ["PUT", "GET", "LIST", "HEAD", "DELETE"]
cleanup_after_run = true
warmup_operations = 0
verify_checksums = false
retry_attempts = 1
timeout_seconds = 60
"""
    )

    # Seed minimal dataset
    (data_dir / "manifest.json").write_text(json.dumps({"seed": 1}))
    (data_dir / "file.bin").write_bytes(b"x" * 1024 * 1024)

    h = S3Harness(config_file=str(cfg), env_file=str(env_path))
    h.setup()
    h.warmup()
    h.run()
    h.teardown()

    # Expect NDJSON written where harness logs
    assert h.metrics_path.exists()
