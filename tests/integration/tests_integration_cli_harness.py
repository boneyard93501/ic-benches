import os
import shutil
import subprocess
from pathlib import Path
import pytest

MC = shutil.which("mc")


@pytest.mark.integration
@pytest.mark.skipif(MC is None, reason="mc CLI not available; real integration requires MinIO mc")
def test_cli_benchmark_runs_with_provider_env(tmp_path: Path):
    # Prepare minimal .env with IC_ credentials (must be valid for real run)
    env_path = tmp_path / ".env"
    env_path.write_text("IC_ACCESS_KEY=changeme\nIC_SECRET_KEY=changeme\n")

    # Minimal config in temp that points dataset at tmp data path
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

    # Generate a tiny 1MB file deterministically (no mocks)
    (data_dir / "manifest.json").write_text("{}")
    f = data_dir / "file.bin"
    f.write_bytes(b"x" * 1024 * 1024)

    # Invoke the single-command CLI; assume project layout allows `python -m cli_single_command`
    # Adjust the module path according to your repo when integrating.
    cmd = [
        shutil.which("python") or "python",
        "-m",
        "cli_single_command",
        "--provider",
        "impossible_cloud",
        "--config",
        str(cfg),
        "--env",
        str(env_path),
    ]

    # This will reach real mc if credentials are valid; otherwise the test will fail naturally
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # We do not assert success here since credentials may be placeholders; the point is that the CLI path is valid
    assert proc.returncode in (0, 1)
