#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path
import click
import tomli

from harness import S3Harness


def _load_base_config(config_file: str) -> dict:
    with open(config_file, "rb") as f:
        return tomli.load(f)


def _write_quick_config(base: dict, quick_dir: Path) -> Path:
    """
    Create a lightweight config that exercises the full pipeline fast.
    - Keeps provider block identical to the base config
    - Shrinks dataset and test loops
    - Writes to ./data/s3-bench-quick (does not touch your main dataset)
    """
    quick_dir.mkdir(parents=True, exist_ok=True)
    data_path = "./data/s3-bench-quick"

    # Derive provider settings from base config
    p = base["provider"]
    provider_block = f"""[provider]
name = "{p['name']}"
endpoint = "{p['endpoint']}"
region = "{p['region']}"
bucket_prefix = "{p.get('bucket_prefix','ic-bench')}"
storage_class = "{p.get('storage_class','STANDARD')}"
"""

    # Lean dataset + tests that still hit PUT/LIST/HEAD/GET/DELETE
    dataset_block = f"""[dataset]
seed = 4242
total_size_gb = 0.02
file_count = 5
size_distribution = "fixed"
min_file_size_mb = 4
max_file_size_mb = 4
directory_depth = 1
files_per_directory = 5
data_path = "{data_path}"
"""

    test_block = """[test]
iterations = 1
operations = ["PUT","GET","LIST","HEAD","DELETE"]
cleanup_after_run = true
warmup_operations = 0
verify_checksums = true
retry_attempts = 3
timeout_seconds = 300
"""

    quick_cfg_text = "\n".join([dataset_block.strip(), provider_block.strip(), test_block.strip()]) + "\n"
    tmp_cfg = Path(".bench_tmp/quick_config.toml")
    tmp_cfg.parent.mkdir(exist_ok=True)
    tmp_cfg.write_text(quick_cfg_text)
    return tmp_cfg


def _generate_dataset(cfg_path: Path) -> None:
    # Use the existing generator to ensure manifest + files exist
    script = Path("scripts/data_gen.py")
    cmd = [sys.executable, str(script), "--config", str(cfg_path)]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Quick dataset generation failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )


@click.command()
@click.option("--provider", default=None, help="Provider override, e.g. impossible_cloud, aws, akave")
@click.option("--config", "config_file", default="config.toml", show_default=True, help="Path to config.toml")
@click.option("--env", "env_file", default=".env", show_default=True, help="Path to .env credentials file")
@click.option(
    "--quick",
    is_flag=True,
    default=False,
    help="Run a fast local/test pass: tiny dataset, 1 iteration, full operation set (NOT default).",
)
def main(provider: str | None, config_file: str, env_file: str, quick: bool) -> None:
    """
    Run the deterministic S3 benchmark harness end-to-end.

    Normal mode:
      - Uses config.toml as-is (full dataset and iterations)

    Quick mode (--quick):
      - Generates a small dataset in ./data/s3-bench-quick
      - Runs 1 iteration with all ops (PUT/LIST/HEAD/GET/DELETE)
      - Leaves your main dataset untouched
    """
    cfg_path = Path(config_file)

    if quick:
        base = _load_base_config(config_file)
        quick_cfg = _write_quick_config(base, Path("./data/s3-bench-quick"))
        _generate_dataset(quick_cfg)
        h = S3Harness(config_file=str(quick_cfg), env_file=env_file, provider_override=provider)
    else:
        # Normal path: trust the provided config (dataset must already exist or be generated separately)
        h = S3Harness(config_file=str(cfg_path), env_file=env_file, provider_override=provider)

    h.setup()
    h.warmup()
    h.run()
    h.teardown()


if __name__ == "__main__":
    main()
