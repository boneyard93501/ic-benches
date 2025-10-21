import json
import hashlib
from pathlib import Path
import tempfile
import pandas as pd
import pytest

from metrics_processor_full import MetricsProcessor  # adjust import path if placed under src/


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


@pytest.mark.integration
def test_metrics_processor_end_to_end(tmp_path: Path):
    # Arrange: build deterministic /data/s3-bench style tree in tmp
    data_root = tmp_path / "s3-bench"
    data_root.mkdir(parents=True, exist_ok=True)

    # Minimal manifest
    manifest = {
        "seed": 42,
        "total_size_gb": 1,
        "file_count": 10,
        "distribution": "fixed",
    }
    manifest_path = data_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    mhash = sha256_file(manifest_path)

    # Create three provider ndjson files with valid schema
    providers = ["impossible_cloud", "aws", "akave"]
    for p in providers:
        nd = data_root / f"{p}.ndjson"
        lines = [
            {
                "provider": p,
                "op": "PUT",
                "iteration": 1,
                "duration_ms": 8000.0,
                "bytes": 100 * 1024 * 1024,
                "exit_code": 0,
            },
            {
                "provider": p,
                "op": "GET",
                "iteration": 1,
                "duration_ms": 7800.0,
                "bytes": 100 * 1024 * 1024,
                "exit_code": 0,
            },
            {
                "provider": p,
                "op": "LIST",
                "iteration": 1,
                "duration_ms": 90.0,
                "bytes": 0,
                "exit_code": 0,
            },
        ]
        with open(nd, 'w') as f:
            for rec in lines:
                f.write(json.dumps(rec) + "\n")

    # Act
    mp = MetricsProcessor(data_path=str(data_root))
    result = mp.process_all()

    # Assert: consolidated exists and per-provider CSVs exist
    cons = Path(result["consolidated_csv"])
    assert cons.exists()
    for p in providers:
        csvp = data_root / f"metrics_{p}.csv"
        assert csvp.exists()
        df = pd.read_csv(csvp)
        # has required columns
        for col in ["p50_ms", "p95_ms", "p99_ms", "avg_ms", "samples", "error_rate", "provider"]:
            assert col in df.columns

    # manifest hash reported matches actual
    assert result["manifest_hash"] == mhash
