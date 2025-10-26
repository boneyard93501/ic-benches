#!/usr/bin/env python3
"""Metrics processor — v1.6.2
Change: error rate is reported as percentage (0–100), column name 'error_rate_pct'.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import tomli


class MetricsProcessor:
    REQUIRED = {"provider", "op", "iteration", "duration_ms", "bytes", "exit_code"}

    def __init__(self, data_path: str | None = None, config_file: str = "config.toml"):
        if data_path is None:
            with open(config_file, "rb") as f:
                cfg = tomli.load(f)
            data_path = cfg["dataset"]["data_path"]
        self.data_path = Path(data_path)
        self.files = sorted(self.data_path.glob("*.ndjson"))
        self.manifest_path = self.data_path / "manifest.json"
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found at {self.manifest_path}")
        self.manifest_hash = self._sha256_file(self.manifest_path)

    def _sha256_file(self, path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _validate(self, rec: Dict[str, Any]) -> bool:
        return self.REQUIRED.issubset(rec.keys())

    def _read_ndjson(self, p: Path) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        with open(p, "r") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                rec = json.loads(s)
                if self._validate(rec):
                    rec["manifest_sha256"] = self.manifest_hash
                    rec.setdefault("provider", p.stem)
                    rows.append(rec)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
            df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce").fillna(0)
            df["exit_code"] = pd.to_numeric(df["exit_code"], errors="coerce").fillna(0)
            df["MBps"] = (df["bytes"] / 1e6) / (df["duration_ms"] / 1000.0)
        return df

    def _provider_csv(self, df: pd.DataFrame, provider: str) -> Path:
        g = df.groupby(["op", "iteration"])
        q = g["duration_ms"].describe(percentiles=[0.5, 0.95, 0.99])[ ["50%", "95%", "99%", "mean", "count"] ]
        q = q.rename(columns={"50%": "p50_ms", "95%": "p95_ms", "99%": "p99_ms", "mean": "avg_ms", "count": "samples"})
        # error rate in percent
        err_pct = df.groupby("op")["exit_code"].apply(lambda x: (x != 0).mean() * 100.0)
        out = q.join(err_pct, on="op").rename(columns={"exit_code": "error_rate_pct"})
        out["provider"] = provider
        csvp = self.data_path / f"metrics_{provider}.csv"
        out.to_csv(csvp)
        return csvp

    def process_all(self) -> Dict[str, Any]:
        if not self.files:
            raise FileNotFoundError(f"No NDJSON metrics in {self.data_path}")
        provider_csvs: List[Path] = []
        frames: List[pd.DataFrame] = []
        for f in self.files:
            df = self._read_ndjson(f)
            if df.empty:
                continue
            provider = df["provider"].iloc[0]
            provider_csvs.append(self._provider_csv(df, provider))
            frames.append(df)
        if not frames:
            raise RuntimeError("NDJSON present but contained no valid rows")
        all_df = pd.concat(frames, ignore_index=True)
        summary = all_df.groupby(["provider", "op"]).agg(
            p50_ms=("duration_ms", lambda x: x.quantile(0.5)),
            p95_ms=("duration_ms", lambda x: x.quantile(0.95)),
            p99_ms=("duration_ms", lambda x: x.quantile(0.99)),
            avg_ms=("duration_ms", "mean"),
            MBps=("MBps", "mean"),
            error_rate_pct=("exit_code", lambda x: (pd.to_numeric(x, errors='coerce') != 0).mean() * 100.0),
            samples=("duration_ms", "count"),
        ).reset_index()
        consolidated = self.data_path / "consolidated_metrics.csv"
        summary.to_csv(consolidated, index=False)
        return {
            "manifest_hash": self.manifest_hash,
            "provider_csvs": [str(p) for p in provider_csvs],
            "consolidated_csv": str(consolidated),
            "providers": sorted(all_df["provider"].unique().tolist()),
        }


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Aggregate S3 benchmark NDJSON metrics")
    ap.add_argument("--data-path", default=None, help="Dataset path (defaults to dataset.data_path in config.toml)")
    ap.add_argument("--config", default="config.toml", help="Path to config.toml (only used when --data-path is omitted)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    mp = MetricsProcessor(data_path=args.data_path, config_file=args.config)
    result = mp.process_all()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
