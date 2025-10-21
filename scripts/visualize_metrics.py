#!/usr/bin/env python3
"""
Generate basic visualizations from consolidated benchmark metrics.

Inputs:
  - CSV (default: data/s3-bench/consolidated_metrics.csv)

Outputs (default: reports/metrics/):
  - p95_latency_by_op.png
  - mbps_by_provider.png
  - error_rate_by_provider_op.png
"""
from pathlib import Path
import argparse
import pandas as pd
import matplotlib.pyplot as plt


def bar(ax, x, y, data, title, xlabel="", ylabel=""):
    groups = data.groupby(x)[y].mean()
    ax.bar(groups.index, groups.values)
    ax.set_title(title)
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    ax.grid(True, axis="y", linestyle=":", linewidth=0.6)


def bar_stacked(ax, data, x_col, y_col, hue_col, title):
    # pivot for stacked bars
    pivot = data.pivot_table(index=x_col, columns=hue_col, values=y_col, aggfunc="mean").fillna(0)
    bottom = None
    for col in pivot.columns:
        vals = pivot[col].values
        ax.bar(pivot.index, vals, bottom=bottom, label=str(col))
        bottom = vals if bottom is None else bottom + vals
    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.legend()
    ax.grid(True, axis="y", linestyle=":", linewidth=0.6)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/s3-bench/consolidated_metrics.csv", help="Path to consolidated CSV")
    p.add_argument("--outdir", default="reports/metrics", help="Output directory for PNGs")
    p.add_argument("--dpi", type=int, default=120, help="Image DPI")
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input)
    # Basic sanity: required columns
    required = {"provider", "op", "p95_ms", "MBps", "error_rate"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in {args.input}: {sorted(missing)}")

    # Figure 1: p95 latency by operation (averaged over providers)
    fig, ax = plt.subplots(figsize=(7, 4))
    bar(ax, x="op", y="p95_ms", data=df, title="p95 Latency per Operation", ylabel="ms")
    fig.tight_layout()
    fig.savefig(outdir / "p95_latency_by_op.png", dpi=args.dpi)
    plt.close(fig)

    # Figure 2: average MB/s by provider
    fig, ax = plt.subplots(figsize=(7, 4))
    bar(ax, x="provider", y="MBps", data=df, title="Average Throughput (MB/s) by Provider", ylabel="MB/s")
    fig.tight_layout()
    fig.savefig(outdir / "mbps_by_provider.png", dpi=args.dpi)
    plt.close(fig)

    # Figure 3: error rate by provider/operation (stacked by op within provider)
    fig, ax = plt.subplots(figsize=(9, 5))
    bar_stacked(ax, df, x_col="provider", y_col="error_rate", hue_col="op", title="Error Rate by Provider / Op")
    fig.tight_layout()
    fig.savefig(outdir / "error_rate_by_provider_op.png", dpi=args.dpi)
    plt.close(fig)

    print(f"Wrote figures to: {outdir}")


if __name__ == "__main__":
    main()
