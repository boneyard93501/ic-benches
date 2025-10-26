#!/usr/bin/env python3
"""
Improved visualizations from consolidated metrics (percent-aware).
- Better error-rate chart: grouped bars with value labels, sorted by worst provider.
- Adds a heatmap-style figure for quick scan.
- Keeps existing p95 latency and MB/s charts.

Inputs
  --input   Path to consolidated CSV (default: data/s3-bench/consolidated_metrics.csv)
  --outdir  Output directory for PNGs (default: reports/metrics)
  --dpi     Image DPI (default: 120)
"""
from pathlib import Path
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def _bar(ax, x, y, data, title, xlabel="", ylabel=""):
    groups = data.groupby(x)[y].mean().sort_index()
    ax.bar(groups.index, groups.values)
    ax.set_title(title)
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    ax.grid(True, axis="y", linestyle=":", linewidth=0.6)


def _grouped_bars_error(ax, df):
    """Grouped bar chart for error_rate_pct by provider/op.
    Providers sorted by mean error_rate_pct desc; value labels shown on bars.
    """
    # pivot to providers x ops
    piv = df.pivot_table(index="provider", columns="op", values="error_rate_pct", aggfunc="mean")
    # sort providers by their mean error rate (desc)
    piv["__mean__"] = piv.mean(axis=1)
    piv = piv.sort_values("__mean__", ascending=False).drop(columns=["__mean__"])

    providers = list(piv.index)
    ops = list(piv.columns)

    n_prov = len(providers)
    n_ops = max(1, len(ops))
    x = np.arange(n_prov)
    width = min(0.8 / n_ops, 0.2)  # keep readable clusters

    for i, op in enumerate(ops):
        vals = piv[op].fillna(0.0).values
        bars = ax.bar(x + i * width - (n_ops - 1) * width / 2, vals, width, label=str(op))
        # value labels
        for b, v in zip(bars, vals):
            if np.isnan(v):
                continue
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.5, f"{v:.1f}%", ha="center", va="bottom", fontsize=8, rotation=0)

    ax.set_xticks(x)
    ax.set_xticklabels(providers, rotation=30, ha="right")
    ax.set_ylabel("Error rate (%)")
    ax.set_title("Error Rate (%) by Provider and Operation")
    ax.set_ylim(0, max(5, float(np.nanmax(piv.values)) * 1.2))
    ax.grid(True, axis="y", linestyle=":", linewidth=0.6)
    ax.legend(title="op", ncols=min(4, n_ops), fontsize=8)


def _heatmap_error(ax, df):
    """Simple heatmap (providers x ops) for error_rate_pct."""
    piv = df.pivot_table(index="provider", columns="op", values="error_rate_pct", aggfunc="mean").fillna(0.0)
    im = ax.imshow(piv.values, aspect="auto")
    ax.set_xticks(range(piv.shape[1]))
    ax.set_xticklabels(piv.columns, rotation=30, ha="right")
    ax.set_yticks(range(piv.shape[0]))
    ax.set_yticklabels(piv.index)
    ax.set_title("Error Rate (%) Heatmap")
    ax.set_xlabel("Operation")
    ax.set_ylabel("Provider")
    ax.grid(False)
    # annotate cells
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            ax.text(j, i, f"{piv.values[i, j]:.1f}", ha="center", va="center", fontsize=7, color="white" if piv.values[i, j] > piv.values.max()/2 else "black")
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="%")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/s3-bench/consolidated_metrics.csv", help="Path to consolidated CSV")
    p.add_argument("--outdir", default="reports/metrics", help="Output directory for PNGs")
    p.add_argument("--dpi", type=int, default=120, help="Image DPI")
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input)
    required = {"provider", "op", "p95_ms", "MBps", "error_rate_pct"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in {args.input}: {sorted(missing)}")

    # 1) p95 latency by operation
    fig, ax = plt.subplots(figsize=(7, 4))
    _bar(ax, x="op", y="p95_ms", data=df, title="p95 Latency per Operation", ylabel="ms")
    fig.tight_layout(); fig.savefig(outdir / "p95_latency_by_op.png", dpi=args.dpi); plt.close(fig)

    # 2) average MB/s by provider
    fig, ax = plt.subplots(figsize=(7, 4))
    _bar(ax, x="provider", y="MBps", data=df, title="Average Throughput (MB/s) by Provider", ylabel="MB/s")
    fig.tight_layout(); fig.savefig(outdir / "mbps_by_provider.png", dpi=args.dpi); plt.close(fig)

    # 3) Improved error-rate grouped bars
    fig, ax = plt.subplots(figsize=(10, 5))
    _grouped_bars_error(ax, df)
    fig.tight_layout(); fig.savefig(outdir / "error_rate_by_provider_op.png", dpi=args.dpi); plt.close(fig)

    # 4) Supplementary heatmap
    fig, ax = plt.subplots(figsize=(8, 5))
    _heatmap_error(ax, df)
    fig.tight_layout(); fig.savefig(outdir / "error_rate_heatmap.png", dpi=args.dpi); plt.close(fig)

    print(f"Wrote figures to: {outdir}")


if __name__ == "__main__":
    main()
