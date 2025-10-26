#!/usr/bin/env python3
"""
Debug NDJSON metrics for failure patterns and error messages.

Usage (from repo root after `make collect-local && make unpack`):
  uv run python scripts/debug_ndjson_errors.py \
      --data metrics/extracted/data/s3-bench \
      [--provider ic-eu] [--top 20] [--out errors_summary.csv]

Reports:
  - Counts by (provider, op, exit_code)
  - Top-N distinct error messages (if 'error' field present)
  - Optional CSV summary with rows: provider,op,exit_code,count,error
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from collections import Counter, defaultdict


def load_records(data_dir: Path, provider_filter: str | None):
    records = []
    for p in sorted(data_dir.glob("*.ndjson")):
        if provider_filter and p.stem != provider_filter:
            continue
        for line in p.read_text().splitlines():
            s = line.strip()
            if not s:
                continue
            try:
                r = json.loads(s)
            except Exception:
                continue
            r.setdefault("provider", p.stem)
            r.setdefault("op", "?")
            r.setdefault("exit_code", 0)
            r.setdefault("error", "")
            records.append(r)
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Path to directory with *.ndjson files")
    ap.add_argument("--provider", default=None, help="Limit to a provider id (e.g., ic-eu)")
    ap.add_argument("--top", type=int, default=20, help="Top-N error messages to show")
    ap.add_argument("--out", default=None, help="Optional CSV path for summary rows")
    args = ap.parse_args()

    data_dir = Path(args.data)
    if not data_dir.exists():
        sys.exit(f"Not found: {data_dir}")

    recs = load_records(data_dir, args.provider)
    if not recs:
        sys.exit("No NDJSON records found.")

    # 1) Counts by (provider, op, exit_code)
    counts = Counter((r["provider"], r["op"], int(r["exit_code"])) for r in recs)
    print("\n== Counts by (provider, op, exit_code)")
    for (prov, op, code), n in sorted(counts.items()):
        print(f"{prov:20s} {op:6s} exit={code}: {n}")

    # 2) Top-N errors
    err_counts: dict[str, Counter] = defaultdict(Counter)
    for r in recs:
        if r.get("error"):
            err_counts[r["provider"]][r["error"]] += 1

    print("\n== Top error messages by provider")
    if not err_counts:
        print("(no 'error' field present in NDJSON; patch harness to record mc stderr)")
    for prov, c in err_counts.items():
        print(f"-- {prov}")
        for msg, n in c.most_common(args.top):
            print(f"  [{n:4d}] {msg}")

    # 3) Optional CSV export
    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", encoding="utf-8") as f:
            f.write("provider,op,exit_code,count,error\n")
            # For CSV, attach the most frequent error per (prov,op,code)
            # If no error messages are present, leave empty
            by_key = defaultdict(list)
            for r in recs:
                key = (r["provider"], r["op"], int(r["exit_code"]))
                by_key[key].append(r.get("error", ""))
            for (prov, op, code), n in counts.items():
                top_err = Counter([e for e in by_key[(prov, op, code)] if e]).most_common(1)
                msg = top_err[0][0] if top_err else ""
                # naive CSV escaping
                msg_esc = '"' + msg.replace('"', '""') + '"' if ',' in msg else msg
                f.write(f"{prov},{op},{code},{n},{msg_esc}\n")
        print(f"\n✅ Wrote summary CSV: {outp}")


if __name__ == "__main__":
    main()
