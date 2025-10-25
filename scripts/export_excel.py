#!/usr/bin/env python3
import argparse, pathlib, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--out", required=False)
args = ap.parse_args()

src = pathlib.Path(args.input).resolve()
if not src.exists():
    raise SystemExit(f"Missing file: {src}")

dst = pathlib.Path(args.out) if args.out else src.with_suffix(".xlsx")
df = pd.read_csv(src)
df.to_excel(dst, index=False)
print(f"✅ Excel file created: {dst}")
