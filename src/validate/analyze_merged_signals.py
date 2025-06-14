#!/usr/bin/env python3
"""
Evaluates signal quality from merged/*.parquet files.
- Outputs quality report CSV and selected_signals.txt
"""

import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq

# ─── Paths ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MERGED_DIR = PROJECT_ROOT / "data" / "merged"
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
CATALOG_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = CATALOG_DIR / "merged_signal_quality.csv"
SELECTED_TXT = CATALOG_DIR / "selected_signals.txt"

# ─── Analyze Each Merged File ──────────────────────────────────
rows = []
for pq_path in sorted(MERGED_DIR.glob("*.parquet")):
    try:
        df = pd.read_parquet(pq_path)
        for col in df.columns:
            if col == "time":
                continue
            series = df[col]
            rows.append({
                "file": pq_path.name,
                "signal_name": col,
                "null_fraction": series.isna().mean(),
                "unique_values": series.nunique(dropna=True),
                "dtype": str(series.dtype),
                "min": series.min() if pd.api.types.is_numeric_dtype(series) else None,
                "max": series.max() if pd.api.types.is_numeric_dtype(series) else None,
                "mean": series.mean() if pd.api.types.is_numeric_dtype(series) else None,
            })
    except Exception as e:
        print(f"⚠️ Error reading {pq_path.name}: {e}")

df_quality = pd.DataFrame(rows)

# ─── Classify Quality ──────────────────────────────────────────
def assess_quality(row):
    if row["null_fraction"] > 0.10:
        return "too_null"
    if row["unique_values"] <= 1:
        return "constant"
    return "good"

df_quality["quality"] = df_quality.apply(assess_quality, axis=1)

# ─── Export ─────────────────────────────────────────────────────
df_quality.to_csv(OUTPUT_CSV, index=False)

selected = df_quality[df_quality["quality"] == "good"]["signal_name"].drop_duplicates()
selected.to_csv(SELECTED_TXT, index=False, header=False)

print(f"✅ Saved: {OUTPUT_CSV.name}")
print(f"✅ Saved: {SELECTED_TXT.name}")
