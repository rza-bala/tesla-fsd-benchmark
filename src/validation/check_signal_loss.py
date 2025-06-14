#!/usr/bin/env python3
"""
Compares signal retention and quality between two processing steps.
Example: Compare signal profiles of mf4_to_parquet vs. downsampled timeseries parquet.

Outputs: Prints lost signals, new signals, changes in null_fraction, and summary stats.
"""
import sys
import pandas as pd
from pathlib import Path

# Update paths to your actual profile CSV locations:
PROFILE1 = Path("data/catalog/parquet_signal_profiles.csv")  # Pre-downsample (decoded parquet)
PROFILE2 = Path("data/catalog/downsampled_signal_profiles.csv")  # Post-downsample

assert PROFILE1.exists(), f"Missing: {PROFILE1}"
assert PROFILE2.exists(), f"Missing: {PROFILE2}"

df1 = pd.read_csv(PROFILE1)
df2 = pd.read_csv(PROFILE2)

# For best matching, compare by (file, signal) combo:
key_cols = ["file", "signal"]

signals1 = set(tuple(row) for row in df1[key_cols].values)
signals2 = set(tuple(row) for row in df2[key_cols].values)

lost = signals1 - signals2
gained = signals2 - signals1
kept = signals1 & signals2

print("=== Signal Retention QC ===")
print(f"Step 1: {PROFILE1.name}: {len(signals1)} signals")
print(f"Step 2: {PROFILE2.name}: {len(signals2)} signals")
print(f"  • Signals lost:   {len(lost)}")
print(f"  • Signals gained: {len(gained)}")
print(f"  • Signals kept:   {len(kept)}")

if lost:
    print("\n-- LOST SIGNALS --")
    for file, signal in sorted(lost):
        print(f"{file:40}  {signal}")

if gained:
    print("\n-- NEW SIGNALS (unexpected) --")
    for file, signal in sorted(gained):
        print(f"{file:40}  {signal}")

# Compare null_fraction (missingness) and value ranges for kept signals
print("\n-- Signal QC Comparison for kept signals --")
merged = pd.merge(
    df1[key_cols + ["null_fraction", "min", "max"]],
    df2[key_cols + ["null_fraction", "min", "max"]],
    on=key_cols,
    suffixes=('_before', '_after'),
)

qc_stats = []
for _, row in merged.iterrows():
    nf1, nf2 = row["null_fraction_before"], row["null_fraction_after"]
    delta_null = nf2 - nf1
    qc_stats.append({
        "file": row["file"],
        "signal": row["signal"],
        "null_before": nf1,
        "null_after": nf2,
        "delta_null": delta_null,
        "min_before": row["min_before"],
        "min_after": row["min_after"],
        "max_before": row["max_before"],
        "max_after": row["max_after"],
    })

qc_df = pd.DataFrame(qc_stats)
# Show only signals where null fraction increased
delta_df = qc_df[qc_df["delta_null"] > 0]
print(f"\n{len(delta_df)} signals have increased null_fraction after downsampling:")
if not delta_df.empty:
    print(delta_df.sort_values("delta_null", ascending=False)[["file", "signal", "null_before", "null_after", "delta_null"]].head(20).to_string(index=False))

# Optionally: save this detailed comparison for audit
qc_df.to_csv("data/catalog/signal_qc_comparison.csv", index=False)
print("\nWrote detailed QC results to data/catalog/signal_qc_comparison.csv")
