#!/usr/bin/env python3
"""
Compares signal columns across pipeline stages:
- decoded → downsampled
- downsampled → processed
- processed → merged

Generates:
- data/catalog/signal_diff_report.csv

Each row includes: file name, stage pair, # dropped, # added, dropped signals, etc.
"""

import pandas as pd
from pathlib import Path
import logging

# ─── Path Setup ─────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
CATALOG_DIR = DATA_DIR / "catalog"
CATALOG_DIR.mkdir(parents=True, exist_ok=True)

STAGES = ["decoded", "downsampled", "processed", "merged"]
OUTPUT_CSV = CATALOG_DIR / "signal_diff_report.csv"

# ─── Logging Setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)

# ─── Compare Two Parquet Files ─────────────────────────────────
def compare_files(before_path: Path, after_path: Path, stage_pair: str):
    try:
        before_cols = pd.read_parquet(before_path, nrows=1).columns.drop("time", errors="ignore")
        after_cols = pd.read_parquet(after_path, nrows=1).columns.drop("time", errors="ignore")

        dropped = sorted(set(before_cols) - set(after_cols))
        added = sorted(set(after_cols) - set(before_cols))
        kept = sorted(set(before_cols).intersection(after_cols))

        return {
            "file_stem": before_path.stem,
            "stage_pair": stage_pair,
            "before_signals": len(before_cols),
            "after_signals": len(after_cols),
            "kept": len(kept),
            "dropped": len(dropped),
            "added": len(added),
            "dropped_signals": "; ".join(dropped),
            "added_signals": "; ".join(added),
        }
    except Exception as e:
        logging.error(f"❌ Failed comparison {before_path.name} → {after_path.name}: {e}")
        return None

# ─── Compare All Pairs ─────────────────────────────────────────
records = []

for i in range(len(STAGES) - 1):
    before_stage = STAGES[i]
    after_stage = STAGES[i + 1]
    stage_pair = f"{before_stage} → {after_stage}"

    before_dir = DATA_DIR / before_stage
    after_dir = DATA_DIR / after_stage

    before_files = sorted(before_dir.glob("*.parquet"))

    for before_path in before_files:
        expected_after = after_dir / f"{before_path.stem}_filtered.parquet" \
            if "processed" in after_stage else \
            after_dir / f"{before_path.stem}_downsampled.parquet" \
            if "downsampled" in after_stage else \
            after_dir / f"{before_path.stem}.parquet"

        if not expected_after.exists():
            logging.warning(f"⚠️ Missing {stage_pair} match for {before_path.name}")
            continue

        result = compare_files(before_path, expected_after, stage_pair)
        if result:
            records.append(result)
            logging.info(f"✅ Compared: {before_path.name} → {expected_after.name} [Dropped: {result['dropped']}, Added: {result['added']}]")

# ─── Save Final CSV ────────────────────────────────────────────
if records:
    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"📊 Saved signal diff report: {OUTPUT_CSV}")
else:
    logging.warning("⚠️ No comparisons completed.")
