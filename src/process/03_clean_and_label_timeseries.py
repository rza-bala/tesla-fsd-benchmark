#!/usr/bin/env python3
"""
Cleans and labels downsampled time series:
- Drops weak signals (>10% nulls or <2 unique if not enum)
- Applies enum mapping using enum_maps.json
- Saves filtered Parquet files to data/processed/
- Generates signal cleaning summary report
"""
import sys
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path


# ─── Force src/ to be discoverable ─────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import DOWNSAMPLED_DIR, PROCESSED_DIR, REGISTRY_DIR, ENUM_MAPS_PATH


PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ─── Config ─────────────────────────────────────────────────────
MAX_NULL_FRAC = 0.10
MIN_UNIQUE_NON_ENUM = 2

# ─── Logging Setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# ─── Load Enum Metadata ─────────────────────────────────────────
with open(ENUM_MAPS_PATH) as f:
    enum_map = json.load(f)
enum_signals = set(enum_map.keys())

# ─── Signal Filtering ───────────────────────────────────────────
def filter_signals(df: pd.DataFrame, record: dict) -> pd.DataFrame:
    keep = ["time"]
    for col in df.columns:
        if col == "time":
            continue
        null_frac = df[col].isna().mean()
        unique_vals = df[col].nunique(dropna=True)
        is_enum = col in enum_signals

        if null_frac > MAX_NULL_FRAC:
            record["dropped"].append((col, "high nulls"))
        elif not is_enum and unique_vals < MIN_UNIQUE_NON_ENUM:
            record["dropped"].append((col, "low uniqueness"))
        else:
            keep.append(col)
            record["kept"].append(col)

    for col, reason in record["dropped"]:
        logging.warning(f"⚠️ Dropped: {col} → {reason}")
    logging.info(f"🧹 Dropped {len(record['dropped'])} signals, kept {len(record['kept']) + 1}")
    return df[keep]

# ─── Enum Labeling ──────────────────────────────────────────────
def apply_enum_labels(df: pd.DataFrame, enum_map: dict) -> pd.DataFrame:
    mapped = 0

    for col in df.columns:
        if col in enum_map:
            try:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    raise TypeError("non-numeric")

                # Convert safely to int where possible
                df[col] = (
                    df[col]
                    .round()
                    .astype("Int64")  # nullable int for safety
                    .astype(str)
                    .map(enum_map[col])
                    .fillna(df[col])
                )
                mapped += 1
            except Exception as e:
                logging.warning(f"⚠️ Enum skipped for {col}: {e}")

    logging.info(f"🧠 Enum signals mapped: {mapped}")
    return df


# ─── Main Processing ────────────────────────────────────────────
summary = []

for pq_file in sorted(DOWNSAMPLED_DIR.glob("*.parquet")):
    logging.info(f"🧼 Cleaning & labeling: {pq_file.name}")
    try:
        df = pd.read_parquet(pq_file)
        record = {
            "file": pq_file.name,
            "dropped": [],
            "kept": [],
            "enum_mapped": [],
            "enum_failed": [],
        }
        df = filter_signals(df, record)
        df = apply_enum_labels(df, enum_map, record)

        out_path = PROCESSED_DIR / pq_file.name.replace(".parquet", "_filtered.parquet")
        df.to_parquet(out_path, index=False)
        logging.info(f"✅ Saved filtered file: {out_path}")
        summary.append(record)

    except Exception as e:
        logging.error(f"❌ Failed to process {pq_file.name}: {e}")

