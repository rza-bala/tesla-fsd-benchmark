#!/usr/bin/env python3
"""
Cleans and labels downsampled timeseries files using enum_maps and DBC metadata.
Drops weak/uninformative signals and applies human-readable enum labels.
Outputs: data/labeled/{drive_name}_labeled.parquet
"""

import pandas as pd
import json
import logging
from pathlib import Path

# ─── Path Setup ────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
LABELED_DIR = PROJECT_ROOT / "data" / "labeled"
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
ENUM_MAP_PATH = CATALOG_DIR / "enum_maps.json"
DBC_META_PATH = CATALOG_DIR / "dbc_signals_metadata.csv"

LABELED_DIR.mkdir(parents=True, exist_ok=True)

# ─── Logger Setup ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

# ─── Load Metadata ─────────────────────────────────────────────
enum_map = json.loads(ENUM_MAP_PATH.read_text())
dbc_meta = pd.read_csv(DBC_META_PATH).fillna("")
enum_signals = set(dbc_meta.query("data_type == 'enum'")["signal_name"].tolist())

# ─── Processing ────────────────────────────────────────────────
for pq_file in sorted(INTERIM_DIR.glob("*_downsampled.parquet")):
    logging.info(f"🧼 Cleaning & labeling: {pq_file.name}")
    df = pd.read_parquet(pq_file)
    original_cols = df.columns.tolist()

    # Drop weak signals
    drop_cols = []
    for col in df.columns:
        if col in ["time", "arbitration_id"]:
            continue
        s = df[col]
        if s.isna().mean() > 0.9 or s.nunique(dropna=True) <= 1:
            drop_cols.append(col)

    if drop_cols:
        logging.info(f"  🧹 Dropping {len(drop_cols)} weak signals: {drop_cols}")
        df.drop(columns=drop_cols, inplace=True)

    # Label enum columns
    num_mapped = 0
    for col in df.columns:
        if col in enum_signals and col in enum_map:
            try:
                df[col] = (
                    df[col]
                    .apply(lambda x: str(int(x)) if pd.notna(x) and str(int(x)) in enum_map[col] else x)
                    .map(enum_map[col])
                    .fillna(df[col])
                )
                num_mapped += 1
            except Exception as e:
                logging.warning(f"  ⚠️ Failed to map enum values for '{col}': {e}")

    if num_mapped:
        logging.info(f"  ✅ {num_mapped} enum signals mapped.")
    else:
        logging.info("  ⚠️ No enum signals mapped.")

    # Save cleaned & labeled output
    out_path = LABELED_DIR / pq_file.name.replace("_downsampled", "_labeled")
    df.to_parquet(out_path, index=False)
    logging.info(f"✅ Saved labeled file: {out_path}")
