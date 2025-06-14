#!/usr/bin/env python3
"""
Merges all *_downsampled.parquet files grouped by DBC tag.
Outputs: data/processed/{dbc_tag}.parquet
"""

import pandas as pd
from pathlib import Path
import logging

# ─── Path Setup ───────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

# ─── Group Files by DBC ───────────────────────────────────────────────────
grouped = {}
for file in sorted(INTERIM_DIR.glob("*_downsampled.parquet")):
    try:
        dbc_tag = file.stem.split("_", maxsplit=1)[1].replace("_downsampled", "")
        grouped.setdefault(dbc_tag, []).append(file)
    except IndexError:
        logging.warning(f"❌ Unexpected filename format: {file.name}")

# ─── Merge by Concatenation (stacking) ─────────────────────────────────────
for dbc_tag, files in grouped.items():
    logging.info(f"🔁 Merging {len(files)} files for DBC: {dbc_tag}")
    dfs = []

    for f in files:
        df = pd.read_parquet(f)
        if "time" not in df.columns:
            logging.warning(f"⚠️ Skipping {f.name} (no 'time' column)")
            continue
        dfs.append(df)

    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df["time"] = pd.to_datetime(merged_df["time"], utc=True)
        merged_df = merged_df.sort_values("time")
        out_path = PROCESSED_DIR / f"{dbc_tag}.parquet"
        merged_df.to_parquet(out_path, index=False)
        logging.info(f"✅ Saved merged file: {out_path}")
    else:
        logging.warning(f"⚠️ No valid files to merge for {dbc_tag}")
