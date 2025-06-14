#!/usr/bin/env python3
"""
Downsamples all decoded Parquet files in data/decoded/ to 1Hz.
Uses signal types from dbc_signals_metadata.csv to apply proper aggregation.
Saves to: data/interim/*_downsampled.parquet
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ─── Path Setup ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DECODED_DIR = PROJECT_ROOT / "data" / "decoded"
INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
SIGNAL_META_PATH = CATALOG_DIR / "dbc_signals_metadata.csv"

# ─── Downsample Config ─────────────────────────────────
DOWNSAMPLE_HZ = 1  # Target frequency: 1Hz
RULE = f"{int(1000 / DOWNSAMPLE_HZ)}ms"

# ─── Load Signal Metadata ──────────────────────────────
dbc_meta = pd.read_csv(SIGNAL_META_PATH)
dbc_meta["qualified_name"] = dbc_meta["signal_name"] + "_" + dbc_meta["dbc_source"].str.replace(".dbc", "").str.replace("-", "_")
type_lookup = dbc_meta.set_index("qualified_name")["data_type"].to_dict()

# ─── Helper: Downsample One File ───────────────────────
def downsample_df(df: pd.DataFrame, signal_types: dict) -> pd.DataFrame:
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time")

    agg = {}
    for col in df.columns:
        if col == "arbitration_id":
            continue
        sig_type = signal_types.get(col)
        if sig_type == "enum":
            agg[col] = lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else np.nan
        else:
            agg[col] = "mean"

    return df.resample(RULE).agg(agg).reset_index()

# ─── Process All Parquet Files ────────────────────────
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

for pq_file in sorted(DECODED_DIR.glob("*.parquet")):
    print(f"📦 Downsampling: {pq_file.name}")
    df = pd.read_parquet(pq_file)
    if "time" not in df.columns:
        continue

    df_down = downsample_df(df, type_lookup)
    out_name = pq_file.with_suffix('').name + "_downsampled.parquet"
    out_path = INTERIM_DIR / out_name
    df_down.to_parquet(out_path, index=False)
    print(f"✅ Saved: {out_path.name}")
