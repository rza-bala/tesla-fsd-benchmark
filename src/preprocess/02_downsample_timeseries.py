#!/usr/bin/env python3
"""
src/processing/downsample_timeseries.py

Downsamples all decoded Parquet files in data/decoded/ to 1Hz using aggregation.
Saves output in data/downsampled/ with *_downsampled.parquet filenames.

- Assumes time column is datetime64[ns, UTC]
- Handles malformed time entries gracefully
- Aggregates numerical signals with mean, others with first
- Maintains column order: time first, then sorted signals
- Logs all steps and summarizes signals kept per file
"""

import pandas as pd
from pathlib import Path
import logging

# ─── Logging Setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

# ─── Constants and Paths ───────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DECODED_DIR = PROJECT_ROOT / "data" / "decoded"
DOWNSAMPLED_DIR = PROJECT_ROOT / "data" / "downsampled"
DOWNSAMPLED_DIR.mkdir(parents=True, exist_ok=True)

TARGET_HZ = 1  # Downsample to 1Hz

# ─── Downsampling Function ─────────────────────────────────────
def downsample_to_1hz(df: pd.DataFrame) -> pd.DataFrame:
    if "time" not in df.columns:
        raise ValueError("Missing required 'time' column.")
    
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True, errors='coerce')
    df = df.dropna(subset=["time"])
    df = df.set_index("time")

    # Choose aggregation based on column dtype
    aggregation = {
        col: "mean" if pd.api.types.is_numeric_dtype(df[col]) else "first"
        for col in df.columns if col != "arbitration_id"
    }
    aggregation["arbitration_id"] = "first"  # keep only one id (optional)

    resampled = df.resample("1S").agg(aggregation)
    resampled = resampled.reset_index()

    # Ensure time is first column
    cols = ["time"] + sorted([col for col in resampled.columns if col != "time"])
    return resampled[cols]

# ─── File Processor ────────────────────────────────────────────
def process_file(file_path: Path):
    try:
        df = pd.read_parquet(file_path)
        original_cols = df.columns.tolist()
        df_down = downsample_to_1hz(df)

        out_path = DOWNSAMPLED_DIR / file_path.name.replace(".parquet", "_downsampled.parquet")
        df_down.to_parquet(out_path, index=False)

        logging.info(f"✅ {file_path.name} → {out_path.name}")
        logging.info(f"    ⏱️ {len(df)} → {len(df_down)} rows, {len(original_cols)} → {len(df_down.columns)} columns")
    except Exception as e:
        logging.warning(f"⚠️ Failed to process {file_path.name}: {e}")

# ─── Main ──────────────────────────────────────────────────────
def main():
    parquet_files = sorted(DECODED_DIR.glob("*.parquet"))
    if not parquet_files:
        logging.warning("⚠️ No decoded Parquet files found in data/decoded/")
        return

    for file in parquet_files:
        logging.info(f"🔽 Downsampling {file.name} ...")
        process_file(file)

if __name__ == "__main__":
    main()
