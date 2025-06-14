#!/usr/bin/env python3
"""
src/decode/mf4_to_parquet.py

Decodes all MF4 log files in data/raw/ using all DBCs in config/dbc/.
Outputs per-DBC decoded Parquet files in data/decoded/.
- Signal names are preserved (no suffix)
- time is always a proper pandas datetime column (UTC, unit='s')
- Columns ordered: [time, arbitration_id, ...signals]
- Handles enums cleanly for Parquet export
- Logging for all steps (audit-ready, company standard)
- No duplicate or extra time columns
"""

import sys
from pathlib import Path
import pandas as pd
import cantools
import can
import logging

# ─── Path bootstrap ───
THIS = Path(__file__).resolve()
PROJECT_ROOT = THIS.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import RAW_DIR, DECODED_DIR, DBC_DIR
from cantools.database.can.signal import NamedSignalValue

# ─── Logging setup ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

def flatten_decoded(decoded):
    """
    Convert NamedSignalValue to .value and recursively flatten any nested dicts.
    Ensures only primitive types (int, float, str) are saved for Parquet.
    """
    def _convert(val):
        if isinstance(val, NamedSignalValue):
            return val.value
        if isinstance(val, dict):
            return {k: _convert(v) for k, v in val.items()}
        return val
    return {k: _convert(v) for k, v in decoded.items()}

def force_time_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts 'time' to pandas datetime64 (UTC) and reorders columns.
    """
    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    core = ["time", "arbitration_id"]
    rest = sorted([c for c in df.columns if c not in core])
    return df[core + rest]

def decode_mf4_with_dbc(mf4_path: Path, dbc_path: Path) -> pd.DataFrame:
    """
    Decodes a single MF4 log using a specific DBC file.
    Returns a DataFrame of decoded signals per message.
    """
    db = cantools.database.load_file(str(dbc_path))
    records = []
    for msg in can.MF4Reader(str(mf4_path)):
        try:
            decoded = db.decode_message(msg.arbitration_id, msg.data)
            decoded_clean = flatten_decoded(decoded)
            record = {
                "time": msg.timestamp,
                "arbitration_id": hex(msg.arbitration_id),
            }
            record.update(decoded_clean)
            records.append(record)
        except Exception:
            continue
    df = pd.DataFrame(records)
    return force_time_to_datetime(df) if not df.empty else df

def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all columns are Parquet-compatible (no mixed object types).
    """
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except Exception:
                df[col] = df[col].astype(str)
    return df

def main():
    logging.info(f"Using RAW_DIR={RAW_DIR}, DECODED_DIR={DECODED_DIR}, DBC_DIR={DBC_DIR}")
    mf4_files = sorted(RAW_DIR.glob("*.MF4"))
    dbc_files = sorted(DBC_DIR.glob("*.dbc"))

    if not mf4_files:
        logging.warning("No MF4 files found in data/raw/")
        return
    if not dbc_files:
        logging.error("No DBC files found in config/dbc/")
        return

    for mf4_path in mf4_files:
        logging.info(f"Processing {mf4_path.name} ...")
        for dbc_path in dbc_files:
            dbc_stem = dbc_path.stem.replace('-', '_')
            logging.info(f"  Decoding with {dbc_path.name} ...")
            df = decode_mf4_with_dbc(mf4_path, dbc_path)
            if not df.empty:
                df = sanitize_for_parquet(df)
                pq_out = DECODED_DIR / f"{mf4_path.stem}_{dbc_stem}.parquet"
                df.to_parquet(pq_out, index=False)
                logging.info(f"    Saved: {pq_out}")
            else:
                logging.info(f"    No signals decoded for {dbc_path.name}")

        logging.info(f"Finished decoding {mf4_path.name}")

if __name__ == "__main__":
    main()
