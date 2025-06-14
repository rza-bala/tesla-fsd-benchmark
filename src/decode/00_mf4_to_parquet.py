#!/usr/bin/env python3
"""
src/decode/mf4_to_parquet.py

Decodes all MF4 log files in data/raw/ using all DBCs in config/dbc/.
Outputs per-DBC decoded Parquet files in data/decoded/:
  - Signal names are preserved (no suffixes)
  - time is standardized to pandas datetime (UTC)
  - Columns ordered: [time, arbitration_id, ...signals]
  - Enums handled safely using .value
  - Logging used for traceability
"""

import sys
import logging
from pathlib import Path

import pandas as pd
import cantools
import can
from cantools.database.can.signal import NamedSignalValue

# ─── Path Bootstrap ─────────────────────────────────────────────
THIS = Path(__file__).resolve()
PROJECT_ROOT = THIS.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import RAW_DIR, DECODED_DIR, DBC_DIR

# ─── Logging Setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

# ─── Helpers ────────────────────────────────────────────────────

def flatten_decoded(decoded: dict) -> dict:
    """
    Ensures NamedSignalValue (enum) → int
    Recursively flattens nested structures.
    """
    def _convert(val):
        if isinstance(val, NamedSignalValue):
            return val.value
        elif isinstance(val, dict):
            return {k: _convert(v) for k, v in val.items()}
        return val

    return {k: _convert(v) for k, v in decoded.items()}


def force_time_and_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts 'time' column to datetime64[ns, UTC] and reorders columns.
    """
    if 'time' in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    core_cols = ["time", "arbitration_id"]
    remaining_cols = sorted([c for c in df.columns if c not in core_cols])
    return df[core_cols + remaining_cols]


def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all object-type columns are safely cast for Parquet:
    Try float, else fallback to str.
    """
    for col in df.select_dtypes(include=['object']):
        try:
            df[col] = df[col].astype(float)
        except Exception:
            df[col] = df[col].astype(str)
    return df


def decode_mf4_with_dbc(mf4_path: Path, dbc_path: Path) -> pd.DataFrame:
    """
    Loads a single MF4 file and decodes it using one DBC file.
    Returns a DataFrame of decoded messages.
    """
    db = cantools.database.load_file(str(dbc_path))
    records = []

    try:
        reader = can.MF4Reader(str(mf4_path))
    except Exception as e:
        logging.error(f"❌ Could not read MF4: {mf4_path.name} — {e}")
        return pd.DataFrame()

    for msg in reader:
        try:
            decoded = db.decode_message(msg.arbitration_id, msg.data)
            clean = flatten_decoded(decoded)
            record = {
                "time": msg.timestamp,
                "arbitration_id": hex(msg.arbitration_id)
            }
            record.update(clean)
            records.append(record)
        except Exception:
            continue  # silently skip unknown arbitration_ids

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = force_time_and_order(df)
    return sanitize_for_parquet(df)


# ─── Main ───────────────────────────────────────────────────────
def main():
    logging.info(f"📂 RAW_DIR={RAW_DIR}")
    logging.info(f"📂 DBC_DIR={DBC_DIR}")
    logging.info(f"📂 DECODED_DIR={DECODED_DIR}")

    mf4_files = sorted(RAW_DIR.glob("*.MF4"))
    dbc_files = sorted(DBC_DIR.glob("*.dbc"))

    if not mf4_files:
        logging.warning("⚠️ No MF4 files found.")
        return
    if not dbc_files:
        logging.error("❌ No DBC files found.")
        return

    for mf4_path in mf4_files:
        logging.info(f"🔍 Processing: {mf4_path.name}")
        for dbc_path in dbc_files:
            logging.info(f"  📘 Using DBC: {dbc_path.name}")
            df = decode_mf4_with_dbc(mf4_path, dbc_path)

            if df.empty:
                logging.info(f"    ⚠️ No decodable signals for {dbc_path.name}")
                continue

            out_file = DECODED_DIR / f"{mf4_path.stem}_{dbc_path.stem.replace('-', '_')}.parquet"
            df.to_parquet(out_file, index=False)
            logging.info(f"    ✅ Saved to: {out_file}")

        logging.info(f"✅ Finished {mf4_path.name}")


if __name__ == "__main__":
    main()
