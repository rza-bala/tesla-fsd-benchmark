#!/usr/bin/env python3
"""
Normalizes enum_values column in DBC metadata into a new column enum_map.
Output: Overwrites data/catalog/dbc_signals_metadata.csv
"""

import pandas as pd
import ast
from pathlib import Path

# ─── Paths ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
DBC_META_PATH = CATALOG_DIR / "dbc_signals_metadata.csv"

# ─── Load Metadata ─────────────────────────────────
df = pd.read_csv(DBC_META_PATH)

def safe_parse_enum(val):
    try:
        parsed = ast.literal_eval(val)
        if isinstance(parsed, dict):
            return parsed
        elif isinstance(parsed, list):
            return {k: v for k, v in parsed}
    except Exception:
        return None

df["enum_map"] = df["enum_values"].apply(safe_parse_enum)

# ─── Save with enum_map added ──────────────────────
df.to_csv(DBC_META_PATH, index=False)
print(f"✅ Updated enum_map column in: {DBC_META_PATH}")
