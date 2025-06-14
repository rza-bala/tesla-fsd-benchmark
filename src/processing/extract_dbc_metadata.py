#!/usr/bin/env python3
"""
Extracts DBC signal metadata and enum mappings.
- Writes DBC signal metadata to: data/catalog/dbc_signals_metadata.csv
- Writes enum maps to:            data/catalog/enum_maps.json
"""

from pathlib import Path
import json
import pandas as pd
import cantools

# ─── Project Paths ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBC_DIR = PROJECT_ROOT / "config" / "dbc"
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
OUTPUT_CSV = CATALOG_DIR / "dbc_signals_metadata.csv"
OUTPUT_JSON = CATALOG_DIR / "enum_maps.json"

CATALOG_DIR.mkdir(parents=True, exist_ok=True)

# ─── Normalize Enum Dictionary ─────────────────────────────────
def normalize_enum_map(choices):
    if not choices:
        return None
    try:
        return {str(int(k)): str(v) for k, v in choices.items()}
    except Exception as e:
        print(f"⚠️ Failed to normalize enum map: {e}")
        return None

# ─── Extract Signal Metadata ───────────────────────────────────
metadata = []
enum_maps = {}

for dbc_file in DBC_DIR.glob("*.dbc"):
    db = cantools.database.load_file(str(dbc_file))
    for msg in db.messages:
        for sig in msg.signals:
            signal_name = sig.name
            enum_map = normalize_enum_map(sig.choices)

            metadata.append({
                "signal_name": signal_name,
                "dbc_source": dbc_file.name,
                "unit": sig.unit,
                "data_type": "enum" if enum_map else ("float" if sig.is_float else "int"),
                "enum_values": json.dumps(enum_map) if enum_map else "",
                "min_physical": sig.minimum,
                "max_physical": sig.maximum,
                "message_name": msg.name,
                "message_id": hex(msg.frame_id),
                "notes": sig.comment or "",
            })

            # Only add enum map if signal hasn't already been added (avoid overwrites)
            if enum_map and signal_name not in enum_maps:
                enum_maps[signal_name] = enum_map

# ─── Save Outputs ──────────────────────────────────────────────
df = pd.DataFrame(metadata)
df.to_csv(OUTPUT_CSV, index=False)

with open(OUTPUT_JSON, "w") as f:
    json.dump(enum_maps, f, indent=2)

print(f"✅ Metadata CSV saved to: {OUTPUT_CSV}")
print(f"✅ Enum JSON saved to: {OUTPUT_JSON}")
