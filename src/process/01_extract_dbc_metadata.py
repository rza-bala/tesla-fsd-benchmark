#!/usr/bin/env python3
"""
Extracts metadata from all DBC files in config/dbc and generates:
- data/registry/dbc_signals_metadata.csv → Signal-level metadata
- data/registry/enum_maps.json → Enum signal mappings (value → name)
"""

from pathlib import Path
import json
import pandas as pd
import cantools
from cantools.database.can.signal import NamedSignalValue
import sys

# ─── Force src/ to be discoverable ─────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import DBC_DIR, REGISTRY_DIR, ENUM_MAPS_PATH, DBC_METADATA_PATH

# ─── Smart Enum Filter ──────────────────────────────────────────
def normalize_enum_map(choices) -> dict | None:
    """
    Cleans and filters enum mappings to exclude fake enums.
    """
    if not choices:
        return None

    try:
        enum_map = {str(int(k)): str(v).strip() for k, v in choices.items()}
        labels = set(v.upper().strip() for v in enum_map.values())

        junk_labels = {"SNA", "N/A", "NONE", "UNKNOWN", "UNDEFINED", ""}
        if labels.issubset(junk_labels):
            return None

        if len(enum_map) <= 2 and any(label in labels for label in junk_labels):
            return None

        return enum_map
    except Exception:
        return None

# ─── Extract Metadata ──────────────────────────────────────────
signal_rows = []
enum_maps = {}

for dbc_file in sorted(DBC_DIR.glob("*.dbc")):
    try:
        db = cantools.database.load_file(str(dbc_file))
        for msg in db.messages:
            for sig in msg.signals:
                signal_name = sig.name
                enum_map = normalize_enum_map(sig.choices)

                data_type = (
                    "enum" if enum_map else
                    "float" if sig.is_float else
                    "int"
                )

                signal_rows.append({
                    "signal_name": signal_name,
                    "unit": sig.unit or "",
                    "data_type": data_type,
                    "enum_values": json.dumps(enum_map) if enum_map else "",
                    "min_physical": sig.minimum,
                    "max_physical": sig.maximum,
                    "scaling": sig.scale,
                    "offset": sig.offset,
                    "bit_length": sig.length,
                    "is_multiplexer": sig.is_multiplexer,
                    "multiplexer_signal": sig.multiplexer_signal or "",
                    "message_name": msg.name,
                    "message_id": hex(msg.frame_id),
                    "dbc_source": dbc_file.name,
                    "notes": sig.comment or ""
                })

                if enum_map and signal_name not in enum_maps:
                    enum_maps[signal_name] = enum_map

    except Exception as e:
        print(f"⚠️ Failed to load {dbc_file.name}: {e}")

# ─── Save Outputs ──────────────────────────────────────────────
df = pd.DataFrame(signal_rows)
df.to_csv(DBC_METADATA_PATH, index=False)

with open(ENUM_MAPS_PATH, "w") as f:
    json.dump(enum_maps, f, indent=2)

print(f"✅ Metadata CSV saved to: {DBC_METADATA_PATH}")
print(f"✅ Enum JSON saved to:  {ENUM_MAPS_PATH}")
