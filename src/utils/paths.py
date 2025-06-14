# src/utils/paths.py

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR            = PROJECT_ROOT / "data"
RAW_DIR             = DATA_DIR / "raw"
DECODED_DIR         = DATA_DIR / "decoded"
DOWNSAMPLED_DIR       = DATA_DIR / "downsampled"
PROCESSED_DIR       = DATA_DIR / "processed"
MERGED_DIR          = DATA_DIR / "merged"
CATALOG_DIR         = DATA_DIR / "catalog"

CONFIG_DIR     = PROJECT_ROOT / "config"
DBC_DIR        = CONFIG_DIR / "dbc"
REGISTRY_DIR   = CONFIG_DIR / "registry"

ENUM_MAPS_PATH = REGISTRY_DIR / "enum_maps.json"
DBC_METADATA_PATH = REGISTRY_DIR / "dbc_signals_metadata.csv"