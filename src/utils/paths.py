# src/utils/paths.py

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR       = PROJECT_ROOT / "data"
RAW_DIR        = DATA_DIR / "raw"
DECODED_DIR    = DATA_DIR / "decoded"
INTERIM_DIR    = DATA_DIR / "interim"
PROCESSED_DIR  = DATA_DIR / "processed"
REPORTS_DIR    = DATA_DIR / "reports"

CONFIG_DIR     = PROJECT_ROOT / "config"
DBC_DIR        = CONFIG_DIR / "dbc"
REGISTRY_DIR   = CONFIG_DIR / "registry"