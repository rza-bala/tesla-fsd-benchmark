# Best Practice Merge Script for Same-DBC Group
import pandas as pd
from pathlib import Path
import logging

# Setup
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MERGED_DIR = PROJECT_ROOT / "data" / "merged"
MERGED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")

# Group by DBC name (e.g., "can1-can", "can1-vehicle", ...)
dbc_groups = {
    "can1-can": [],
    "can1-party": [],
    "can1-vehicle": [],
    "can9-internal": [],
}

# Group files by DBC name
for file in sorted(PROCESSED_DIR.glob("*_filtered.parquet")):
    name = file.name
    for group in dbc_groups:
        if group.replace("-", "_") in name:
            dbc_groups[group].append(file)
            break

# Merge each group vertically
for dbc_name, files in dbc_groups.items():
    if not files:
        continue

    logging.info(f"🔗 Merging {len(files)} files for {dbc_name} ...")
    dfs = []

    for path in files:
        df = pd.read_parquet(path)

        # Optional: enforce same column order for consistency
        df = df[sorted(df.columns)]
        dfs.append(df)

    # Vertically stack (safe since same schema)
    merged = pd.concat(dfs, axis=0, ignore_index=True)

    # Sort by time (optional, but highly recommended)
    if "time" in merged.columns:
        merged = merged.sort_values("time")

    # Optional: cast objects to str or float to avoid ArrowTypeError
    for col in merged.select_dtypes(include='object'):
        try:
            merged[col] = pd.to_numeric(merged[col], errors="raise")
        except Exception:
            merged[col] = merged[col].astype(str)

    # Save to merged dir
    out_path = MERGED_DIR / f"{dbc_name}.parquet"
    merged.to_parquet(out_path, index=False)
    logging.info(f"✅ Saved: {out_path.name}")
