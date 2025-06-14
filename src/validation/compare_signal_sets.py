#!/usr/bin/env python3
"""
Compares signal sets across pipeline stages:
- decoded → downsampled
- downsampled → processed
- processed → merged

Outputs detailed CSV and Markdown reports to: data/catalog/
Includes:
- Total signals in each stage
- Dropped and added signals
- Clean visual layout with newlines
"""

import pandas as pd
from pathlib import Path
import logging
import pyarrow.parquet as pq
import os

# ─── Setup ───
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_DIR = PROJECT_ROOT / "data" / "catalog"
CATALOG_DIR.mkdir(parents=True, exist_ok=True)

STAGES = [
    ("decoded", "downsampled"),
    ("downsampled", "processed"),
    ("processed", "merged"),
]

CSV_PATH = CATALOG_DIR / "signal_diff_report.csv"
MD_PATH = CATALOG_DIR / "signal_diff_report.md"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# ─── Helpers ───
def list_parquet_signals(path: Path) -> set:
    try:
        cols = pq.read_table(path).schema.names
        return set(cols) - {"time"}
    except Exception as e:
        logging.error(f"❌ Failed reading {path.name}: {e}")
        return set()

# ─── Comparison Logic ───
records = []
md_lines = ["# Signal Comparison Report\n"]

for src_stage, dst_stage in STAGES:
    src_dir = PROJECT_ROOT / "data" / src_stage
    dst_dir = PROJECT_ROOT / "data" / dst_stage

    src_files = sorted(src_dir.glob("*.parquet"))
    if not src_files:
        logging.warning(f"⚠️ No files found in: {src_dir.name}")
        continue

    for src_path in src_files:
        stem = src_path.stem

        if dst_stage == "processed":
            dst_filename = stem + "_filtered.parquet"
        elif dst_stage == "merged":
            parts = stem.split("_")
            dst_filename = f"{parts[1]}-{parts[2]}.parquet"
        else:
            dst_filename = stem + ".parquet"

        dst_path = dst_dir / dst_filename
        if not dst_path.exists():
            logging.warning(f"⚠️ Missing {src_stage} → {dst_stage} match for {src_path.name}")
            continue

        src_signals = list_parquet_signals(src_path)
        dst_signals = list_parquet_signals(dst_path)
        if not src_signals or not dst_signals:
            continue

        dropped = sorted(src_signals - dst_signals)
        added = sorted(dst_signals - src_signals)
        kept = sorted(src_signals & dst_signals)

        records.append({
            "Stage": f"{src_stage} → {dst_stage}",
            "File": stem,
            "# Source Signals": len(src_signals),
            "# Destination Signals": len(dst_signals),
            "# Kept": len(kept),
            "# Dropped": len(dropped),
            "# Added": len(added),
            "Dropped Signals": "; ".join(dropped),
            "Added Signals": "; ".join(added),
        })

        md_lines.append(f"## {src_stage} → {dst_stage}: `{stem}`\n")
        md_lines.append(f"**Dropped ({len(dropped)}):**")
        md_lines.extend([f"- {s}" for s in dropped])
        md_lines.append(f"\n**Added ({len(added)}):**")
        md_lines.extend([f"- {s}" for s in added])
        md_lines.append("\n---\n")

        logging.info(f"🔍 Compared: {stem} ({src_stage} → {dst_stage}) — Dropped: {len(dropped)}, Added: {len(added)}")

# ─── Save Reports ───
if records:
    pd.DataFrame(records).to_csv(CSV_PATH, index=False)
    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    logging.info(f"✅ Saved CSV: {CSV_PATH}")
    logging.info(f"✅ Saved Markdown: {MD_PATH}")
else:
    logging.warning("⚠️ No comparisons completed.")