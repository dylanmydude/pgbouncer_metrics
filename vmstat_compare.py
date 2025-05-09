#!/usr/bin/env python3


import pandas as pd
import glob, os, sys
from datetime import datetime

# === Configuration ===
EXPORTS_FOLDER = "./exports"               # your logs directory
THRESHOLD      = datetime(2025, 4, 30, 9)  # PgBouncer activation

# === Helpers ===
def extract_timestamp(path, prefix, suffix):
    name = os.path.basename(path)
    ts    = name.replace(prefix, "").replace(suffix, "")
    return datetime.strptime(ts, "%Y%m%d_%H%M%S")

def ingest_vmstat(folder):
    pattern = os.path.join(folder, "vmstat_*.log")
    files   = sorted(glob.glob(pattern))
    if not files:
        print(f"No files found in {pattern}", file=sys.stderr)
        return pd.DataFrame()

    all_recs = []
    for f in files:
        ts = extract_timestamp(f, "vmstat_", ".log")
        with open(f) as fh:
            lines = [l.rstrip() for l in fh if l.strip()]
   
        header_idx = next(
            (i for i, l in enumerate(lines) if "swpd" in l and "free" in l), 
            None
        )
        if header_idx is None:
            continue

        cols      = lines[header_idx].split()
        data_lines = lines[header_idx + 1 :]

        for line in data_lines:
            vals = line.split()
            if len(vals) < len(cols):
                continue
            rec = dict(zip(cols, vals))
            rec["timestamp"] = ts
            all_recs.append(rec)

    df = pd.DataFrame(all_recs)
    if df.empty:
        return df

    for c in df.columns:
        if c == "timestamp":
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# === Main ===
def main():
    df = ingest_vmstat(EXPORTS_FOLDER)
    if df.empty:
        sys.exit("No vmstat data ingested.")

    df["period"] = df["timestamp"].apply(lambda t: "before" if t < THRESHOLD else "after")

    summary = (
        df
        .drop(columns=["timestamp"])
        .groupby("period")
        .mean()
        .round(2)
        .transpose()
    )

    summary.to_csv("vmstat_summary.csv")
    print("→ Wrote vmstat_summary.csv")

if __name__ == "__main__":
    main()
