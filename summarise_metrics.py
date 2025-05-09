#!/usr/bin/env python3

import pandas as pd
import glob, os, sys
from datetime import datetime

# === Configuration ===
EXPORTS_FOLDER = "./exports"  # path to your exports directory
THRESHOLD = datetime(2025, 4, 30, 9, 0, 0)

# === Helpers ===
def extract_timestamp(filepath, prefix, suffix):
    fname = os.path.basename(filepath)
    ts_str = fname.replace(prefix, "").replace(suffix, "")
    return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")

# === Ingestion Functions ===
def ingest_activity(folder):
    pattern = os.path.join(folder, "pg_stat_activity_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files matching {pattern}", file=sys.stderr)
        return pd.DataFrame()
    records = []
    for f in files:
        ts = extract_timestamp(f, "pg_stat_activity_", ".csv")
        df = pd.read_csv(f, usecols=["state"])
        counts = df["state"].value_counts().to_dict()
        counts["timestamp"] = ts
        records.append(counts)
    df = pd.DataFrame(records).fillna(0)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def ingest_wait_events(folder):
    pattern = os.path.join(folder, "pg_wait_events_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files matching {pattern}", file=sys.stderr)
        return pd.DataFrame()
    records = []
    for f in files:
        ts = extract_timestamp(f, "pg_wait_events_", ".csv")
        df = pd.read_csv(f, usecols=["wait_event_type"])
        counts = df["wait_event_type"].value_counts().to_dict()
        counts["timestamp"] = ts
        records.append(counts)
    df = pd.DataFrame(records).fillna(0)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def ingest_statements(folder, top_n=5):
    pattern = os.path.join(folder, "pg_stat_statements_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files matching {pattern}", file=sys.stderr)
        return pd.DataFrame()
    records = []
    for f in files:
        ts = extract_timestamp(f, "pg_stat_statements_", ".csv")
        df = pd.read_csv(f)
        df_top = df.nlargest(top_n, "total_exec_time")
        for _, row in df_top.iterrows():
            records.append({
                "timestamp": ts,
                "query": row["query"],
                "mean_exec_time": row["mean_exec_time"]
            })
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# === Main ===
def main():
    # Load datasets
    act_df = ingest_activity(EXPORTS_FOLDER)
    wait_df = ingest_wait_events(EXPORTS_FOLDER)
    stmt_df = ingest_statements(EXPORTS_FOLDER, top_n=5)

    # Check presence
    if act_df.empty and wait_df.empty and stmt_df.empty:
        print("No data ingested. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Tag period column
    for df in (act_df, wait_df, stmt_df):
        if "timestamp" in df:
            df["period"] = df["timestamp"].apply(lambda ts: "before" if ts < THRESHOLD else "after")

    # Summaries
    if not act_df.empty:
        activity_summary = act_df.drop(columns=["timestamp"]).groupby("period").sum()
        activity_summary.to_csv("activity_summary.csv")
        print("Wrote activity_summary.csv")

    if not wait_df.empty:
        wait_summary = wait_df.drop(columns=["timestamp"]).groupby("period").sum()
        wait_summary.to_csv("wait_summary.csv")
        print("Wrote wait_summary.csv")

    if not stmt_df.empty:
        stmt_summary = (
            stmt_df
            .groupby(["query", "period"])["mean_exec_time"]
            .mean()
            .unstack("period")
            .fillna(0)
        )
        stmt_summary.to_csv("statements_summary.csv")
        print("Wrote statements_summary.csv")

if __name__ == "__main__":
    main()
