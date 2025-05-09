Ingest PostgreSQL CSV exports, tag as 'before' or 'after' the threshold (2025-04-30 09:00),
and output summary tables for easy comparison of trends.

Outputs:
  - activity_summary.csv: total connection counts by state, before vs after
  - wait_summary.csv: total wait event counts by type, before vs after
  - statements_summary.csv: average mean_exec_time of top queries, before vs after

Usage:
  pip install pandas
  Set EXPORTS_FOLDER and THRESHOLD below
  Run: python3 compare_before_after.py
