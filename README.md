export_metrics.sh
a .pgpass file is needed to discretely access the passwords for the db, this is the format:

# hostname : port : database      : user         : password
localhost:5432:db_name            :user          :password1
localhost:5432:db_name_2          :user_2        :password2
this is located in /home/user/.pgpass

summarise_metrics.py
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

vmstat_compate.py:
Ingest vmstat_*.log from a folder, tag as 'before' or 'after'
the PgBouncer threshold, and output vmstat_summary.csv
showing average values of each metric for easy comparison.

Usage:
  pip install pandas
  Update EXPORTS_FOLDER and THRESHOLD below
  Run: python3 vmstat_compare.py

