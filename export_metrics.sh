#!/bin/bash

# db details
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="db"
DB_USER="db_2"

# Output directory for CSV/log files
OUTPUT_DIR="/home/deploy/scripts/exports"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# pg_stat_activity
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  -c "\COPY (
    SELECT
        pid,
        usename,
        application_name,
        client_addr,
        backend_start,
        state,
        query
    FROM pg_stat_activity
  ) TO '$OUTPUT_DIR/pg_stat_activity_${TIMESTAMP}.csv' WITH CSV HEADER"

# pg_stat_activity
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  -c "\COPY (
    SELECT
       pid,
       wait_event_type,
       wait_event,
       state,
       query
    FROM pg_stat_activity
    WHERE wait_event IS NOT NULL
  ) TO '$OUTPUT_DIR/pg_wait_events_${TIMESTAMP}.csv' WITH CSV HEADER"

# pg_stat_statements
CURRENT_HOUR=$(date +%H)
# if [ "$CURRENT_HOUR" == "00" ] || [ "$CURRENT_HOUR" == "08" ] || [ "$CURRENT_HOUR" == "16" ]; then
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "\COPY (
      SELECT
          query,
          calls,
          total_exec_time,
          mean_exec_time,
          min_exec_time,
          max_exec_time,
          stddev_exec_time,
          rows
      FROM pg_stat_statements
      ORDER BY total_exec_time DESC
      LIMIT 10
    ) TO '$OUTPUT_DIR/pg_stat_statements_${TIMESTAMP}.csv' WITH CSV HEADER"
# fi

# vmstat
vmstat 1 5 > "$OUTPUT_DIR/vmstat_${TIMESTAMP}.log"
