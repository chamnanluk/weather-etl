#!/usr/bin/env bash
set -euo pipefail

docker compose exec trino trino --catalog iceberg --schema curated --execute \
  "SELECT * FROM weather_daily_summary ORDER BY observed_date, station_id"
