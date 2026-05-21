#!/usr/bin/env bash
set -euo pipefail

docker compose up -d --build
docker compose exec airflow-webserver airflow dags trigger lakehouse_weather_pipeline
