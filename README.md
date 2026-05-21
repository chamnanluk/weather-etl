# Weather Data Lakehouse

Complete local Data Lakehouse scaffold using Airflow, MinIO, Hive Metastore, Spark, Iceberg, and Trino.

## Services

| Service | URL |
| --- | --- |
| MinIO API | http://localhost:19000 |
| MinIO Console | http://localhost:19001 |
| Spark Master UI | http://localhost:18081 |
| Spark Worker UI | http://localhost:18082 |
| Trino | http://localhost:18080 |
| Airflow | http://localhost:8088 |

Default credentials:

- MinIO: `minioadmin` / `minioadmin`
- Airflow: `admin` / `admin`

## Layout

```text
.
├── airflow/dags/                  # Airflow orchestration
├── config/
│   ├── hive/hive-site.xml          # Hive metastore and S3 settings
│   ├── spark/spark-defaults.conf   # Spark Iceberg catalog and MinIO settings
│   └── trino/catalog/iceberg.properties
├── data/raw/weather/               # Local raw input files
├── docker/airflow/Dockerfile       # Airflow image with Spark client
├── notebooks/                      # Jupyter exploration notebooks
├── scripts/                        # Helper commands
├── spark/jobs/                     # PySpark Iceberg jobs
├── .env                            # Local credentials and endpoints
└── docker-compose.yml
```

## Run

```bash
docker compose up -d --build
```

Open Airflow at http://localhost:8088 and trigger `lakehouse_weather_pipeline`.

Or trigger from the shell:

```bash
bash scripts/run_pipeline.sh
```

The pipeline:

1. Uploads `data/raw/weather/*.csv` to `s3://lakehouse/raw/weather/` in MinIO.
2. Runs Spark job `raw_to_iceberg.py` and writes `lakehouse.raw.weather_observations`.
3. Runs Spark job `curated_weather.py` and writes `lakehouse.curated.weather_daily_summary`.
4. Validates the curated table through Trino.

## Query

```bash
docker compose exec trino trino --catalog iceberg --schema curated
```

Example SQL:

```sql
SHOW TABLES;
SELECT * FROM weather_daily_summary ORDER BY observed_date, station_id;
```

## Notes

- Iceberg warehouse path: `s3a://lakehouse/warehouse`
- Hive Metastore URI: `thrift://hive-metastore:9083`
- MinIO endpoint inside Compose: `http://minio:9000`
- Spark master host port: `17077` maps to container port `7077`. Internal services still use `spark://spark-master:7077`.
- Spark uses the Iceberg catalog name `lakehouse`; Trino exposes the same metastore as catalog `iceberg`.
