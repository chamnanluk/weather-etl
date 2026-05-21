from __future__ import annotations

import os
from pathlib import Path

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
LOCAL_RAW_DIR = Path("/opt/airflow/data/raw/weather")
SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.24.8",
        "software.amazon.awssdk:url-connection-client:2.24.8",
    ]
)
COMMON_SPARK_CONF = {
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}


def upload_raw_weather_files() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
    )

    existing_buckets = {bucket["Name"] for bucket in client.list_buckets()["Buckets"]}
    if MINIO_BUCKET not in existing_buckets:
        client.create_bucket(Bucket=MINIO_BUCKET)

    for path in LOCAL_RAW_DIR.glob("*.csv"):
        key = f"raw/weather/{path.name}"
        client.upload_file(str(path), MINIO_BUCKET, key)


default_args = {
    "owner": "data-engineering",
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_weather_pipeline",
    default_args=default_args,
    description="Upload local weather CSVs to MinIO, transform them with Spark, and publish Iceberg curated tables.",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    tags=["lakehouse", "iceberg", "spark", "minio"],
) as dag:
    ingest_raw_to_minio = PythonOperator(
        task_id="ingest_raw_to_minio",
        python_callable=upload_raw_weather_files,
    )

    write_raw_iceberg = SparkSubmitOperator(
        task_id="write_raw_iceberg",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/raw_to_iceberg.py",
        packages=SPARK_PACKAGES,
        conf=COMMON_SPARK_CONF,
    )

    load_curated_iceberg = SparkSubmitOperator(
        task_id="load_curated_iceberg",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/curated_weather.py",
        packages=SPARK_PACKAGES,
        conf=COMMON_SPARK_CONF,
    )

    validate_curated_table = SQLExecuteQueryOperator(
        task_id="validate_curated_table",
        conn_id="trino_default",
        sql="""
        SELECT station_id, observed_date, observation_count, avg_temperature_c
        FROM iceberg.curated.weather_daily_summary
        ORDER BY observed_date, station_id
        """,
    )

    ingest_raw_to_minio >> write_raw_iceberg >> load_curated_iceberg >> validate_curated_table
