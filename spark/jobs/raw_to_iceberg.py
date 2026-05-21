from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp, trim, upper
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


RAW_PATH = "s3a://lakehouse/raw/weather/*.csv"
TABLE_NAME = "lakehouse.raw.weather_observations"


def main() -> None:
    spark = (
        SparkSession.builder.appName("raw-weather-to-iceberg")
        .enableHiveSupport()
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("observed_at", StringType(), False),
            StructField("temperature_c", DoubleType(), True),
            StructField("humidity_pct", IntegerType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("condition", StringType(), True),
        ]
    )

    raw = spark.read.option("header", True).schema(schema).csv(RAW_PATH)

    cleaned = (
        raw.select(
            upper(trim(col("station_id"))).alias("station_id"),
            to_timestamp("observed_at").alias("observed_at"),
            col("temperature_c"),
            col("humidity_pct"),
            col("wind_kph"),
            upper(trim(col("condition"))).alias("condition"),
        )
        .withColumn("observed_date", to_date("observed_at"))
        .withColumn("ingested_at", current_timestamp())
        .where(col("station_id").isNotNull() & col("observed_at").isNotNull())
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")

    (
        cleaned.writeTo(TABLE_NAME)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("observed_date")
        .createOrReplace()
    )

    spark.stop()


if __name__ == "__main__":
    main()
