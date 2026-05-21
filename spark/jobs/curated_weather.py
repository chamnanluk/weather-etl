from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, max as spark_max, min as spark_min, round


SOURCE_TABLE = "lakehouse.raw.weather_observations"
TARGET_TABLE = "lakehouse.curated.weather_daily_summary"


def main() -> None:
    spark = (
        SparkSession.builder.appName("curated-weather-daily-summary")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.curated")

    observations = spark.table(SOURCE_TABLE)

    summary = (
        observations.groupBy("station_id", "observed_date")
        .agg(
            count("*").alias("observation_count"),
            round(avg("temperature_c"), 2).alias("avg_temperature_c"),
            spark_min("temperature_c").alias("min_temperature_c"),
            spark_max("temperature_c").alias("max_temperature_c"),
            round(avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            round(avg("wind_kph"), 2).alias("avg_wind_kph"),
        )
        .withColumn("loaded_at", current_timestamp())
        .orderBy(col("observed_date"), col("station_id"))
    )

    (
        summary.writeTo(TARGET_TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("observed_date")
        .createOrReplace()
    )

    spark.stop()


if __name__ == "__main__":
    main()
