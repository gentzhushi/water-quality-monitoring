import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, lit, struct, to_json, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
READINGS_TOPIC = os.getenv("READINGS_TOPIC", "water-quality-readings")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "water-quality-alerts")


schema = StructType(
    [
        StructField("sensor_id", StringType(), nullable=False),
        StructField("sensor_type", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
    ]
)


spark = (
    SparkSession.builder.appName("WaterQualityAlertStreaming")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(
    f"Starting Spark stream: {READINGS_TOPIC} -> {ALERTS_TOPIC} using {BOOTSTRAP_SERVERS}",
    flush=True,
)

raw_readings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", READINGS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

readings = (
    raw_readings.select(from_json(col("value").cast("string"), schema).alias("reading"))
    .select("reading.*")
    .where(col("sensor_id").isNotNull())
)

alerts = (
    readings.withColumn(
        "alert_type",
        when((col("sensor_type") == "pH") & (col("value") < 6.5), lit("LOW_PH"))
        .when((col("sensor_type") == "pH") & (col("value") > 8.5), lit("HIGH_PH"))
        .when(
            (col("sensor_type") == "temperature") & (col("value") < 0),
            lit("LOW_TEMPERATURE"),
        )
        .when(
            (col("sensor_type") == "temperature") & (col("value") > 35),
            lit("HIGH_TEMPERATURE"),
        ),
    )
    .where(col("alert_type").isNotNull())
    .withColumn(
        "message",
        when(col("alert_type") == "LOW_PH", lit("pH value is below allowed threshold"))
        .when(col("alert_type") == "HIGH_PH", lit("pH value is above allowed threshold"))
        .when(
            col("alert_type") == "LOW_TEMPERATURE",
            lit("Temperature value is below allowed threshold"),
        )
        .when(
            col("alert_type") == "HIGH_TEMPERATURE",
            lit("Temperature value is above allowed threshold"),
        ),
    )
    .withColumn("processed_at", expr("date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")"))
)

alert_output = alerts.select(
    col("sensor_id").cast("string").alias("key"),
    to_json(
        struct(
            "sensor_id",
            "sensor_type",
            "value",
            "alert_type",
            "message",
            "timestamp",
            "processed_at",
        )
    ).alias("value"),
)

console_query = (
    alerts.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/water-quality-console-checkpoint")
    .start()
)

kafka_query = (
    alert_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", "/tmp/water-quality-alerts-checkpoint")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
