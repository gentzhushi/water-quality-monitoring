import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    current_timestamp,
    date_format,
    from_json,
    last,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    struct,
    to_date,
    to_json,
    to_timestamp,
    when,
    window as time_window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window


# Configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
READINGS_TOPIC = os.getenv("READINGS_TOPIC", "water-quality-readings")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "water-quality-alerts")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "water_quality")


# Kafka message schema
schema = StructType(
    [
        StructField("sensor_id", StringType(), nullable=True),
        StructField("sensor_type", StringType(), nullable=True),
        StructField("value", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
    ]
)


# Spark session
spark = (
    SparkSession.builder.appName("WaterQualityProcessingStreaming")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", CASSANDRA_PORT)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(
    f"Starting Spark stream from {READINGS_TOPIC} using {BOOTSTRAP_SERVERS}",
    flush=True,
)


# Cassandra write helpers
def write_to_cassandra(dataframe, table_name):
    dataframe.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        keyspace=CASSANDRA_KEYSPACE,
        table=table_name,
    ).save()


def has_rows(dataframe):
    return len(dataframe.take(1)) > 0


def write_processed_readings(batch_df, batch_id):
    if not has_rows(batch_df):
        return

    batch_df.cache()
    try:
        write_to_cassandra(
            batch_df.select(
                "sensor_id",
                "bucket_date",
                "event_time",
                "location_id",
                "parameter",
                "value",
                "unit",
                "quality_status",
                "ingestion_time",
            ),
            "readings_by_sensor_day",
        )

        write_to_cassandra(
            batch_df.select(
                "location_id",
                "parameter",
                "bucket_date",
                "event_time",
                "sensor_id",
                "value",
                "unit",
                "quality_status",
                "ingestion_time",
            ),
            "readings_by_location_parameter_day",
        )

        latest_window = Window.partitionBy(
            "location_id",
            "parameter",
            "sensor_id",
        ).orderBy(col("event_time").desc())

        latest_readings = (
            batch_df.withColumn("row_number", row_number().over(latest_window))
            .where(col("row_number") == 1)
            .select(
                "location_id",
                "parameter",
                "sensor_id",
                "event_time",
                "value",
                "unit",
                "quality_status",
                col("ingestion_time").alias("updated_at"),
            )
        )

        write_to_cassandra(latest_readings, "latest_readings_by_location")
    finally:
        batch_df.unpersist()


def write_hourly_aggregates(batch_df, batch_id):
    if not has_rows(batch_df):
        return

    write_to_cassandra(
        batch_df.select(
            "location_id",
            "parameter",
            "bucket_month",
            "window_start",
            "reading_count",
            "avg_value",
            "min_value",
            "max_value",
            "last_value",
            "unit",
            "updated_at",
        ),
        "readings_hourly_by_location_parameter",
    )


def write_daily_aggregates(batch_df, batch_id):
    if not has_rows(batch_df):
        return

    write_to_cassandra(
        batch_df.select(
            "location_id",
            "parameter",
            "bucket_year",
            "day",
            "reading_count",
            "avg_value",
            "min_value",
            "max_value",
            "last_value",
            "unit",
            "updated_at",
        ),
        "readings_daily_by_location_parameter",
    )


# Sensor metadata used to enrich incoming readings
sensor_metadata = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(keyspace=CASSANDRA_KEYSPACE, table="sensors_by_id")
    .load()
    .select(
        col("sensor_id").alias("metadata_sensor_id"),
        "location_id",
        "parameter",
        "unit",
    )
)

# Read JSON readings from Kafka
raw_readings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", READINGS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Parse and validate readings
parsed_readings = (
    raw_readings.select(from_json(col("value").cast("string"), schema).alias("reading"))
    .select("reading.*")
)

readings_with_event_time = parsed_readings.withColumn(
    "event_time",
    coalesce(
        to_timestamp(col("timestamp")),
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ),
)

valid_readings = (
    readings_with_event_time.where(col("sensor_id").isNotNull())
    .where(col("sensor_type").isNotNull())
    .where(col("value").isNotNull())
    .where(col("timestamp").isNotNull())
    .where(col("event_time").isNotNull())
)

# Enrich valid readings and prepare Cassandra columns
processed_readings = (
    valid_readings.join(
        sensor_metadata,
        valid_readings.sensor_id == sensor_metadata.metadata_sensor_id,
        "inner",
    )
    .withColumn("parameter", coalesce(col("parameter"), col("sensor_type")))
    .where(col("location_id").isNotNull())
    .where(col("parameter").isNotNull())
    .withColumn("bucket_date", to_date(col("event_time")))
    .withColumn("bucket_month", date_format(col("event_time"), "yyyy-MM"))
    .withColumn("bucket_year", date_format(col("event_time"), "yyyy").cast("int"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn(
        "quality_status",
        when((col("sensor_type") == "pH") & (col("value") < 6.5), lit("alert"))
        .when((col("sensor_type") == "pH") & (col("value") > 8.5), lit("alert"))
        .when(
            (col("sensor_type") == "temperature") & (col("value") < 0),
            lit("alert"),
        )
        .when(
            (col("sensor_type") == "temperature") & (col("value") > 35),
            lit("alert"),
        )
        .otherwise(lit("normal")),
    )
    .select(
        valid_readings.sensor_id,
        "event_time",
        "location_id",
        "parameter",
        "value",
        "unit",
        "quality_status",
        "ingestion_time",
        "bucket_date",
        "bucket_month",
        "bucket_year",
    )
)

# Detect abnormal readings and send alerts back to Kafka
alerts = (
    valid_readings.withColumn(
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
    .withColumn(
        "processed_at",
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    )
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

# Build hourly and daily aggregates for dashboard trends
processed_readings_for_aggregates = processed_readings.withWatermark(
    "event_time",
    "10 minutes",
)

hourly_aggregates = (
    processed_readings_for_aggregates.groupBy(
        "location_id",
        "parameter",
        "unit",
        time_window(col("event_time"), "1 hour"),
    )
    .agg(
        count("*").alias("reading_count"),
        avg("value").alias("avg_value"),
        spark_min("value").alias("min_value"),
        spark_max("value").alias("max_value"),
        last("value", ignorenulls=True).alias("last_value"),
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("bucket_month", date_format(col("window_start"), "yyyy-MM"))
    .withColumn("updated_at", current_timestamp())
    .select(
        "location_id",
        "parameter",
        "bucket_month",
        "window_start",
        "reading_count",
        "avg_value",
        "min_value",
        "max_value",
        "last_value",
        "unit",
        "updated_at",
    )
)

daily_aggregates = (
    processed_readings_for_aggregates.groupBy(
        "location_id",
        "parameter",
        "unit",
        time_window(col("event_time"), "1 day"),
    )
    .agg(
        count("*").alias("reading_count"),
        avg("value").alias("avg_value"),
        spark_min("value").alias("min_value"),
        spark_max("value").alias("max_value"),
        last("value", ignorenulls=True).alias("last_value"),
    )
    .withColumn("day", to_date(col("window.start")))
    .withColumn("bucket_year", date_format(col("day"), "yyyy").cast("int"))
    .withColumn("updated_at", current_timestamp())
    .select(
        "location_id",
        "parameter",
        "bucket_year",
        "day",
        "reading_count",
        "avg_value",
        "min_value",
        "max_value",
        "last_value",
        "unit",
        "updated_at",
    )
)

# Start streaming outputs
valid_readings_query = (
    valid_readings.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/valid-readings-console")
    .queryName("valid_readings_console")
    .start()
)

processed_readings_query = (
    processed_readings.writeStream.outputMode("append")
    .foreachBatch(write_processed_readings)
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/processed-cassandra")
    .queryName("processed_readings_to_cassandra")
    .start()
)

alerts_console_query = (
    alerts.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/alerts-console")
    .queryName("alerts_console")
    .start()
)

alerts_kafka_query = (
    alert_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/alerts-kafka")
    .queryName("alerts_to_kafka")
    .outputMode("append")
    .start()
)

hourly_aggregates_query = (
    hourly_aggregates.writeStream.outputMode("update")
    .foreachBatch(write_hourly_aggregates)
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/hourly-aggregates-cassandra")
    .queryName("hourly_aggregates_to_cassandra")
    .start()
)

daily_aggregates_query = (
    daily_aggregates.writeStream.outputMode("update")
    .foreachBatch(write_daily_aggregates)
    .option("checkpointLocation", "/tmp/water-quality-checkpoints/v2/daily-aggregates-cassandra")
    .queryName("daily_aggregates_to_cassandra")
    .start()
)

spark.streams.awaitAnyTermination()
