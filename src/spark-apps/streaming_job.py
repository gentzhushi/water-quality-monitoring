import os

from ml_prediction import write_ml_predictions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    coalesce,
    concat,
    concat_ws,
    count,
    current_timestamp,
    date_format,
    date_trunc,
    from_json,
    greatest,
    lag,
    last,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    round as spark_round,
    regexp_replace,
    sha2,
    stddev_samp,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    to_timestamp,
    upper,
    unix_timestamp,
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
KAFKA_MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "600")
CHECKPOINT_BASE = "/tmp/water-quality-checkpoints/v5-clusters"
DEFAULT_THRESHOLD_SCALE = 1.0
DEFAULT_RATE_CHANGE_SCALE = 1.0


# Kafka message schema
reading_schema = StructType(
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
    .config("spark.cassandra.connection.timeoutMS", "30000")
    .config("spark.cassandra.read.timeoutMS", "30000")
    .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
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


def batch_has_rows(dataframe):
    return len(dataframe.take(1)) > 0


def write_ml_predictions_to_cassandra(batch_df, batch_id):
    prediction_df = (
        batch_df.withColumn("location_id", col("cluster_id"))
        .withColumn("location_name", col("cluster_id"))
    )
    write_ml_predictions(prediction_df, batch_id, CASSANDRA_KEYSPACE)


def write_processed_readings(batch_df, _batch_id):
    if not batch_has_rows(batch_df):
        return

    batch_df.cache()
    try:
        write_to_cassandra(
            batch_df.select(
                "sensor_id",
                "bucket_date",
                "event_time",
                "cluster_id",
                "local_sensor_id",
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
                "cluster_id",
                "parameter",
                "bucket_date",
                "event_time",
                "sensor_id",
                "local_sensor_id",
                "value",
                "unit",
                "quality_status",
                "ingestion_time",
            ),
            "readings_by_cluster_parameter_day",
        )

        latest_reading_window = Window.partitionBy(
            "cluster_id",
            "parameter",
            "sensor_id",
        ).orderBy(col("event_time").desc())

        latest_readings = (
            batch_df.withColumn("row_number", row_number().over(latest_reading_window))
            .where(col("row_number") == 1)
            .select(
                "cluster_id",
                "parameter",
                "sensor_id",
                "local_sensor_id",
                "event_time",
                "value",
                "unit",
                "quality_status",
                col("ingestion_time").alias("updated_at"),
            )
        )

        write_to_cassandra(latest_readings, "latest_readings_by_cluster")
    finally:
        batch_df.unpersist()


def write_hourly_aggregates(batch_df, _batch_id):
    if not batch_has_rows(batch_df):
        return

    write_to_cassandra(
        batch_df.select(
            "cluster_id",
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
        "readings_hourly_by_cluster_parameter",
    )


def write_daily_aggregates(batch_df, _batch_id):
    if not batch_has_rows(batch_df):
        return

    write_to_cassandra(
        batch_df.select(
            "cluster_id",
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
        "readings_daily_by_cluster_parameter",
    )

def write_alerts_to_cassandra(batch_df, _batch_id):
    if not batch_has_rows(batch_df):
        return

    batch_df.cache()
    try:
        write_to_cassandra(
            batch_df.select(
                "sensor_id",
                "bucket_date",
                "event_time",
                "alert_id",
                "cluster_id",
                "local_sensor_id",
                "parameter",
                "value",
                "unit",
                "alert_type",
                "severity",
                "threshold_low",
                "threshold_high",
                "alarm_state",
                "message",
                "explanation",
                "processed_at",
            ),
            "alerts_by_sensor_day",
        )

        write_to_cassandra(
            batch_df.select(
                "cluster_id",
                "bucket_date",
                "event_time",
                "sensor_id",
                "local_sensor_id",
                "alert_id",
                "parameter",
                "value",
                "unit",
                "alert_type",
                "severity",
                "threshold_low",
                "threshold_high",
                "alarm_state",
                "message",
                "explanation",
                "processed_at",
            ),
            "alerts_by_cluster_day",
        )
    finally:
        batch_df.unpersist()


def write_ai_scores_and_metrics(batch_df, _batch_id):
    if not batch_has_rows(batch_df):
        return

    rolling_window = (
        Window.partitionBy("sensor_id")
        .orderBy("event_time")
        .rowsBetween(-4, 0)
    )
    previous_reading_window = Window.partitionBy("sensor_id").orderBy("event_time")

    batch_df.cache()
    try:
        scored = (
            batch_df.withColumn("rolling_average", avg("value").over(rolling_window))
            .withColumn(
                "rolling_stddev",
                coalesce(stddev_samp("value").over(rolling_window), lit(0.0)),
            )
            .withColumn("previous_value", lag("value").over(previous_reading_window))
            .withColumn(
                "rate_of_change",
                coalesce(spark_abs(col("value") - col("previous_value")), lit(0.0)),
            )
            .withColumn(
                "threshold_distance",
                when(
                    col("normal_low").isNotNull() & (col("value") < col("normal_low")),
                    col("normal_low") - col("value"),
                )
                .when(
                    col("normal_high").isNotNull() & (col("value") > col("normal_high")),
                    col("value") - col("normal_high"),
                )
                .otherwise(lit(0.0)),
            )
            .withColumn(
                "threshold_scale",
                coalesce(col("threshold_scale"), lit(DEFAULT_THRESHOLD_SCALE)),
            )
            .withColumn(
                "rate_scale",
                coalesce(col("rate_change_scale"), lit(DEFAULT_RATE_CHANGE_SCALE)),
            )
            .withColumn(
                "threshold_component",
                when(
                    col("threshold_scale") > 0,
                    least(lit(1.0), col("threshold_distance") / col("threshold_scale")),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "z_score",
                when(
                    col("rolling_stddev") > 0,
                    spark_abs((col("value") - col("rolling_average")) / col("rolling_stddev")),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "statistical_component",
                least(lit(1.0), greatest(lit(0.0), (col("z_score") - lit(1.5)) / lit(1.5))),
            )
            .withColumn(
                "rate_component",
                when(
                    col("rate_scale") > 0,
                    least(lit(1.0), col("rate_of_change") / col("rate_scale")),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "critical_rule_component",
                when(
                    (
                        col("critical_low").isNotNull()
                        & (col("value") <= col("critical_low"))
                    )
                    | (
                        col("critical_high").isNotNull()
                        & (col("value") >= col("critical_high"))
                    ),
                    lit(1.0),
                )
                .otherwise(lit(0.0)),
            )
            .withColumn(
                "raw_anomaly_score",
                lit(0.5) * col("threshold_component")
                + lit(0.3) * col("statistical_component")
                + lit(0.2) * col("rate_component"),
            )
            .withColumn(
                "anomaly_score",
                spark_round(
                    greatest(
                        col("raw_anomaly_score"),
                        when(col("critical_rule_component") > 0, lit(0.85)).otherwise(lit(0.0)),
                        when(col("threshold_component") > 0, lit(0.60)).otherwise(lit(0.0)),
                        when(col("statistical_component") > 0.5, lit(0.45)).otherwise(lit(0.0)),
                        when(col("rate_component") > 0.5, lit(0.35)).otherwise(lit(0.0)),
                    ),
                    4,
                ),
            )
            .withColumn(
                "anomaly_level",
                when(col("anomaly_score") >= 0.85, lit("CRITICAL"))
                .when(col("anomaly_score") >= 0.60, lit("WARNING"))
                .when(col("anomaly_score") >= 0.35, lit("WATCH"))
                .otherwise(lit("NORMAL")),
            )
            .withColumn(
                "explanation",
                when(
                    col("critical_rule_component") > 0,
                    lit("Value crossed a critical water-quality threshold."),
                )
                .when(
                    (col("threshold_component") > 0) & (col("statistical_component") > 0.5),
                    lit("Value is outside the allowed range and significantly different from recent behavior."),
                )
                .when(
                    col("threshold_component") > 0,
                    lit("Value is outside the allowed water-quality range."),
                )
                .when(
                    col("statistical_component") > 0.5,
                    lit("Value is statistically unusual compared with recent readings."),
                )
                .when(
                    col("rate_component") > 0.5,
                    lit("Value changed faster than expected in the recent window."),
                )
                .otherwise(lit("Reading is consistent with the recent sensor pattern.")),
            )
            .withColumn("computed_at", current_timestamp())
        )

        ai_scores = scored.select(
            "sensor_id",
            "bucket_date",
            "event_time",
            "cluster_id",
            "local_sensor_id",
            "parameter",
            "value",
            "unit",
            "rolling_average",
            "rolling_stddev",
            "z_score",
            "rate_of_change",
            "threshold_component",
            "statistical_component",
            "rate_component",
            "anomaly_score",
            "anomaly_level",
            "explanation",
            "computed_at",
        )

        write_to_cassandra(ai_scores, "ai_scores_by_sensor_day")

        latest_ai_window = Window.partitionBy(
            "cluster_id",
            "parameter",
            "sensor_id",
        ).orderBy(col("event_time").desc())

        latest_ai_scores = (
            ai_scores.withColumn("row_number", row_number().over(latest_ai_window))
            .where(col("row_number") == 1)
            .select(
                "cluster_id",
                "parameter",
                "sensor_id",
                "local_sensor_id",
                "event_time",
                "value",
                "unit",
                "anomaly_score",
                "anomaly_level",
                "rolling_average",
                "z_score",
                "rate_of_change",
                "explanation",
                col("computed_at").alias("updated_at"),
            )
        )

        write_to_cassandra(latest_ai_scores, "latest_ai_scores_by_cluster")

        pipeline_metrics = (
            scored.withColumn("minute_start", date_trunc("minute", col("computed_at")))
            .withColumn("metric_date", to_date(col("minute_start")))
            .withColumn(
                "event_latency_ms",
                ((unix_timestamp(col("computed_at")) - unix_timestamp(col("event_time"))) * 1000).cast("double"),
            )
            .groupBy("cluster_id", "metric_date", "minute_start")
            .agg(
                count("*").alias("processed_reading_count"),
                spark_sum(when(col("quality_status") == "alert", lit(1)).otherwise(lit(0))).cast("bigint").alias("alert_count"),
                avg("anomaly_score").alias("avg_anomaly_score"),
                spark_max("anomaly_score").alias("max_anomaly_score"),
                avg("event_latency_ms").alias("avg_event_latency_ms"),
                spark_max("event_latency_ms").alias("max_event_latency_ms"),
            )
            .withColumn("updated_at", current_timestamp())
            .select(
                "cluster_id",
                "metric_date",
                "minute_start",
                "processed_reading_count",
                "alert_count",
                "avg_anomaly_score",
                "max_anomaly_score",
                "avg_event_latency_ms",
                "max_event_latency_ms",
                "updated_at",
            )
        )

        write_to_cassandra(pipeline_metrics, "pipeline_metrics_by_cluster_minute")
    finally:
        batch_df.unpersist()


# Runtime sensor configuration used to enrich incoming cluster readings.
sensor_metadata = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(keyspace=CASSANDRA_KEYSPACE, table="sensor_configs_by_cluster")
    .load()
    .select(
        "cluster_id",
        col("sensor_id").alias("local_sensor_id"),
        col("sensor_type").alias("parameter"),
        "unit",
    )
    .withColumn("sensor_id", concat(col("cluster_id"), col("local_sensor_id")))
)

# Parameter rules used for generic alerting and anomaly scoring.
parameter_rules = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(keyspace=CASSANDRA_KEYSPACE, table="parameter_rules_by_parameter")
    .load()
    .select(
        col("parameter").alias("rule_parameter"),
        "display_name",
        col("unit").alias("rule_unit"),
        "normal_low",
        "normal_high",
        "critical_low",
        "critical_high",
        "threshold_scale",
        "rate_change_scale",
        "enabled",
    )
)

# Read JSON readings from Kafka
raw_readings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", READINGS_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
    .load()
)

# Parse and validate readings
parsed_readings = (
    raw_readings.select(
        from_json(col("value").cast("string"), reading_schema).alias("reading")
    )
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

rule_enabled = col("rule_enabled")
below_normal = (
    rule_enabled
    & col("normal_low").isNotNull()
    & (col("value") < col("normal_low"))
)
above_normal = (
    rule_enabled
    & col("normal_high").isNotNull()
    & (col("value") > col("normal_high"))
)
below_critical = (
    rule_enabled
    & col("critical_low").isNotNull()
    & (col("value") <= col("critical_low"))
)
above_critical = (
    rule_enabled
    & col("critical_high").isNotNull()
    & (col("value") >= col("critical_high"))
)
abnormal_reading = below_normal | above_normal

# Enrich valid readings and prepare Cassandra columns
processed_readings = (
    valid_readings.join(sensor_metadata, "sensor_id", "inner")
    .withColumn("parameter", coalesce(col("parameter"), col("sensor_type")))
    .join(parameter_rules, col("parameter") == col("rule_parameter"), "left")
    .where(col("cluster_id").isNotNull())
    .where(col("parameter").isNotNull())
    .withColumn("rule_enabled", coalesce(col("enabled"), lit(False)))
    .withColumn("display_name", coalesce(col("display_name"), col("parameter")))
    .withColumn("unit", coalesce(col("unit"), col("rule_unit")))
    .withColumn("bucket_date", to_date(col("event_time")))
    .withColumn("bucket_month", date_format(col("event_time"), "yyyy-MM"))
    .withColumn("bucket_year", date_format(col("event_time"), "yyyy").cast("int"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn(
        "quality_status",
        when(~rule_enabled, lit("unconfigured"))
        .when(abnormal_reading, lit("alert"))
        .otherwise(lit("normal")),
    )
    .select(
        "sensor_id",
        "cluster_id",
        "local_sensor_id",
        "sensor_type",
        "event_time",
        "parameter",
        "display_name",
        "value",
        "unit",
        "quality_status",
        "rule_enabled",
        "normal_low",
        "normal_high",
        "critical_low",
        "critical_high",
        "threshold_scale",
        "rate_change_scale",
        "ingestion_time",
        "bucket_date",
        "bucket_month",
        "bucket_year",
    )
)

# Detect abnormal readings and prepare enriched alert events.
alerts = (
    processed_readings.withColumn(
        "alert_direction",
        when(below_normal, lit("LOW")).when(above_normal, lit("HIGH")),
    )
    .where(col("alert_direction").isNotNull())
    .withColumn(
        "parameter_key",
        upper(regexp_replace(col("parameter"), "[^A-Za-z0-9]+", "_")),
    )
    .withColumn("alert_type", concat_ws("_", col("alert_direction"), col("parameter_key")))
    .withColumn("threshold_low", col("normal_low"))
    .withColumn("threshold_high", col("normal_high"))
    .withColumn("alarm_state", lit("ACTIVE"))
    .withColumn(
        "severity",
        when(below_critical | above_critical, lit("CRITICAL")).otherwise(lit("WARNING")),
    )
    .withColumn(
        "message",
        when(
            col("alert_direction") == "LOW",
            concat(col("display_name"), lit(" value is below allowed threshold")),
        ).otherwise(
            concat(col("display_name"), lit(" value is above allowed threshold")),
        ),
    )
    .withColumn(
        "explanation",
        when(
            col("alert_direction") == "LOW",
            concat(col("display_name"), lit(" is below the configured lower threshold.")),
        ).otherwise(
            concat(col("display_name"), lit(" is above the configured upper threshold.")),
        ),
    )
    .withColumn("processed_at", current_timestamp())
    .withColumn(
        "event_time_text",
        date_format(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    )
    .withColumn(
        "processed_at_text",
        date_format(col("processed_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    )
    .withColumn(
        "alert_id",
        sha2(
            concat_ws(
                "|",
                col("sensor_id"),
                col("alert_type"),
                col("event_time_text"),
                col("value").cast("string"),
            ),
            256,
        ),
    )
)

alert_output = alerts.select(
    col("sensor_id").cast("string").alias("key"),
    to_json(
        struct(
            "alert_id",
            "sensor_id",
            "cluster_id",
            "local_sensor_id",
            "parameter",
            "value",
            "unit",
            "alert_type",
            "severity",
            "threshold_low",
            "threshold_high",
            "message",
            col("event_time_text").alias("event_time"),
            col("processed_at_text").alias("processed_at"),
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
        "cluster_id",
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
        "cluster_id",
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
        "cluster_id",
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
        "cluster_id",
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
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid-readings-console")
    .queryName("valid_readings_console")
    .start()
)

processed_readings_query = (
    processed_readings.writeStream.outputMode("append")
    .foreachBatch(write_processed_readings)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/processed-cassandra")
    .queryName("processed_readings_to_cassandra")
    .start()
)

alerts_console_query = (
    alerts.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts-console")
    .queryName("alerts_console")
    .start()
)

alerts_kafka_query = (
    alert_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts-kafka")
    .queryName("alerts_to_kafka")
    .outputMode("append")
    .start()
)

alerts_cassandra_query = (
    alerts.writeStream.outputMode("append")
    .foreachBatch(write_alerts_to_cassandra)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts-cassandra")
    .queryName("alerts_to_cassandra")
    .start()
)

ai_scores_query = (
    processed_readings.writeStream.outputMode("append")
    .foreachBatch(write_ai_scores_and_metrics)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ai-scores-and-metrics-cassandra")
    .queryName("ai_scores_and_metrics_to_cassandra")
    .start()
)

ml_predictions_query = (
    processed_readings.writeStream.outputMode("append")
    .foreachBatch(write_ml_predictions_to_cassandra)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ml-predictions-cassandra")
    .queryName("ml_predictions_to_cassandra")
    .start()
)

hourly_aggregates_query = (
    hourly_aggregates.writeStream.outputMode("update")
    .foreachBatch(write_hourly_aggregates)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/hourly-aggregates-cassandra")
    .queryName("hourly_aggregates_to_cassandra")
    .start()
)

daily_aggregates_query = (
    daily_aggregates.writeStream.outputMode("update")
    .foreachBatch(write_daily_aggregates)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/daily-aggregates-cassandra")
    .queryName("daily_aggregates_to_cassandra")
    .start()
)

spark.streams.awaitAnyTermination()
