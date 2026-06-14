-- Water Quality Monitoring Cassandra schema, version 001.
-- This file is safe to run from DataGrip or cqlsh.

CREATE KEYSPACE IF NOT EXISTS water_quality
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE water_quality;

-- Metadata lookup used by Spark/backend to enrich incoming readings.
CREATE TABLE IF NOT EXISTS sensors_by_id (
    sensor_id text PRIMARY KEY,
    location_id text,
    location_name text,
    sensor_type text,
    parameter text,
    unit text,
    status text,
    installed_at timestamp,
    last_seen_at timestamp
);

-- Dashboard lookup for all sensors installed at one location.
CREATE TABLE IF NOT EXISTS sensors_by_location (
    location_id text,
    location_name text,
    sensor_id text,
    sensor_type text,
    parameter text,
    unit text,
    status text,
    last_seen_at timestamp,
    PRIMARY KEY ((location_id), sensor_id)
) WITH CLUSTERING ORDER BY (sensor_id ASC);

-- Demo rule catalog used by Spark for generic alerting and anomaly scoring.
-- These are scenario thresholds for the local demo, not official standards.
CREATE TABLE IF NOT EXISTS parameter_rules_by_parameter (
    parameter text PRIMARY KEY,
    display_name text,
    unit text,
    normal_low double,
    normal_high double,
    critical_low double,
    critical_high double,
    threshold_scale double,
    rate_change_scale double,
    enabled boolean
);

-- Raw readings for a single sensor on a single day.
CREATE TABLE IF NOT EXISTS readings_by_sensor_day (
    sensor_id text,
    bucket_date date,
    event_time timestamp,
    location_id text,
    parameter text,
    value double,
    unit text,
    quality_status text,
    ingestion_time timestamp,
    PRIMARY KEY ((sensor_id, bucket_date), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 7776000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '1'
  };

-- Raw readings for one dashboard chart: location + parameter + day.
CREATE TABLE IF NOT EXISTS readings_by_location_parameter_day (
    location_id text,
    parameter text,
    bucket_date date,
    event_time timestamp,
    sensor_id text,
    value double,
    unit text,
    quality_status text,
    ingestion_time timestamp,
    PRIMARY KEY ((location_id, parameter, bucket_date), event_time, sensor_id)
) WITH CLUSTERING ORDER BY (event_time DESC, sensor_id ASC)
  AND default_time_to_live = 7776000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '1'
  };

-- Current dashboard state for one location.
CREATE TABLE IF NOT EXISTS latest_readings_by_location (
    location_id text,
    parameter text,
    sensor_id text,
    event_time timestamp,
    value double,
    unit text,
    quality_status text,
    updated_at timestamp,
    PRIMARY KEY ((location_id), parameter, sensor_id)
) WITH CLUSTERING ORDER BY (parameter ASC, sensor_id ASC);

-- Alert history for alarm dashboards and notification audit.
CREATE TABLE IF NOT EXISTS alerts_by_sensor_day (
    sensor_id text,
    bucket_date date,
    event_time timestamp,
    alert_id text,
    location_id text,
    location_name text,
    parameter text,
    value double,
    unit text,
    alert_type text,
    severity text,
    threshold_low double,
    threshold_high double,
    alarm_state text,
    message text,
    explanation text,
    processed_at timestamp,
    PRIMARY KEY ((sensor_id, bucket_date), event_time, alert_id)
) WITH CLUSTERING ORDER BY (event_time DESC, alert_id ASC)
  AND default_time_to_live = 157680000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '30'
  };

-- Hourly aggregates for recent dashboard trend charts.
CREATE TABLE IF NOT EXISTS readings_hourly_by_location_parameter (
    location_id text,
    parameter text,
    bucket_month text,
    window_start timestamp,
    reading_count bigint,
    avg_value double,
    min_value double,
    max_value double,
    last_value double,
    unit text,
    updated_at timestamp,
    PRIMARY KEY ((location_id, parameter, bucket_month), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 34560000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '7'
  };

-- Daily aggregates for longer-term dashboard trend charts.
CREATE TABLE IF NOT EXISTS readings_daily_by_location_parameter (
    location_id text,
    parameter text,
    bucket_year int,
    day date,
    reading_count bigint,
    avg_value double,
    min_value double,
    max_value double,
    last_value double,
    unit text,
    updated_at timestamp,
    PRIMARY KEY ((location_id, parameter, bucket_year), day)
) WITH CLUSTERING ORDER BY (day DESC)
  AND default_time_to_live = 157680000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '30'
  };

-- Dashboard-friendly alert timeline for one location on one day.
CREATE TABLE IF NOT EXISTS alerts_by_location_day (
    location_id text,
    bucket_date date,
    event_time timestamp,
    sensor_id text,
    alert_id text,
    location_name text,
    parameter text,
    value double,
    unit text,
    alert_type text,
    severity text,
    threshold_low double,
    threshold_high double,
    alarm_state text,
    message text,
    explanation text,
    processed_at timestamp,
    PRIMARY KEY ((location_id, bucket_date), event_time, sensor_id, alert_id)
) WITH CLUSTERING ORDER BY (event_time DESC, sensor_id ASC, alert_id ASC)
  AND default_time_to_live = 157680000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '7'
  };

ALTER TABLE alerts_by_sensor_day ADD IF NOT EXISTS alarm_state text;
ALTER TABLE alerts_by_sensor_day ADD IF NOT EXISTS explanation text;
ALTER TABLE alerts_by_location_day ADD IF NOT EXISTS alert_id text;
ALTER TABLE alerts_by_location_day ADD IF NOT EXISTS location_name text;
ALTER TABLE alerts_by_location_day ADD IF NOT EXISTS threshold_low double;
ALTER TABLE alerts_by_location_day ADD IF NOT EXISTS threshold_high double;

-- Explainable statistical anomaly scores for one sensor on one day.
CREATE TABLE IF NOT EXISTS ai_scores_by_sensor_day (
    sensor_id text,
    bucket_date date,
    event_time timestamp,
    location_id text,
    parameter text,
    value double,
    unit text,
    rolling_average double,
    rolling_stddev double,
    z_score double,
    rate_of_change double,
    threshold_component double,
    statistical_component double,
    rate_component double,
    anomaly_score double,
    anomaly_level text,
    explanation text,
    computed_at timestamp,
    PRIMARY KEY ((sensor_id, bucket_date), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 157680000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '7'
  };

-- Latest anomaly state for the digital twin overview.
CREATE TABLE IF NOT EXISTS latest_ai_scores_by_location (
    location_id text,
    parameter text,
    sensor_id text,
    event_time timestamp,
    value double,
    unit text,
    anomaly_score double,
    anomaly_level text,
    rolling_average double,
    z_score double,
    rate_of_change double,
    explanation text,
    updated_at timestamp,
    PRIMARY KEY ((location_id), parameter, sensor_id)
) WITH CLUSTERING ORDER BY (parameter ASC, sensor_id ASC);

-- Lightweight pipeline metrics used by the Performance & Analysis dashboard.
CREATE TABLE IF NOT EXISTS pipeline_metrics_by_minute (
    metric_date date,
    minute_start timestamp,
    processed_reading_count bigint,
    alert_count bigint,
    avg_anomaly_score double,
    max_anomaly_score double,
    avg_event_latency_ms double,
    max_event_latency_ms double,
    updated_at timestamp,
    PRIMARY KEY ((metric_date), minute_start)
) WITH CLUSTERING ORDER BY (minute_start DESC)
  AND default_time_to_live = 157680000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '7'
  };

-- Demo rules for the supported water-quality parameters.
INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'pH', 'pH', 'pH', 6.5, 8.5, 6.0, 9.0, 1.0, 1.0, true
) IF NOT EXISTS;

INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'temperature', 'Temperature', 'C', 0.0, 35.0, -5.0, 40.0, 10.0, 10.0, true
) IF NOT EXISTS;

INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'turbidity', 'Turbidity', 'NTU', 0.0, 5.0, -1.0, 20.0, 15.0, 5.0, true
) IF NOT EXISTS;

INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'conductivity', 'Conductivity', 'uS/cm', 50.0, 1500.0, 20.0, 2500.0, 500.0, 300.0, true
) IF NOT EXISTS;

INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'dissolved_oxygen', 'Dissolved Oxygen', 'mg/L', 5.0, 14.0, 3.0, 18.0, 3.0, 2.0, true
) IF NOT EXISTS;

INSERT INTO parameter_rules_by_parameter (
    parameter, display_name, unit, normal_low, normal_high, critical_low,
    critical_high, threshold_scale, rate_change_scale, enabled
) VALUES (
    'ORP', 'ORP', 'mV', 150.0, 500.0, 50.0, 700.0, 200.0, 100.0, true
) IF NOT EXISTS;

-- Demo metadata for the current mock producer messages.
INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_01', 'demo_location_01', 'Demo Lake 01', 'pH', 'pH',
    'pH', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_02', 'demo_location_01', 'Demo Lake 01', 'temperature',
    'temperature', 'C', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_03', 'demo_location_01', 'Demo Lake 01', 'turbidity',
    'turbidity', 'NTU', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_04', 'demo_location_01', 'Demo Lake 01', 'conductivity',
    'conductivity', 'uS/cm', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_05', 'demo_location_01', 'Demo Lake 01', 'dissolved_oxygen',
    'dissolved_oxygen', 'mg/L', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_id (
    sensor_id, location_id, location_name, sensor_type, parameter, unit,
    status, installed_at, last_seen_at
) VALUES (
    'mock_sensor_06', 'demo_location_01', 'Demo Lake 01', 'ORP', 'ORP',
    'mV', 'active', toTimestamp(now()), toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_01', 'pH', 'pH',
    'pH', 'active', toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_02', 'temperature',
    'temperature', 'C', 'active', toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_03', 'turbidity',
    'turbidity', 'NTU', 'active', toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_04', 'conductivity',
    'conductivity', 'uS/cm', 'active', toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_05', 'dissolved_oxygen',
    'dissolved_oxygen', 'mg/L', 'active', toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensors_by_location (
    location_id, location_name, sensor_id, sensor_type, parameter, unit,
    status, last_seen_at
) VALUES (
    'demo_location_01', 'Demo Lake 01', 'mock_sensor_06', 'ORP', 'ORP',
    'mV', 'active', toTimestamp(now())
) IF NOT EXISTS;


-- Sensor Clusters
CREATE TABLE IF NOT EXISTS sensor_configs_by_cluster (
    cluster_id text,
    sensor_id text,
    min double,
    max double,
    sensor_type text,
    unit text,
    measure_interval_s int,
    config_interval_s int,
    updated_at timestamp,
    PRIMARY KEY ((cluster_id), sensor_id)
);
