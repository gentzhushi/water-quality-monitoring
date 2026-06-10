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

-- Demo metadata for the current mock producer messages.
INSERT INTO sensors_by_id (
    sensor_id,
    location_id,
    location_name,
    sensor_type,
    parameter,
    unit,
    status,
    installed_at,
    last_seen_at
) VALUES (
    'mock_sensor_01',
    'demo_location_01',
    'Demo Lake 01',
    'pH',
    'pH',
    'pH',
    'active',
    toTimestamp(now()),
    toTimestamp(now())
);

INSERT INTO sensors_by_id (
    sensor_id,
    location_id,
    location_name,
    sensor_type,
    parameter,
    unit,
    status,
    installed_at,
    last_seen_at
) VALUES (
    'mock_sensor_02',
    'demo_location_01',
    'Demo Lake 01',
    'temperature',
    'temperature',
    'C',
    'active',
    toTimestamp(now()),
    toTimestamp(now())
);

INSERT INTO sensors_by_location (
    location_id,
    location_name,
    sensor_id,
    sensor_type,
    parameter,
    unit,
    status,
    last_seen_at
) VALUES (
    'demo_location_01',
    'Demo Lake 01',
    'mock_sensor_01',
    'pH',
    'pH',
    'pH',
    'active',
    toTimestamp(now())
);

INSERT INTO sensors_by_location (
    location_id,
    location_name,
    sensor_id,
    sensor_type,
    parameter,
    unit,
    status,
    last_seen_at
) VALUES (
    'demo_location_01',
    'Demo Lake 01',
    'mock_sensor_02',
    'temperature',
    'temperature',
    'C',
    'active',
    toTimestamp(now())
);
