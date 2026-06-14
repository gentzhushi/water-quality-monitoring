-- Runtime sensor configuration schema, version 002.
-- These tables store sensors added or changed through the sensor-server API.

USE water_quality;

CREATE TABLE IF NOT EXISTS sensor_clusters (
    cluster_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sensor_configs_by_cluster (
    cluster_id text,
    sensor_id text,
    min_value double,
    max_value double,
    sensor_type text,
    unit text,
    measure_interval_s int,
    config_interval_s int,
    updated_at timestamp,
    PRIMARY KEY ((cluster_id), sensor_id)
) WITH CLUSTERING ORDER BY (sensor_id ASC);

-- Demo sensor-server configuration used by the simulated sensor clusters.
INSERT INTO sensor_clusters (cluster_id) VALUES ('C1') IF NOT EXISTS;
INSERT INTO sensor_clusters (cluster_id) VALUES ('C2') IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C1',
    'S001',
    1,
    100,
    'temperature',
    '°C',
    4,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C1',
    'S002',
    1,
    14,
    'pH',
    'pH units',
    2,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S001',
    0,
    14,
    'pH',
    'pH units',
    2,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S002',
    -10,
    50,
    'temperature',
    'C',
    4,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S003',
    0,
    1000,
    'turbidity',
    'NTU',
    1,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S004',
    0,
    2000,
    'conductivity',
    'uS/cm',
    3,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S005',
    0,
    15,
    'dissolved_oxygen',
    'mg/L',
    2,
    10,
    toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO sensor_configs_by_cluster (
    cluster_id,
    sensor_id,
    min_value,
    max_value,
    sensor_type,
    unit,
    measure_interval_s,
    config_interval_s,
    updated_at
) VALUES (
    'C3',
    'S006',
    -1000,
    1000,
    'ORP',
    'mV',
    4,
    10,
    toTimestamp(now())
) IF NOT EXISTS;
