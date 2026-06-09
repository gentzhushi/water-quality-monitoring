# IoT Water Quality Monitoring Architecture Context

This file summarizes the target architecture and Docker setup for a local single-PC IoT water quality monitoring project.

The goal is to run the infrastructure locally with Docker Compose, while keeping the architecture close to a real IoT streaming system.

## High-Level Architecture

```text
Sensor Simulator UI
React / Angular / HTML
        |
        v
Sensor Simulator Service
REST API + WebSocket
Generates fake sensors
        |
        | HTTP / internal call
        v
Ingestion Producer
Kafka producer client
Publishes raw readings
        |
        v
Apache Kafka
Topic: water-quality-readings
        |
        v
Spark Streaming App
Kafka consumer
Validation, windows, ML/anomaly detection
        |
        v
Apache Cassandra
Time-series storage
        |
        v
Dashboard Backend
REST API for dashboard
        |
        v
Dashboard / Digital Twin
Charts, alerts, status
```

## Main Goal

Build a local IoT streaming pipeline:

```text
Mock Producer / Sensor Simulator
        -> Kafka
        -> Spark Structured Streaming
        -> Cassandra
        -> Dashboard Backend/UI
```

The project is for a water quality monitoring system. Simulated sensors publish readings such as pH, temperature, dissolved oxygen, conductivity, turbidity, and ORP.

Spark consumes these readings from Kafka, validates/processes them, detects alerts or anomalies, and writes the results to Cassandra.

## Important Architecture Decision

Do **not** use the simple local Spark container anymore.

Remove this style:

```yaml
spark:
  build:
    context: ./spark
```

where the container directly runs:

```bash
spark-submit streaming_job.py
```

The target setup should use Spark standalone cluster mode with:

```text
spark-master
spark-worker
```

There should be **no separate `spark-job` service/container** in Docker Compose.

The Spark streaming Python script should be stored in a mounted folder and submitted manually through `spark-submit` from the `spark-master` container.

## Why No `spark-job` Container?

The `spark-job` container is only a job submitter/driver container. It is not a Spark worker node.

For this repository, we want a simpler cluster-style setup:

```text
spark-master
spark-worker
```

The Spark consumer application lives as a Python script, for example:

```text
./spark-apps/streaming_job.py
```

It is submitted manually when needed:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit ...
```

This avoids an extra container while still using a Spark master/worker cluster.

## Spark UI Requirements

The user wants these Spark UIs:

```text
Spark Master UI       -> http://localhost:8081
Spark Worker UI       -> http://localhost:8082
Spark Application UI  -> http://localhost:4040
```

Important clarification:

The **Application UI on port 4040 only exists after a Spark application is running**.

It is not created just by starting the master and worker. The Spark Application UI belongs to the Spark driver process.

In this setup, the Spark driver will run inside the `spark-master` container because the job is submitted using `docker exec` inside `spark-master`.

Therefore, expose port `4040` on the `spark-master` service.

The Application UI will be available at:

```text
http://localhost:4040
```

only while the streaming job is running.

The streaming job should contain:

```python
query.awaitTermination()
```

so that the job stays alive and the UI remains available.

## Docker Compose Target Setup

This is the target `docker-compose.yml`.

```yaml
services:
  kafka:
    image: apache/kafka:4.3.0
    container_name: kafka
    hostname: kafka
    ports:
      # Host access to Kafka. Use localhost:9094 from apps running on the host PC.
      - "9094:9094"
    environment:
      # Single-node Kafka in KRaft mode, no ZooKeeper.
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      # Internal Docker listener + controller listener + host listener.
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # Single-broker settings.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      # Local development defaults.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 1

      # KRaft cluster id for local single-node cluster.
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 12

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      DYNAMIC_CONFIG_ENABLED: "true"

  cassandra:
    image: cassandra:4.1
    container_name: cassandra
    hostname: cassandra
    ports:
      - "9042:9042"
    environment:
      CASSANDRA_CLUSTER_NAME: water-quality-cluster
      CASSANDRA_DC: datacenter1
      CASSANDRA_RACK: rack1
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch
    volumes:
      - cassandra_data:/var/lib/cassandra
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES' >/dev/null 2>&1"]
      interval: 20s
      timeout: 10s
      retries: 15
      start_period: 60s

  mock-producer:
    build:
      context: ./mock-producer
    container_name: mock-producer
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: water-quality-readings
    restart: unless-stopped

  spark-master:
    image: spark:3.5.2-scala2.12-java11-python3-ubuntu
    container_name: spark-master
    hostname: spark-master
    command:
      [
        "/opt/spark/bin/spark-class",
        "org.apache.spark.deploy.master.Master",
        "--host",
        "spark-master",
        "--port",
        "7077",
        "--webui-port",
        "8080"
      ]
    ports:
      # Spark standalone master port, used by spark-submit.
      - "7077:7077"

      # Spark Master UI.
      - "8081:8080"

      # Spark Application UI.
      # This is available only after submitting a running Spark app from this container.
      - "4040:4040"
    volumes:
      - ./spark-apps:/opt/spark-apps
    depends_on:
      kafka:
        condition: service_healthy
      cassandra:
        condition: service_healthy

  spark-worker:
    image: spark:3.5.2-scala2.12-java11-python3-ubuntu
    container_name: spark-worker
    hostname: spark-worker
    command:
      [
        "/opt/spark/bin/spark-class",
        "org.apache.spark.deploy.worker.Worker",
        "spark://spark-master:7077",
        "--cores",
        "2",
        "--memory",
        "2g",
        "--webui-port",
        "8081"
      ]
    ports:
      # Spark Worker UI.
      - "8082:8081"
    volumes:
      - ./spark-apps:/opt/spark-apps
    depends_on:
      - spark-master

volumes:
  cassandra_data:
```

## Expected Running Containers

The expected containers are:

```text
kafka
kafka-ui
cassandra
mock-producer
spark-master
spark-worker
```

There should be no:

```text
spark-job
```

There should also be no old local-mode Spark container that directly starts `streaming_job.py` as its default container command.

## Folder Structure

Recommended repository structure:

```text
water-quality-iot/
│
├── docker-compose.yml
│
├── mock-producer/
│   ├── Dockerfile
│   └── producer code
│
├── spark-apps/
│   └── streaming_job.py
│
├── dashboard-backend/
│   └── later backend API
│
└── dashboard-ui/
    └── later frontend
```

The Spark application should be placed here on the host:

```text
./spark-apps/streaming_job.py
```

Inside the Spark containers, it will be available at:

```text
/opt/spark-apps/streaming_job.py
```

because of this volume mount:

```yaml
volumes:
  - ./spark-apps:/opt/spark-apps
```

## How To Start The Infrastructure

Run:

```bash
docker compose up -d --build
```

Check running containers:

```bash
docker ps
```

Expected UI URLs after startup:

```text
Kafka UI:         http://localhost:8080
Spark Master UI: http://localhost:8081
Spark Worker UI: http://localhost:8082
```

The Spark Application UI will not exist yet.

It appears only after running the Spark streaming job.

## How To Submit The Spark Streaming Job

Run this command after the infrastructure is up:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.ui.port=4040 \
  --conf spark.jars.ivy=/tmp/spark-ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  /opt/spark-apps/streaming_job.py
```

After this command starts successfully, open:

```text
Spark Application UI: http://localhost:4040
```

This UI will show Jobs, Stages, Executors, SQL queries, Structured Streaming information, and Environment.

If the streaming job exits, the Application UI disappears.

## How The Spark Job Should Connect To Kafka

Inside Docker, Spark should use:

```text
kafka:9092
```

not:

```text
localhost:9094
```

Use `localhost:9094` only from applications running directly on the host PC.

In `streaming_job.py`, read Kafka using something like:

```python
kafka_bootstrap_servers = "kafka:9092"
readings_topic = "water-quality-readings"
```

or read from environment variables if they are passed.

## How The Spark Job Should Connect To Cassandra

Inside Docker, Spark should use:

```text
cassandra:9042
```

not:

```text
localhost:9042
```

The `spark-submit` command already includes:

```bash
--conf spark.cassandra.connection.host=cassandra
--conf spark.cassandra.connection.port=9042
```

## Cassandra Volume

Cassandra has a named Docker volume:

```yaml
volumes:
  - cassandra_data:/var/lib/cassandra
```

and at the bottom:

```yaml
volumes:
  cassandra_data:
```

This is correct.

The service-level volume mount means:

```text
Mount cassandra_data into /var/lib/cassandra inside the Cassandra container.
```

The top-level volume declaration means:

```text
Docker Compose should create/manage the named volume cassandra_data.
```

Do not mount this volume into Spark. Spark talks to Cassandra over the network using CQL on port `9042`.

## Create Cassandra Keyspace And Tables

Use:

```bash
docker exec -it cassandra cqlsh
```

Then create the keyspace:

```sql
CREATE KEYSPACE IF NOT EXISTS water_quality
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
```

Example readings table:

```sql
USE water_quality;

CREATE TABLE IF NOT EXISTS sensor_readings_by_sensor (
    sensor_id text,
    day date,
    timestamp timestamp,
    parameter text,
    value double,
    unit text,
    location_id text,
    status text,
    PRIMARY KEY ((sensor_id, day), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

Example alerts table:

```sql
CREATE TABLE IF NOT EXISTS water_quality_alerts (
    sensor_id text,
    day date,
    timestamp timestamp,
    location_id text,
    parameter text,
    value double,
    alert_type text,
    severity text,
    message text,
    PRIMARY KEY ((sensor_id, day), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

## Kafka Topics

Main topic:

```text
water-quality-readings
```

Optional alert topic:

```text
water-quality-alerts
```

Kafka auto-topic creation is enabled in the Compose file, but topics can also be created manually:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic water-quality-readings \
  --bootstrap-server localhost:9092
```

List topics:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

## Why The Application UI Needs A Running Spark Job

The Spark Master UI and Worker UI are created by the master and worker daemons.

The Spark Application UI is created by the Spark driver.

In this setup, the driver runs inside `spark-master` because we run `spark-submit` using:

```bash
docker exec -it spark-master ...
```

That is why port `4040` is exposed on `spark-master`.

If no job is running, `http://localhost:4040` will not work.

If the job is running and has `query.awaitTermination()`, the Application UI should remain available.

## Final Target Architecture

The final target architecture is:

```text
Sensor Simulator UI
        |
        v
Sensor Simulator Service
        |
        v
Ingestion Producer / mock-producer
        |
        v
Kafka
        |
        v
Spark Standalone Cluster
  spark-master + spark-worker
        |
        v
Cassandra
        |
        v
Dashboard Backend
        |
        v
Dashboard / Digital Twin UI
```

The Spark application itself is the Kafka consumer:

```text
streaming_job.py
```

It is not a separate normal Kafka consumer service.

The Spark job should read from Kafka using Structured Streaming and write processed readings/alerts to Cassandra.

## Task Summary

Modify the repository so that:

1. The old local-mode `spark` service is removed.
2. There is no `spark-job` container.
3. Add `spark-master` and `spark-worker` services.
4. Mount `./spark-apps` into both Spark containers.
5. Expose:
   - `8081:8080` for Spark Master UI
   - `8082:8081` for Spark Worker UI
   - `4040:4040` on `spark-master` for the Spark Application UI
6. Keep Kafka and Kafka UI.
7. Keep Cassandra with a persistent named volume.
8. Keep `mock-producer`.
9. Place `streaming_job.py` in `./spark-apps`.
10. Run the Spark job manually with `docker exec spark-master spark-submit`.
11. Ensure the job uses Kafka bootstrap server `kafka:9092`.
12. Ensure Spark connects to Cassandra using `cassandra:9042`.
