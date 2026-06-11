#!/usr/bin/env bash
set -e

if [ "$(id -u)" = "0" ]; then
    mkdir -p /tmp/spark-ivy /tmp/water-quality-checkpoints
    chown -R spark:spark /tmp/spark-ivy /tmp/water-quality-checkpoints
    exec /usr/sbin/gosu spark /bin/bash "$0"
fi

SPARK_MASTER_URL="spark://spark-master:7077"
STARTUP_WAIT_SECONDS="${SPARK_STARTUP_WAIT_SECONDS:-15}"

master_pid=""
submit_pid=""

stop_processes() {
    if [ -n "$submit_pid" ] && kill -0 "$submit_pid" 2>/dev/null; then
        kill -TERM "$submit_pid" 2>/dev/null || true
        wait "$submit_pid" 2>/dev/null || true
    fi

    if [ -n "$master_pid" ] && kill -0 "$master_pid" 2>/dev/null; then
        kill -TERM "$master_pid" 2>/dev/null || true
        wait "$master_pid" 2>/dev/null || true
    fi
}

trap stop_processes INT TERM

/opt/spark/bin/spark-class \
    org.apache.spark.deploy.master.Master \
    --host spark-master &
master_pid="$!"

echo "Waiting ${STARTUP_WAIT_SECONDS}s before submitting the streaming job..."
sleep "$STARTUP_WAIT_SECONDS"

if ! kill -0 "$master_pid" 2>/dev/null; then
    wait "$master_pid"
    exit $?
fi

echo "Submitting water quality Spark streaming job..."
/opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER_URL" \
    --conf spark.ui.port=4040 \
    --conf spark.jars.ivy=/tmp/spark-ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf spark.cassandra.connection.host=cassandra \
    --conf spark.cassandra.connection.port=9042 \
    /opt/spark-apps/streaming_job.py &
submit_pid="$!"

wait "$submit_pid"
submit_status="$?"

stop_processes
exit "$submit_status"
