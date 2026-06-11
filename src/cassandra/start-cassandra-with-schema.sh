#!/usr/bin/env bash
set -e

CQLSH="/opt/cassandra/bin/cqlsh"
MIGRATION_FILE="/opt/cassandra-setup/migrations/001_create_water_quality_schema.cql"

/usr/local/bin/docker-entrypoint.sh cassandra -f &
cassandra_pid="$!"

stop_cassandra() {
    if kill -0 "$cassandra_pid" 2>/dev/null; then
        kill -TERM "$cassandra_pid" 2>/dev/null || true
        wait "$cassandra_pid" 2>/dev/null || true
    fi
}

trap stop_cassandra INT TERM

echo "Waiting for Cassandra to accept CQL connections..."
until "$CQLSH" 127.0.0.1 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
    if ! kill -0 "$cassandra_pid" 2>/dev/null; then
        wait "$cassandra_pid"
        exit $?
    fi

    sleep 5
done

echo "Applying Cassandra schema migration..."
"$CQLSH" 127.0.0.1 9042 -f "$MIGRATION_FILE"
echo "Cassandra schema is ready."

wait "$cassandra_pid"
