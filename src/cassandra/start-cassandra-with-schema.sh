#!/usr/bin/env bash
set -e

CQLSH="/opt/cassandra/bin/cqlsh"
MIGRATIONS_DIR="/opt/cassandra-setup/migrations"

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

echo "Applying Cassandra schema migrations..."
for migration_file in "$MIGRATIONS_DIR"/*.cql; do
    echo "Applying ${migration_file}..."
    "$CQLSH" 127.0.0.1 9042 -f "$migration_file"
done
echo "Cassandra schema is ready."

wait "$cassandra_pid"
