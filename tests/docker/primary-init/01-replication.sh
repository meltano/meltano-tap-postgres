#!/bin/bash
# Create a streaming-replication role for the read replica and allow it in pg_hba.
set -e

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-SQL
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replpass';
    CREATE EXTENSION IF NOT EXISTS hstore;
    CREATE EXTENSION IF NOT EXISTS citext;
SQL

echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
