#!/bin/bash
# Bootstrap a streaming replica from the primary with pg_basebackup, then start postgres.
set -e

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Waiting for primary to accept replication connections..."
    export PGPASSWORD=replpass
    until pg_basebackup -h "${PRIMARY_HOST:-postgres_primary}" -p 5432 -U replicator \
            -D "$PGDATA" -Fp -Xs -R -w; do
        rm -rf "$PGDATA"/*
        sleep 2
    done
    chmod 700 "$PGDATA"
fi

exec postgres
