FROM postgres:16
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-wal2json \
    && rm -rf /var/lib/apt/lists/*
