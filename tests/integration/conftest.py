"""Integration test infrastructure (SPEC §8.1).

By default the PostgreSQL primary (built with wal2json, wal_level=logical) and a
streaming read replica are provisioned automatically with testcontainers. Set
TAP_POSTGRES_HOST (plus the other TAP_POSTGRES_* variables) to run against an
externally managed server instead - e.g. the docker-compose stack or a CI service.
"""

import os
import time
import uuid
from pathlib import Path

import psycopg2
import pytest
import singer

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOCKER_CONTEXT = REPO_ROOT / "tests" / "docker"

POSTGRES_USER = "tap_postgres"
POSTGRES_PASSWORD = "tappassword"
POSTGRES_DB = "tap_postgres_test"


def _external_stack():
    host = os.environ.get("TAP_POSTGRES_HOST")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.environ.get("TAP_POSTGRES_PORT", 5432)),
        "user": os.environ.get("TAP_POSTGRES_USER", POSTGRES_USER),
        "password": os.environ.get("TAP_POSTGRES_PASSWORD", POSTGRES_PASSWORD),
        "dbname": os.environ.get("TAP_POSTGRES_DBNAME", POSTGRES_DB),
        "secondary_host": os.environ.get("TAP_POSTGRES_SECONDARY_HOST"),
        "secondary_port": int(os.environ.get("TAP_POSTGRES_SECONDARY_PORT", 0)) or None,
    }


def _wait_for_postgres(host, port, *, timeout=120, **kwargs):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            psycopg2.connect(host=host, port=port, connect_timeout=5, **kwargs).close()
            return
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"PostgreSQL at {host}:{port} never became ready: {last_error}")


@pytest.fixture(scope="session")
def pg_stack():
    """Connection details for the primary and replica."""
    external = _external_stack()
    if external:
        yield external
        return

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.image import DockerImage
    from testcontainers.core.network import Network

    with (
        DockerImage(path=DOCKER_CONTEXT, tag="tap-postgres-integration:latest") as image,
        Network() as network,
    ):
        primary = (
            DockerContainer(str(image))
            .with_env("POSTGRES_USER", POSTGRES_USER)
            .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .with_env("POSTGRES_DB", POSTGRES_DB)
            .with_exposed_ports(5432)
            .with_network(network)
            .with_network_aliases("postgres_primary")
            .with_volume_mapping(
                str(DOCKER_CONTEXT / "primary-init"), "/docker-entrypoint-initdb.d", "ro"
            )
            .with_command(
                "postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10"
            )
        )
        with primary:
            primary_host = primary.get_container_host_ip()
            primary_port = int(primary.get_exposed_port(5432))
            _wait_for_postgres(
                primary_host,
                primary_port,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
            )

            replica = (
                DockerContainer(str(image))
                .with_env("PRIMARY_HOST", "postgres_primary")
                .with_exposed_ports(5432)
                .with_network(network)
                .with_volume_mapping(
                    str(DOCKER_CONTEXT / "replica-entrypoint.sh"),
                    "/replica-entrypoint.sh",
                    "ro",
                )
                .with_kwargs(entrypoint=["bash", "/replica-entrypoint.sh"], user="postgres")
            )
            with replica:
                replica_host = replica.get_container_host_ip()
                replica_port = int(replica.get_exposed_port(5432))
                _wait_for_postgres(
                    replica_host,
                    replica_port,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname=POSTGRES_DB,
                )
                yield {
                    "host": primary_host,
                    "port": primary_port,
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "dbname": POSTGRES_DB,
                    "secondary_host": replica_host,
                    "secondary_port": replica_port,
                }


@pytest.fixture
def tap_config(pg_stack):
    """A tap config dict for the primary."""
    return {
        "host": pg_stack["host"],
        "port": pg_stack["port"],
        "user": pg_stack["user"],
        "password": pg_stack["password"],
        "dbname": pg_stack["dbname"],
    }


@pytest.fixture
def superuser_connection(pg_stack):
    connection = psycopg2.connect(
        host=pg_stack["host"],
        port=pg_stack["port"],
        user=pg_stack["user"],
        password=pg_stack["password"],
        dbname=pg_stack["dbname"],
    )
    connection.autocommit = True
    yield connection
    connection.close()


@pytest.fixture
def test_schema(superuser_connection, tap_config):
    """A dedicated schema per test, filtered into the tap config."""
    schema = f"it_{uuid.uuid4().hex[:12]}"
    with superuser_connection.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    tap_config["filter_schemas"] = schema
    yield schema
    with superuser_connection.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.fixture
def emitted_messages(monkeypatch):
    messages = []
    monkeypatch.setattr(singer, "write_message", messages.append)
    monkeypatch.setattr(singer.messages, "write_message", messages.append)
    return messages


def select_stream(stream, *, replication_method, replication_key=None, extra=None):
    """Mark a discovered stream as selected, in place."""
    for entry in stream["metadata"]:
        if not entry["breadcrumb"]:
            entry["metadata"]["selected"] = True
            entry["metadata"]["replication-method"] = replication_method
            if replication_key:
                entry["metadata"]["replication-key"] = replication_key
            if extra:
                entry["metadata"].update(extra)
    return stream
