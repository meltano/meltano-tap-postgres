"""Apache Arrow / ADBC connectivity for BATCH mode (MEL-541).

Arrow BATCH mode reads tables through ADBC (``adbc-driver-manager``'s DBAPI
layer plus the native PostgreSQL ADBC driver) instead of psycopg2, yielding
``pyarrow.RecordBatch`` objects with no per-row Python materialization.

Nothing in this module is imported eagerly by the rest of the tap -
it is only touched when ``batch_config`` is configured.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from tap_postgres import db

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    import pyarrow as pa


def connection_uri(config: dict[str, Any], *, dbname: str | None = None) -> str:
    """Build a libpq connection URI from the tap config.

    Reuses :func:`db.connection_dsn_kwargs` so replica routing
    (``use_secondary``), SSL, the application name and the connect timeout all
    behave exactly as they do for the psycopg2 connections - no new
    connectivity configuration keys are introduced for BATCH mode.
    """
    kwargs = db.connection_dsn_kwargs(config)
    query = {
        "application_name": kwargs["application_name"],
        "connect_timeout": str(kwargs["connect_timeout"]),
    }
    if "sslmode" in kwargs:
        query["sslmode"] = kwargs["sslmode"]
    query_string = "&".join(f"{key}={quote(value, safe='')}" for key, value in query.items())
    user = quote(str(kwargs["user"]), safe="")
    password = quote(str(kwargs["password"]), safe="")
    host = quote(str(kwargs["host"]), safe="")
    database = quote(str(dbname or kwargs["dbname"]), safe="")
    return f"postgresql://{user}:{password}@{host}:{kwargs['port']}/{database}?{query_string}"


@contextlib.contextmanager
def stream_record_batches(
    config: dict[str, Any],
    sql: str,
    params: Sequence[Any] | None = None,
    *,
    dbname: str | None = None,
) -> Generator[pa.RecordBatchReader]:
    """Execute *sql* over ADBC and yield a ``pyarrow.RecordBatchReader``.

    *sql* is expected to be fully built by :mod:`tap_postgres.sqlgen` with
    PostgreSQL-native ``$1`` placeholders (the ADBC driver prepares statements
    server-side, so psycopg2's ``%s`` style is not substituted).
    """
    import adbc_driver_postgresql.dbapi  # local import: only reached in BATCH mode

    with (
        adbc_driver_postgresql.dbapi.connect(connection_uri(config, dbname=dbname)) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(sql, params)
        yield cursor.fetch_record_batch()
