"""Connections, cursors, identifier quoting and driver type registration (SPEC §2.3, §5, §6.4)."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, TypeVar, overload

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import singer

from tap_postgres import config as cfg

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from psycopg2.sql import Composable

if sys.version_info >= (3, 12):
    from typing import Self
else:
    from typing_extensions import Self

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

LOGGER = singer.get_logger()

APPLICATION_NAME = "tap-postgres"
CONNECT_TIMEOUT_SECONDS = 30

# psycopg2 parses PostgreSQL `time` values into datetime.time, which cannot
# represent the legal PostgreSQL value 24:00:00. Receive them as raw strings
# and let the conversion layer apply the hour-24 fix-up (SPEC §5).
_TIME_OID = 1083
_TIMETZ_OID = 1266


_TC = TypeVar("_TC")
_T_cur = TypeVar("_T_cur")
_Vars: TypeAlias = Sequence[Any] | Mapping[str, Any] | None


class CursorProtocol(Protocol):
    itersize: int

    def execute(self, query: str | bytes | Composable, vars: _Vars = None) -> None: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def __iter__(self) -> Self: ...
    def __next__(self) -> tuple[Any, ...]: ...


class ConnectionProtocol(Protocol):
    """The slice of a connection that query helpers need (satisfied by fakes in tests)."""

    @property
    def closed(self) -> int: ...
    def close(self) -> None: ...
    @overload
    def cursor(
        self,
        name: str | bytes | None = None,
        cursor_factory: None = None,
        withhold: bool = False,
        scrollable: bool | None = None,
    ) -> CursorProtocol: ...
    @overload
    def cursor(
        self,
        name: str | bytes | None = None,
        *,
        cursor_factory: Callable[[psycopg2.extensions.connection, str | bytes | None], _T_cur],
        withhold: bool = False,
        scrollable: bool | None = None,
    ) -> _T_cur: ...
    @overload
    def cursor(
        self,
        name: str | bytes | None,
        cursor_factory: Callable[[psycopg2.extensions.connection, str | bytes | None], _T_cur],
        withhold: bool = False,
        scrollable: bool | None = None,
    ) -> _T_cur: ...


def connection_dsn_kwargs(
    config: dict[str, Any],
    *,
    primary: bool = False,
    extra_options: str | None = None,
) -> dict[str, Any]:
    """Build psycopg2 connect() kwargs, routing to the replica when configured.

    ``primary=True`` forces the primary regardless of ``use_secondary``
    (WAL/LSN operations, replication, log-based decode helpers - SPEC §2.3).
    """
    use_secondary = cfg.use_secondary(config) and not primary
    kwargs: dict[str, Any] = {
        "host": config["secondary_host"] if use_secondary else config["host"],
        "port": config["secondary_port"] if use_secondary else config["port"],
        "user": config["user"],
        "password": config["password"],
        "dbname": config["dbname"],
        "application_name": APPLICATION_NAME,
        "connect_timeout": CONNECT_TIMEOUT_SECONDS,
    }
    if cfg.use_ssl(config):
        kwargs["sslmode"] = "require"
    if extra_options:
        kwargs["options"] = extra_options
    return kwargs


def open_connection(
    config: dict[str, Any],
    *,
    primary: bool = False,
    dbname: str | None = None,
    connection_factory: type[_TC] | None = None,
    extra_options: str | None = None,
) -> _TC:
    """Open a database connection per the routing rules in SPEC §2.3."""
    kwargs = connection_dsn_kwargs(config, primary=primary, extra_options=extra_options)
    if dbname:
        kwargs["dbname"] = dbname
    if connection_factory is not None:
        kwargs["connection_factory"] = connection_factory
    connection = psycopg2.connect(**kwargs)
    return connection


def named_cursor(connection: ConnectionProtocol, itersize: int) -> CursorProtocol:
    """A server-side cursor streaming rows in batches of ``itersize`` (SPEC §2.3)."""
    name = f"tap_postgres_{uuid.uuid4().hex}"
    cursor = connection.cursor(name)
    cursor.itersize = itersize
    return cursor


class EmptyResultError(Exception):
    """A query that must return a row returned none."""


def fetch_scalar(
    connection: ConnectionProtocol,
    sql: str,
    params: tuple[Any, ...] | None = None,
) -> Any:  # ruff:ignore[any-type]
    """Execute a query expected to return one row and return its first column.

    Raises :class:`EmptyResultError` instead of crashing on ``fetchone()``
    returning ``None``.
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if row is None:
        msg = f"Query returned no rows: {sql}"
        raise EmptyResultError(msg)
    return row[0]


def quote_ident(identifier: str) -> str:
    """Double-quote an SQL identifier, doubling embedded quotes (SPEC §5)."""
    return '"{}"'.format(identifier.replace('"', '""'))


def fully_qualified_table_name(schema_name: str, table_name: str) -> str:
    return f"{quote_ident(schema_name)}.{quote_ident(table_name)}"


def hstore_available(connection: ConnectionProtocol) -> bool:
    with connection.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_type WHERE typname = 'hstore'")
        return cur.fetchone() is not None


def register_hstore_if_available(connection: ConnectionProtocol) -> bool:
    """Enable the driver's hstore mapping when the server has the extension (SPEC §5)."""
    if hstore_available(connection):
        psycopg2.extras.register_hstore(connection)
        return True
    return False


def register_type_adapters(connection: psycopg2.extensions.connection) -> None:
    """Configure driver-level decoding for traditional syncs (SPEC §6.4).

    - citext[], bit[], uuid[], money[] and enum-array values arrive as string arrays;
    - json/jsonb arrive as raw strings (parsed once by the tap itself, SPEC §5);
    - time/timetz arrive as raw strings so 24:00:00 survives the driver.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT t.typarray
              FROM pg_type t
             WHERE t.typname IN ('citext', 'bit', 'uuid', 'money')
               AND t.typarray <> 0
            UNION
            SELECT t.typarray
              FROM pg_type t
             WHERE t.typtype = 'e'
               AND t.typarray <> 0
            """
        )
        array_oids = [row[0] for row in cur.fetchall()]

    if array_oids:
        string_array = psycopg2.extensions.new_array_type(
            tuple(array_oids), "TAP_POSTGRES_STRING_ARRAY", psycopg2.STRING
        )
        psycopg2.extensions.register_type(string_array, connection)

    # json/jsonb: disable driver-level parsing; the tap parses the raw text (SPEC §5).
    psycopg2.extras.register_default_json(connection, loads=lambda raw: raw)
    psycopg2.extras.register_default_jsonb(connection, loads=lambda raw: raw)

    time_as_string = psycopg2.extensions.new_type(
        (_TIME_OID, _TIMETZ_OID), "TAP_POSTGRES_TIME_STRING", psycopg2.STRING
    )
    psycopg2.extensions.register_type(time_as_string, connection)


def log_encodings(connection: ConnectionProtocol) -> None:
    """Log server and client encodings (SPEC §6.1)."""
    server_encoding = fetch_scalar(connection, "SHOW server_encoding")
    client_encoding = fetch_scalar(connection, "SHOW client_encoding")
    LOGGER.info("Server encoding: %s, client encoding: %s", server_encoding, client_encoding)
