"""LOG_BASED replication via wal2json format version 2 (SPEC §6.3)."""

import datetime
import json
import re
import select
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import dateutil.parser
import psycopg2
import psycopg2.extras
import singer
from singer import utils

from tap_postgres import config as cfg
from tap_postgres import db, discovery, stream_utils

LOGGER = singer.get_logger()

# Operators create replication slots by hand with exactly this prefix (SPEC §6.3.2).
SLOT_PREFIX = "tap_postgres"

FALLBACK_TIMESTAMP = "9999-12-31T23:59:59.999+00:00"
FALLBACK_DATE = "9999-12-31T00:00:00+00:00"
_MAX_NAIVE_TIMESTAMP = datetime.datetime(9999, 12, 31, 23, 59, 59, 999000)

UPDATE_BOOKMARK_PERIOD = 10_000  # fully-processed LSN advances between STATE messages
FEEDBACK_INTERVAL_SECONDS = 10
WAL_SENDER_TIMEOUT_MS = 10_800_000  # 3 hours, applied on servers >= 12

# Element types wal2json array literals can be cast back to losslessly;
# everything else is reconstructed through text[] (SPEC §6.3.6).
NATIVE_ARRAY_CAST_TYPES = {
    "boolean",
    "character varying",
    "cidr",
    "double precision",
    "inet",
    "integer",
    "macaddr",
    "real",
    "smallint",
}


class LogicalReplicationError(Exception):
    """Unrecoverable problem in the logical replication path."""


class UnsupportedPayloadError(LogicalReplicationError):
    """A wal2json payload with an action other than I/U/D."""


# ---------------------------------------------------------------------------
# LSN arithmetic (SPEC §6.3.3)


def lsn_to_int(lsn: str | None) -> int | None:
    """Convert PostgreSQL `file/offset` hex notation to a single integer."""
    if not lsn:
        return None
    file_part, offset = lsn.split("/")
    return (int(file_part, 16) << 32) + int(offset, 16)


def int_to_lsn(lsn: int | None) -> str | None:
    """Convert the integer form back to `file/offset` hex notation."""
    if lsn is None:
        return None
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


# ---------------------------------------------------------------------------
# Server version gate (SPEC §6.3.1)

# (lower bound of the version line, first fixed version)
_BUGGY_VERSION_RANGES = (
    (90400, 90421),
    (90500, 90516),
    (90600, 90612),
    (100000, 100007),
    (110000, 110002),
)


def validate_server_version(version: int) -> None:
    if version < 90400:
        msg = f"Logical replication requires PostgreSQL 9.4 or newer (server reports {version})"
        raise LogicalReplicationError(msg)
    for line_start, first_fixed in _BUGGY_VERSION_RANGES:
        if line_start <= version < first_fixed:
            msg = (
                f"PostgreSQL server version {version} is affected by a known WAL bug; "
                f"upgrade to at least {first_fixed}"
            )
            raise LogicalReplicationError(msg)


def fetch_current_lsn(config: dict[str, Any]) -> int:
    """Read the primary's current WAL LSN, validating the server version (SPEC §4.1)."""
    connection = db.open_connection(config, primary=True)
    try:
        validate_server_version(connection.server_version)
        lsn_function = (
            "pg_current_wal_lsn()"
            if connection.server_version >= 100000
            else "pg_current_xlog_location()"
        )
        current_lsn = lsn_to_int(db.fetch_scalar(connection, f"SELECT {lsn_function}"))
    finally:
        connection.close()
    if current_lsn is None:
        msg = "The server did not report a current WAL LSN"
        raise LogicalReplicationError(msg)
    return current_lsn


# ---------------------------------------------------------------------------
# Replication slots (SPEC §6.3.2)


def generate_slot_name(dbname: str, tap_id: str | None = None) -> str:
    raw = f"{SLOT_PREFIX}_{dbname}_{tap_id}" if tap_id else f"{SLOT_PREFIX}_{dbname}"
    return re.sub(r"[^a-z0-9_]", "_", raw.lower())


def locate_replication_slot_by_cur(cursor: Any, dbname: str, tap_id: str | None = None) -> str:
    """Probe the un-suffixed name first (legacy deployments), then the suffixed one."""
    candidates = [generate_slot_name(dbname)]
    if tap_id:
        candidates.append(generate_slot_name(dbname, tap_id))
    for candidate in candidates:
        cursor.execute(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s", (candidate,)
        )
        if cursor.fetchone():
            LOGGER.info("Using pg_replication_slot %s", candidate)
            return candidate
    msg = f"Unable to find replication slot {' or '.join(candidates)} with wal2json output plugin"
    raise LogicalReplicationError(msg)


def locate_replication_slot(connection: Any, dbname: str, tap_id: str | None = None) -> str:
    with connection.cursor() as cur:
        return locate_replication_slot_by_cur(cur, dbname, tap_id)


# ---------------------------------------------------------------------------
# wal2json options (SPEC §6.3.5)


def escape_wal2json_name(name: str) -> str:
    """Backslash-escape space, single quote, comma, period and asterisk."""
    return re.sub(r"([\\ ',.*])", r"\\\1", name)


def build_add_tables(streams: list[dict[str, Any]]) -> str:
    tables = []
    for stream in streams:
        schema_name = stream_utils.schema_name(stream)
        tables.append(
            f"{escape_wal2json_name(schema_name)}.{escape_wal2json_name(stream['table_name'])}"
        )
    return ",".join(tables)


def add_automatic_properties(stream: dict[str, Any], debug_lsn: bool) -> dict[str, Any]:
    """Add `_sdc_deleted_at` (and `_sdc_lsn` under debug_lsn) to the stream schema."""
    properties = stream["schema"]["properties"]
    properties["_sdc_deleted_at"] = {"type": ["null", "string"], "format": "date-time"}
    if debug_lsn:
        properties["_sdc_lsn"] = {"type": ["null", "string"]}
    else:
        properties.pop("_sdc_lsn", None)
    return stream


# ---------------------------------------------------------------------------
# Value decoding (SPEC §6.3.6)


def parse_logical_timestamp(value: Any) -> str:
    """wal2json timestamps to ISO-8601, degrading to the max sentinel (SPEC §6.3.6)."""
    if isinstance(value, datetime.datetime):
        parsed = value
    else:
        text = str(value)
        if text.endswith(" BC"):
            return FALLBACK_TIMESTAMP
        try:
            parsed = dateutil.parser.parse(text)
        except (ValueError, OverflowError):
            return FALLBACK_TIMESTAMP

    if parsed.tzinfo is None:
        if parsed > _MAX_NAIVE_TIMESTAMP:
            return FALLBACK_TIMESTAMP
        return parsed.isoformat() + "+00:00"
    try:
        as_utc = parsed.astimezone(datetime.timezone.utc)
    except (OverflowError, ValueError):
        return FALLBACK_TIMESTAMP
    if as_utc.replace(tzinfo=None) > _MAX_NAIVE_TIMESTAMP:
        return FALLBACK_TIMESTAMP
    return parsed.isoformat()


def parse_logical_date(value: Any) -> str:
    """Dates to ISO-8601 midnight; years above 9999 degrade, other failures are fatal."""
    if isinstance(value, datetime.date):
        return value.isoformat() + "T00:00:00+00:00"
    text = str(value)
    year_match = re.match(r"^(\d+)-\d{2}-\d{2}$", text)
    if year_match and int(year_match.group(1)) > 9999:
        return FALLBACK_DATE
    return datetime.date.fromisoformat(text).isoformat() + "T00:00:00+00:00"


def reconstruct_array(literal: Any, element_type: str, connection: Any) -> list | None:
    """Cast a PostgreSQL array literal back to a typed array on the server."""
    cast_type = element_type if element_type in NATIVE_ARRAY_CAST_TYPES else "text"
    return db.fetch_scalar(connection, f"SELECT %s::{cast_type}[]", (literal,))


def explode_hstore(literal: str, connection: Any) -> dict[str, Any]:
    """Ask the server to explode an hstore literal into a key/value array."""
    flat = db.fetch_scalar(connection, "SELECT hstore_to_array(%s::hstore)", (literal,)) or []
    return dict(zip(flat[::2], flat[1::2], strict=True))


def selected_value_to_singer_value(value: Any, sql_datatype: str, connection_supplier: Any) -> Any:
    """Decode one wal2json value using the column's catalog datatype (SPEC §6.3.6)."""
    if value is None:
        return None

    if sql_datatype.endswith("[]"):
        element_type = sql_datatype[:-2]
        if isinstance(value, str):
            value = reconstruct_array(value, element_type, connection_supplier())
        if value is None:
            return None
        return [
            selected_value_to_singer_value(element, element_type, connection_supplier)
            if not isinstance(element, list)
            else [
                selected_value_to_singer_value(sub, element_type, connection_supplier)
                for sub in element
            ]
            for element in value
        ]

    if sql_datatype in ("timestamp without time zone", "timestamp with time zone"):
        return parse_logical_timestamp(value)
    if sql_datatype == "date":
        return parse_logical_date(value)
    if sql_datatype == "time with time zone":
        from tap_postgres.conversion import convert_time

        return convert_time(value, with_timezone=True)
    if sql_datatype == "time without time zone":
        from tap_postgres.conversion import convert_time

        return convert_time(value, with_timezone=False)
    if sql_datatype == "bit":
        return value == "1" if isinstance(value, str) else bool(value)
    if sql_datatype == "boolean":
        return value if isinstance(value, bool) else str(value).lower() in ("t", "true", "1")
    if sql_datatype in ("numeric", "decimal") or sql_datatype.startswith(("numeric(", "decimal(")):
        try:
            decimal_value = Decimal(str(value))
        except InvalidOperation as exc:
            msg = f"Cannot decode numeric value {value!r}"
            raise LogicalReplicationError(msg) from exc
        return None if decimal_value.is_nan() else decimal_value
    if sql_datatype == "money":
        return str(value)
    if sql_datatype in ("json", "jsonb"):
        return json.loads(value) if isinstance(value, str) else value
    if sql_datatype == "hstore":
        if isinstance(value, dict):
            return value
        return explode_hstore(value, connection_supplier())
    if sql_datatype in ("smallint", "integer", "bigint"):
        return int(value)
    if sql_datatype in ("real", "double precision"):
        as_float = float(value)
        import math

        return None if (math.isnan(as_float) or math.isinf(as_float)) else as_float

    if isinstance(value, (bool, int, float, str)):
        return value
    msg = f"Cannot decode value of class {type(value).__name__} for datatype {sql_datatype}"
    raise LogicalReplicationError(msg)


# ---------------------------------------------------------------------------
# Message processing (SPEC §6.3.6)


def row_to_singer_message(
    stream: dict[str, Any],
    row: dict[str, Any],
    version: int,
    time_extracted: datetime.datetime,
) -> singer.RecordMessage:
    return singer.RecordMessage(
        stream=stream_utils.dest_stream_name(stream),
        record=row,
        version=version,
        time_extracted=time_extracted,
    )


class LogicalSession:
    """One streaming replication session covering all pure-logical streams of a database."""

    def __init__(
        self,
        config: dict[str, Any],
        streams: list[dict[str, Any]],
        state: dict[str, Any],
        end_lsn: int,
        state_file: Path | None,
        dbname: str,
    ) -> None:
        self.config = config
        self.state = state
        self.end_lsn = end_lsn
        self.state_file = state_file
        self.dbname = dbname
        self.debug_lsn = cfg.debug_lsn(config)
        self.time_extracted = utils.now()

        self.streams = {}
        for stream in streams:
            self.streams[stream_utils.schema_name(stream), stream["table_name"]] = stream

        self.stream_ids = [s["tap_stream_id"] for s in streams]
        self.committed_lsn = self._committed_min_lsn(state)
        self.flushed_lsn: int | None = None
        self.last_processed_lsn: int | None = None
        self.last_fully_processed_lsn: int | None = None
        self.processed_advances = 0
        self.received_first_message = False
        self.rows_synced: dict[str, int] = {}
        self._helper_connection: Any = None

    # -- helpers -----------------------------------------------------------

    def _committed_min_lsn(self, state: dict[str, Any]) -> int | None:
        lsns = [singer.get_bookmark(state, stream_id, "lsn") for stream_id in self.stream_ids]
        lsns = [lsn for lsn in lsns if lsn is not None]
        return min(lsns) if lsns else None

    def helper_connection(self) -> Any:
        """Primary-side connection for array/hstore reconstruction (SPEC §2.3)."""
        if self._helper_connection is None or self._helper_connection.closed:
            self._helper_connection = db.open_connection(
                self.config, primary=True, dbname=self.dbname
            )
        return self._helper_connection

    def close(self) -> None:
        if self._helper_connection is not None and not self._helper_connection.closed:
            self._helper_connection.close()

    # -- message consumption ------------------------------------------------

    def consume_message(self, payload: str, lsn: int) -> None:
        try:
            message = json.loads(payload)
        except (ValueError, TypeError):
            # SPEC §10.2.6: never skip silently.
            LOGGER.warning(
                "Skipping unparseable wal2json payload at %s: %.200s", int_to_lsn(lsn), payload
            )
            self.record_processed(lsn)
            return

        action = message.get("action")
        stream = self.streams.get((message.get("schema"), message.get("table")))
        if stream is None:
            self.record_processed(lsn)
            return
        if action not in ("I", "U", "D"):
            msg = f"Unsupported payload action {action!r}"
            raise UnsupportedPayloadError(msg)

        stream_id = stream["tap_stream_id"]
        version = singer.get_bookmark(self.state, stream_id, "version")
        if version is None:
            msg = f"No version bookmark for stream {stream_id}"
            raise LogicalReplicationError(msg)

        if action in ("I", "U"):
            payload_columns = message.get("columns", [])
            self._maybe_refresh_schema(stream, payload_columns)
            source_columns = payload_columns
            deleted_at = None
        else:
            source_columns = message.get("identity", [])
            deleted_at = singer.utils.strftime(self.time_extracted)

        selected = set(stream_utils.selected_columns(stream))
        row: dict[str, Any] = {}
        for entry in source_columns:
            name = entry.get("name")
            if name not in selected:
                continue
            datatype = stream_utils.sql_datatype_for_column(stream, name)
            if datatype is None:
                msg = f"No sql-datatype metadata for column {name} of stream {stream_id}"
                raise LogicalReplicationError(msg)
            row[name] = selected_value_to_singer_value(
                entry.get("value"), datatype, self.helper_connection
            )
        row["_sdc_deleted_at"] = deleted_at
        if self.debug_lsn:
            row["_sdc_lsn"] = str(lsn)

        singer.write_message(row_to_singer_message(stream, row, version, self.time_extracted))
        self.rows_synced[stream_id] = self.rows_synced.get(stream_id, 0) + 1
        self.state = singer.write_bookmark(self.state, stream_id, "lsn", lsn)
        self.record_processed(lsn)

    def _maybe_refresh_schema(
        self, stream: dict[str, Any], payload_columns: list[dict[str, Any]]
    ) -> None:
        """Schema drift: unknown payload columns refresh the stream schema (SPEC §6.3.6)."""
        known = set(stream["schema"]["properties"])
        incoming = {entry.get("name") for entry in payload_columns}
        if incoming - known:
            LOGGER.info("Schema drift detected on %s: refreshing schema", stream["tap_stream_id"])
            discovery.refresh_streams_schema(self.config, [stream], dbname=self.dbname)
            add_automatic_properties(stream, self.debug_lsn)
            stream_utils.write_schema_message(stream, bookmark_properties=["lsn"])

    def record_processed(self, lsn: int) -> None:
        """A message's LSN is only "fully processed" once a higher LSN arrives (SPEC §6.3.7)."""
        if self.last_processed_lsn is not None and lsn > self.last_processed_lsn:
            self.last_fully_processed_lsn = self.last_processed_lsn
            self.processed_advances += 1
            if self.processed_advances % UPDATE_BOOKMARK_PERIOD == 0:
                self.write_all_bookmarks(self.last_fully_processed_lsn)
                stream_utils.write_state_message(self.state)
        self.last_processed_lsn = lsn

    def write_all_bookmarks(self, lsn: int) -> None:
        for stream_id in self.stream_ids:
            self.state = singer.write_bookmark(self.state, stream_id, "lsn", lsn)

    # -- flush control (SPEC §6.3.7) ----------------------------------------

    def flush_on_first_message(self, cursor: Any, first_lsn: int) -> None:
        if self.received_first_message:
            return
        self.received_first_message = True
        flush_to = first_lsn if self.committed_lsn is None else min(self.committed_lsn, first_lsn)
        LOGGER.info("First message received; flushing slot to %s", int_to_lsn(flush_to))
        cursor.send_feedback(flush_lsn=flush_to, force=True)
        self.flushed_lsn = flush_to

    def refresh_committed_lsn_from_state_file(self, cursor: Any) -> None:
        """Re-read the state file; advance the flush position only past data the
        downstream target has durably committed."""
        if not self.state_file:
            return
        try:
            file_state = utils.load_json(self.state_file)
        except Exception:
            return  # read failures are ignored silently (SPEC §6.3.7)
        committed = self._committed_min_lsn(file_state)
        if committed is None:
            return
        if self.committed_lsn is None or committed > self.committed_lsn:
            self.committed_lsn = committed
        behind_current = self.last_processed_lsn is None or committed <= self.last_processed_lsn
        already_flushed = self.flushed_lsn is not None and committed <= self.flushed_lsn
        if behind_current and not already_flushed:
            LOGGER.info(
                "Downstream committed LSN advanced; flushing slot to %s", int_to_lsn(committed)
            )
            cursor.send_feedback(flush_lsn=committed, force=True)
            self.flushed_lsn = committed

    # -- session end ---------------------------------------------------------

    def finalize(self) -> dict[str, Any]:
        if self.last_fully_processed_lsn is not None:
            final_lsn = self.last_fully_processed_lsn
            if self.committed_lsn is not None:
                final_lsn = max(final_lsn, self.committed_lsn)
            self.write_all_bookmarks(final_lsn)
        for stream_id, count in self.rows_synced.items():
            LOGGER.info("Stream %s: %s records synced from WAL", stream_id, count)
        stream_utils.write_state_message(self.state)
        return self.state


def sync_logical_streams(
    config: dict[str, Any],
    streams: list[dict[str, Any]],
    state: dict[str, Any],
    end_lsn: int,
    state_file: Path | None,
    dbname: str,
) -> dict[str, Any]:
    """Stream WAL changes for all pure-logical streams of one database (SPEC §6.3.5)."""
    LOGGER.info("Starting logical replication for %s", [s["tap_stream_id"] for s in streams])
    debug_lsn = cfg.debug_lsn(config)
    for stream in streams:
        add_automatic_properties(stream, debug_lsn)
        stream_utils.write_schema_message(stream, bookmark_properties=["lsn"])

    session = LogicalSession(config, streams, state, end_lsn, state_file, dbname)
    start_lsn = session.committed_lsn or 0

    lookup_connection = db.open_connection(config, primary=True, dbname=dbname)
    try:
        server_version = lookup_connection.server_version
        slot = locate_replication_slot(lookup_connection, dbname, config.get("tap_id"))
    finally:
        lookup_connection.close()

    extra_options = None
    if server_version >= 120000:
        extra_options = f"-c wal_sender_timeout={WAL_SENDER_TIMEOUT_MS}"

    replication_connection = db.open_connection(
        config,
        primary=True,
        dbname=dbname,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        extra_options=extra_options,
    )
    cursor = replication_connection.cursor()

    wal2json_options = {
        "format-version": "2",
        "include-transaction": "false",
        "include-timestamp": "true",
        "include-types": "false",
        "actions": "insert,update,delete",
        "add-tables": build_add_tables(streams),
    }
    LOGGER.info(
        "Starting replication on slot %s from %s (end LSN %s)",
        slot,
        int_to_lsn(start_lsn),
        int_to_lsn(end_lsn),
    )
    try:
        cursor.start_replication(
            slot_name=slot,
            decode=True,
            start_lsn=start_lsn,
            status_interval=FEEDBACK_INTERVAL_SECONDS,
            options=wal2json_options,
        )
    except psycopg2.ProgrammingError as exc:
        msg = f"Unable to start replication on slot {slot}: {exc}"
        raise LogicalReplicationError(msg) from exc

    max_run_seconds = cfg.max_run_seconds(config)
    poll_total_seconds = cfg.logical_poll_total_seconds(config)
    break_at_end = cfg.break_at_end_lsn(config)

    session_start = time.monotonic()
    last_data_received = time.monotonic()
    last_feedback = time.monotonic()

    try:
        while True:
            now = time.monotonic()
            if now - session_start > max_run_seconds:
                LOGGER.info("Stopping: max_run_seconds (%s) reached", max_run_seconds)
                break
            if now - last_data_received > poll_total_seconds:
                LOGGER.info(
                    "Stopping: no data received for logical_poll_total_seconds (%s)",
                    poll_total_seconds,
                )
                break

            if now - last_feedback > FEEDBACK_INTERVAL_SECONDS:
                cursor.send_feedback(force=True)
                session.refresh_committed_lsn_from_state_file(cursor)
                last_feedback = now

            message = cursor.read_message()
            if message is None:
                # Block on the socket for up to a second, then loop (SPEC §6.3.5).
                select.select([cursor], [], [], 1.0)
                continue

            last_data_received = time.monotonic()
            if break_at_end and message.data_start > end_lsn:
                LOGGER.info(
                    "Stopping: message LSN %s is beyond end LSN %s",
                    int_to_lsn(message.data_start),
                    int_to_lsn(end_lsn),
                )
                break
            session.flush_on_first_message(cursor, message.data_start)
            session.consume_message(message.payload, message.data_start)
    finally:
        state = session.finalize()
        session.close()
        cursor.close()
        replication_connection.close()

    return state
