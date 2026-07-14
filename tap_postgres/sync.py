"""Sync orchestration (SPEC §4)."""

from pathlib import Path
from typing import Any

import singer
from singer import bookmarks

from tap_postgres import config as cfg
from tap_postgres import db, discovery, full_table, incremental, logical, stream_utils
from tap_postgres.batch import ArrowBatchSource, BatchConfig

LOGGER = singer.get_logger()


class SyncError(Exception):
    """Unsupported replication situation."""


def resolve_replication_method(stream: dict[str, Any], config: dict[str, Any]) -> str:
    method = stream_utils.stream_metadata(stream).get("replication-method") or config.get(
        "default_replication_method"
    )
    if method not in cfg.REPLICATION_METHODS:
        msg = (
            f"Unrecognized replication method {method!r} for stream "
            f"{stream['tap_stream_id']}; expected one of {', '.join(cfg.REPLICATION_METHODS)}"
        )
        raise SyncError(msg)
    return method


def reset_state_on_method_change(
    state: dict[str, Any], stream_id: str, method: str, replication_key: str | None
) -> dict[str, Any]:
    """Wipe bookmarks when the replication method or incremental key changed (SPEC §4.2)."""
    previous_method = singer.get_bookmark(state, stream_id, "last_replication_method")
    if previous_method is not None and previous_method != method:
        LOGGER.info(
            "Replication method of stream %s changed from %s to %s: wiping bookmarks",
            stream_id,
            previous_method,
            method,
        )
        state["bookmarks"][stream_id] = {}
    if method == "INCREMENTAL":
        previous_key = singer.get_bookmark(state, stream_id, "replication_key")
        if previous_key is not None and previous_key != replication_key:
            LOGGER.info(
                "Replication key of stream %s changed from %s to %s: wiping bookmarks",
                stream_id,
                previous_key,
                replication_key,
            )
            state["bookmarks"][stream_id] = {}
    return singer.write_bookmark(state, stream_id, "last_replication_method", method)


def classify_log_based_work(state: dict[str, Any], stream_id: str) -> str:
    """Decide between the initial snapshot phase and pure logical streaming (SPEC §4.3)."""
    has_xmin = singer.get_bookmark(state, stream_id, "xmin") is not None
    has_lsn = singer.get_bookmark(state, stream_id, "lsn") is not None
    if not has_xmin and not has_lsn:
        return "snapshot"
    if has_xmin and has_lsn:
        return "snapshot"  # resume an interrupted initial copy
    if has_lsn:
        return "logical"
    msg = f"Stream {stream_id} has an xmin bookmark but no lsn bookmark: inconsistent state"
    raise SyncError(msg)


def _database_name(stream: dict[str, Any]) -> str | None:
    """The stream's `database-name` metadata; callers fall back to the config dbname."""
    return stream_utils.stream_metadata(stream).get("database-name")


def sync_traditional_stream(
    config: dict[str, Any],
    stream: dict[str, Any],
    state: dict[str, Any],
    method: str,
    end_lsn: int | None,
    batch_config: BatchConfig | None = None,
) -> dict[str, Any]:
    stream_id = stream["tap_stream_id"]
    LOGGER.info("Syncing stream %s with method %s", stream_id, method)

    columns = stream_utils.selected_columns(stream)
    if not columns:
        LOGGER.warning("No columns selected for stream %s: skipping it", stream_id)
        return state

    if method == "LOG_BASED" and stream_utils.is_view(stream):
        msg = f"LOG_BASED replication is not supported for view {stream_id}"
        raise SyncError(msg)

    state = singer.set_currently_syncing(state, stream_id)

    # The source is either an ADBC batch source or a psycopg2 connection,
    # never both: BATCH mode opens no psycopg2 connection at all, and
    # LOG_BASED (including its initial snapshot) always uses RECORD messages.
    dbname = _database_name(stream)
    source: db.ConnectionProtocol | ArrowBatchSource
    if batch_config is not None and method in ("FULL_TABLE", "INCREMENTAL"):
        source = ArrowBatchSource(batch_config, dbname=dbname)
    else:
        source = db.open_connection(config, dbname=dbname)
        db.register_type_adapters(source)
    try:
        if method == "FULL_TABLE":
            state, version, first_run = full_table.prepare(state, stream)
            state = full_table.sync_table(
                source, stream, state, config, columns, version, first_run
            )
        elif method == "INCREMENTAL":
            state = incremental.sync_table(source, stream, state, config, columns)
        elif method == "LOG_BASED":
            # Initial snapshot phase: bookmark the end LSN captured at run
            # start so the snapshot and the WAL stream dovetail (SPEC §6.3.4).
            if singer.get_bookmark(state, stream_id, "lsn") is None:
                state = singer.write_bookmark(state, stream_id, "lsn", end_lsn)
            state, version, first_run = full_table.prepare(state, stream)
            state = full_table.sync_table(
                source, stream, state, config, columns, version, first_run
            )
    finally:
        if not isinstance(source, ArrowBatchSource):
            source.close()

    state = singer.set_currently_syncing(state, None)
    stream_utils.write_state_message(state)
    return state


def do_sync(
    config: dict[str, Any],
    catalog: dict[str, Any],
    state: dict[str, Any],
    state_file: Path | None,
) -> dict[str, Any]:
    state.setdefault("bookmarks", {})

    # Fail fast on a bad batch_config (or a missing 'arrow' extra) before any
    # sync work starts. LOG_BASED streams always stay in RECORD mode (MEL-541).
    batch_config = BatchConfig.from_config(config)

    selected = [
        stream for stream in catalog.get("streams", []) if stream_utils.is_stream_selected(stream)
    ]
    if not selected:
        LOGGER.info("No streams marked as selected in the catalog: nothing to sync")
        return state

    # Fail fast on malformed catalog entries; fill in derivable fields (SPEC §9).
    for stream in selected:
        stream_utils.validate_and_normalize_stream(stream)
    selected.sort(key=lambda stream: stream["tap_stream_id"])

    methods = {
        stream["tap_stream_id"]: resolve_replication_method(stream, config) for stream in selected
    }

    # Capture the end LSN once, up front; also validates the server version (SPEC §4.1).
    end_lsn = None
    if any(method == "LOG_BASED" for method in methods.values()):
        end_lsn = logical.fetch_current_lsn(config)
        LOGGER.info("End LSN for this run: %s", logical.int_to_lsn(end_lsn))

    discovery.refresh_streams_schema(config, selected)

    for stream in selected:
        stream_id = stream["tap_stream_id"]
        method = methods[stream_id]
        if method == "LOG_BASED" and stream_utils.is_view(stream):
            msg = f"LOG_BASED replication is not supported for view {stream_id}"
            raise SyncError(msg)
        state = reset_state_on_method_change(
            state, stream_id, method, stream_utils.replication_key(stream)
        )

    traditional: list[dict[str, Any]] = []
    logical_streams: list[dict[str, Any]] = []
    for stream in selected:
        method = methods[stream["tap_stream_id"]]
        if (
            method in ("FULL_TABLE", "INCREMENTAL")
            or classify_log_based_work(state, stream["tap_stream_id"]) == "snapshot"
        ):
            traditional.append(stream)
        else:
            logical_streams.append(stream)

    currently_syncing = bookmarks.get_currently_syncing(state)
    if currently_syncing:
        selected_ids = {stream["tap_stream_id"] for stream in selected}
        if currently_syncing not in selected_ids:
            LOGGER.warning(
                "State says stream %s is currently syncing, but it is no longer selected",
                currently_syncing,
            )
        else:
            traditional.sort(
                key=lambda stream: (
                    stream["tap_stream_id"] != currently_syncing,
                    stream["tap_stream_id"],
                )
            )

    for stream in traditional:
        state = sync_traditional_stream(
            config,
            stream,
            state,
            methods[stream["tap_stream_id"]],
            end_lsn,
            batch_config=batch_config,
        )

    if logical_streams:
        # Drop bookmarks of de-selected LOG_BASED streams so stale positions
        # cannot drag the slot's restart point backwards (SPEC §4.3).
        selected_ids = {stream["tap_stream_id"] for stream in selected}
        for stream_id in list(state["bookmarks"]):
            if stream_id not in selected_ids and "lsn" in state["bookmarks"][stream_id]:
                LOGGER.info("Dropping bookmark of de-selected LOG_BASED stream %s", stream_id)
                del state["bookmarks"][stream_id]

        if end_lsn is None:
            # Unreachable: LOG_BASED streams imply the end LSN was captured above.
            msg = "No end LSN captured for the logical replication phase"
            raise SyncError(msg)

        groups: dict[str, list[dict[str, Any]]] = {}
        for stream in logical_streams:
            groups.setdefault(_database_name(stream) or config["dbname"], []).append(stream)
        for dbname in sorted(groups):
            state = logical.sync_logical_streams(
                config,
                groups[dbname],
                state,
                end_lsn,
                state_file,
                dbname,
            )

    stream_utils.write_state_message(state)
    return state
