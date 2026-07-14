"""INCREMENTAL replication (SPEC §6.2)."""

from typing import Any

import singer
from singer import metrics, utils

from tap_postgres import config as cfg
from tap_postgres import db, sqlgen, stream_utils
from tap_postgres.conversion import selected_value_to_singer_value
from tap_postgres.full_table import new_table_version

LOGGER = singer.get_logger()

STATE_UPDATE_PERIOD = 10_000  # rows between STATE messages

ALLOWED_BOOKMARK_KEYS = {
    "replication_key",
    "replication_key_value",
    "version",
    "last_replication_method",
}


class ReplicationKeyError(Exception):
    """The stream is not usable for INCREMENTAL replication."""


def validate_bookmark_keys(state: dict[str, Any], stream_id: str) -> None:
    """Any unexpected bookmark key on an INCREMENTAL stream is fatal (SPEC §4.5)."""
    bookmark = state.get("bookmarks", {}).get(stream_id, {})
    unexpected = set(bookmark) - ALLOWED_BOOKMARK_KEYS
    if unexpected:
        msg = (
            f"Unexpected bookmark keys for INCREMENTAL stream {stream_id}: "
            f"{', '.join(sorted(unexpected))}"
        )
        raise ReplicationKeyError(msg)


def sync_table(
    connection: Any,
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
) -> dict[str, Any]:
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)
    schema_name = stream_utils.schema_name(stream)

    validate_bookmark_keys(state, stream_id)

    replication_key = stream_utils.replication_key(stream)
    if not replication_key:
        msg = f"No replication-key metadata for INCREMENTAL stream {stream_id}"
        raise ReplicationKeyError(msg)
    key_datatype = stream_utils.sql_datatype_for_column(stream, replication_key)
    if not key_datatype:
        msg = f"Replication key {replication_key} of stream {stream_id} has no sql-datatype"
        raise ReplicationKeyError(msg)

    # An interrupted incremental run keeps the same version: resumption relies
    # on the key bookmark, not versioning (SPEC §6.2).
    version = singer.get_bookmark(state, stream_id, "version")
    if version is None:
        version = new_table_version()
    state = singer.write_bookmark(state, stream_id, "version", version)
    state = singer.write_bookmark(state, stream_id, "replication_key", replication_key)
    stream_utils.write_state_message(state)
    stream_utils.write_schema_message(stream, bookmark_properties=[replication_key])
    # Incremental never truncates the target: the version does not change
    # across runs, so activating it every run is safe.
    singer.write_message(singer.ActivateVersionMessage(stream=dest_stream, version=version))

    db.log_encodings(connection)
    hstore = db.register_hstore_if_available(connection)
    LOGGER.info("hstore %s available", "is" if hstore else "is not")

    bookmark_value = singer.get_bookmark(state, stream_id, "replication_key_value")
    sql = sqlgen.incremental_sql(
        stream,
        schema_name,
        columns,
        replication_key,
        key_datatype,
        has_bookmark=bookmark_value is not None,
        limit=config.get("limit"),
    )
    params = (bookmark_value,) if bookmark_value is not None else None
    LOGGER.info("Running %s", sql)

    datatypes = {column: stream_utils.sql_datatype_for_column(stream, column) for column in columns}
    time_extracted = utils.now()
    rows_saved = 0
    cursor = db.named_cursor(connection, cfg.itersize(config))
    try:
        cursor.execute(sql, params)
        with metrics.record_counter(dest_stream) as counter:
            for row in cursor:
                record = {
                    column: selected_value_to_singer_value(value, datatypes[column])
                    for column, value in zip(columns, row, strict=True)
                }
                singer.write_message(
                    singer.RecordMessage(
                        stream=dest_stream,
                        record=record,
                        version=version,
                        time_extracted=time_extracted,
                    )
                )
                counter.increment()
                rows_saved += 1
                key_value = record.get(replication_key)
                if key_value is not None:
                    # A NULL key never poisons the bookmark (SPEC §6.2).
                    state = singer.write_bookmark(
                        state, stream_id, "replication_key_value", key_value
                    )
                if rows_saved % STATE_UPDATE_PERIOD == 0:
                    stream_utils.write_state_message(state)
    finally:
        cursor.close()

    return state
