"""INCREMENTAL replication (SPEC §6.2)."""

from __future__ import annotations

from typing import Any

import singer
from singer import metrics, utils

from tap_postgres import adbc, db, sqlgen, stream_utils
from tap_postgres import config as cfg
from tap_postgres.batch import ArrowBatchSource, ArrowBatchWriter
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
    source: db.ConnectionProtocol | ArrowBatchSource,
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
) -> dict[str, Any]:
    """Sync rows at or beyond the replication-key bookmark.

    ``source`` selects the read path: a psycopg2 connection emits per-row
    RECORD messages, an :class:`ArrowBatchSource` emits Arrow BATCH messages
    read over ADBC.
    """
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

    if isinstance(source, ArrowBatchSource):
        return _sync_table_arrow(
            stream,
            state,
            config,
            columns,
            replication_key,
            key_datatype,
            source,
        )

    db.log_encodings(source)
    hstore = db.register_hstore_if_available(source)
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
    cursor = db.named_cursor(source, cfg.itersize(config))
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


def _sync_table_arrow(
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
    replication_key: str,
    key_datatype: str,
    source: ArrowBatchSource,
) -> dict[str, Any]:
    """The BATCH-message path: stream Arrow record batches through ADBC (MEL-541).

    Record batches pass through to the batch files untouched - targets consume
    Arrow-native types. Only the replication-key bookmark goes through the
    Singer value conversion, so state files stay identical across RECORD and
    BATCH modes. A STATE message is emitted whenever a batch file has been
    published (never ahead of the emitted data).
    """
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)
    schema_name = stream_utils.schema_name(stream)

    bookmark_value = singer.get_bookmark(state, stream_id, "replication_key_value")
    sql = sqlgen.incremental_sql(
        stream,
        schema_name,
        columns,
        replication_key,
        key_datatype,
        has_bookmark=bookmark_value is not None,
        limit=config.get("limit"),
        placeholder="$1",
    )
    params = (bookmark_value,) if bookmark_value is not None else None
    LOGGER.info("Running (Arrow/ADBC) %s", sql)

    batch_writer = ArrowBatchWriter(dest_stream, source.batch_config)
    with (
        metrics.record_counter(dest_stream) as counter,
        adbc.stream_record_batches(config, sql, params, dbname=source.dbname) as reader,
    ):
        for record_batch in reader:
            if record_batch.num_rows == 0:
                continue
            counter.increment(record_batch.num_rows)

            # Write before bookmarking so a mid-write exception does not
            # advance the bookmark past the last successfully emitted batch.
            checkpoint = batch_writer.write(record_batch)

            # The query orders by the key ascending with NULLs last, so the
            # last non-null value is the batch's maximum; a NULL key never
            # poisons the bookmark (SPEC §6.2).
            key_values = record_batch.column(replication_key).drop_null()
            if len(key_values):
                key_value = selected_value_to_singer_value(
                    key_values[len(key_values) - 1].as_py(), key_datatype
                )
                state = singer.write_bookmark(state, stream_id, "replication_key_value", key_value)
            if checkpoint:
                stream_utils.write_state_message(state)

    batch_writer.flush()
    return state
