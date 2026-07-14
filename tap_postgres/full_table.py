"""FULL_TABLE replication (SPEC §6.1)."""

from __future__ import annotations

import time
from typing import Any

import singer
from singer import metrics, utils

from tap_postgres import adbc, db, sqlgen, stream_utils
from tap_postgres import config as cfg
from tap_postgres.batch import ArrowBatchSource, ArrowBatchWriter
from tap_postgres.conversion import selected_value_to_singer_value

LOGGER = singer.get_logger()

STATE_UPDATE_PERIOD = 1_000  # rows between STATE messages


def new_table_version() -> int:
    return int(time.time() * 1000)


def prepare(state: dict[str, Any], stream: dict[str, Any]) -> tuple[dict[str, Any], int, bool]:
    """Pick/reuse the table version and emit STATE + SCHEMA (SPEC §6.1 steps 1-3)."""
    stream_id = stream["tap_stream_id"]
    first_run = singer.get_bookmark(state, stream_id, "version") is None

    # An xmin bookmark means the previous copy was interrupted: reuse its version.
    if singer.get_bookmark(state, stream_id, "xmin") is not None:
        version = singer.get_bookmark(state, stream_id, "version")
    else:
        version = new_table_version()

    state = singer.write_bookmark(state, stream_id, "version", version)
    stream_utils.write_state_message(state)
    stream_utils.write_schema_message(stream, bookmark_properties=[])
    return state, version, first_run


def sync_table(
    source: db.ConnectionProtocol | ArrowBatchSource,
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
    version: int,
    first_run: bool,
) -> dict[str, Any]:
    """Stream every row of the table, resumably, and activate the version at the end.

    ``source`` selects the read path: a psycopg2 connection emits per-row
    RECORD messages, an :class:`ArrowBatchSource` emits Arrow BATCH messages
    read over ADBC.
    """
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)

    if first_run:
        # Let the target start a clean slate before the first copy.
        singer.write_message(singer.ActivateVersionMessage(stream=dest_stream, version=version))

    if isinstance(source, ArrowBatchSource):
        state = _sync_table_arrow(stream, state, config, columns, source)
    else:
        state = _sync_table_records(source, stream, state, config, columns, version)

    # Copy complete: drop the resume watermark, then let the target discard
    # rows from older versions.
    state = singer.clear_bookmark(state, stream_id, "xmin")
    singer.write_message(singer.ActivateVersionMessage(stream=dest_stream, version=version))
    return state


def _sync_table_records(
    connection: db.ConnectionProtocol,
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
    version: int,
) -> dict[str, Any]:
    """The RECORD-message path: stream rows through psycopg2 one by one."""
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)
    schema_name = stream_utils.schema_name(stream)
    view = stream_utils.is_view(stream)

    db.log_encodings(connection)
    hstore = db.register_hstore_if_available(connection)
    LOGGER.info("hstore %s available", "is" if hstore else "is not")

    resume_xmin = None if view else singer.get_bookmark(state, stream_id, "xmin")
    sql = sqlgen.full_table_sql(
        stream,
        schema_name,
        columns,
        is_view=view,
        resuming=resume_xmin is not None,
    )
    params = (str(resume_xmin),) if resume_xmin is not None else None
    LOGGER.info("Running %s", sql)

    datatypes = {column: stream_utils.sql_datatype_for_column(stream, column) for column in columns}
    time_extracted = utils.now()
    rows_saved = 0
    cursor = db.named_cursor(connection, cfg.itersize(config))
    try:
        cursor.execute(sql, params)
        with metrics.record_counter(dest_stream) as counter:
            for row in cursor:
                if view:
                    xmin_text, values = None, row
                else:
                    xmin_text, *values = row
                record = {
                    column: selected_value_to_singer_value(value, datatypes[column])
                    for column, value in zip(columns, values, strict=True)
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
                if xmin_text is not None:
                    state = singer.write_bookmark(state, stream_id, "xmin", int(xmin_text))
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
    source: ArrowBatchSource,
) -> dict[str, Any]:
    """The BATCH-message path: stream Arrow record batches through ADBC (MEL-541).

    Record batches pass through to the batch files untouched - targets consume
    Arrow-native types, so no per-value Singer conversion happens here. The
    xmin resume watermark is still maintained: each batch's last xmin is
    bookmarked, and a STATE message is emitted whenever a batch file has been
    published (never ahead of the emitted data).
    """
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)
    schema_name = stream_utils.schema_name(stream)
    view = stream_utils.is_view(stream)

    resume_xmin = None if view else singer.get_bookmark(state, stream_id, "xmin")
    sql = sqlgen.full_table_sql(
        stream,
        schema_name,
        columns,
        is_view=view,
        resuming=resume_xmin is not None,
        placeholder="$1",
    )
    params = (str(resume_xmin),) if resume_xmin is not None else None
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

            last_xmin = None
            if not view:
                # The first column is the xmin watermark: bookmark its last
                # value and keep it out of the published batch file.
                last_xmin = record_batch.column(0)[record_batch.num_rows - 1].as_py()
                record_batch = record_batch.select(list(range(1, record_batch.num_columns)))

            # Write before bookmarking so a mid-write exception does not
            # advance the watermark past the last successfully emitted batch.
            checkpoint = batch_writer.write(record_batch)
            if last_xmin is not None:
                state = singer.write_bookmark(state, stream_id, "xmin", int(last_xmin))
            if checkpoint:
                stream_utils.write_state_message(state)

    batch_writer.flush()
    return state
