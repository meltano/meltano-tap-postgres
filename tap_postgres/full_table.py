"""FULL_TABLE replication (SPEC §6.1)."""

import time
from typing import Any

import singer
from singer import metrics, utils

from tap_postgres import config as cfg
from tap_postgres import db, sqlgen, stream_utils
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
    connection: Any,
    stream: dict[str, Any],
    state: dict[str, Any],
    config: dict[str, Any],
    columns: list[str],
    version: int,
    first_run: bool,
) -> dict[str, Any]:
    """Stream every row of the table, resumably, and activate the version at the end."""
    stream_id = stream["tap_stream_id"]
    dest_stream = stream_utils.dest_stream_name(stream)
    schema_name = stream_utils.schema_name(stream)
    view = stream_utils.is_view(stream)

    db.log_encodings(connection)
    hstore = db.register_hstore_if_available(connection)
    LOGGER.info("hstore %s available", "is" if hstore else "is not")

    if first_run:
        # Let the target start a clean slate before the first copy.
        singer.write_message(singer.ActivateVersionMessage(stream=dest_stream, version=version))

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

    # Copy complete: drop the resume watermark, then let the target discard
    # rows from older versions.
    state = singer.clear_bookmark(state, stream_id, "xmin")
    singer.write_message(singer.ActivateVersionMessage(stream=dest_stream, version=version))
    return state
