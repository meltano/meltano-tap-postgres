"""Arrow BATCH mode against a real PostgreSQL server via ADBC (MEL-541).

These tests exercise the actual ADBC connection path (tap_postgres/adbc.py),
which the unit tests fake out: real driver connectivity, ``$1`` parameter
binding through server-side prepared statements, and the Arrow types the
driver produces for real tables.
"""

import copy
import datetime
import json

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from tap_postgres import db, discovery, sync
from tests.integration.conftest import select_stream

pytestmark = pytest.mark.integration


def discover_streams(config):
    connection = db.open_connection(config)
    try:
        return discovery.discover_streams(
            connection, itersize=1000, filter_schemas=[config["filter_schemas"]]
        )
    finally:
        connection.close()


def message_types(messages):
    return [type(m).__name__ for m in messages]


def batch_messages(capsys):
    """BATCH messages are written straight to stdout; Singer messages are captured
    by the emitted_messages fixture, so stdout holds only BATCH lines."""
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def read_arrow_table(batch_message):
    path = batch_message["manifest"][0].removeprefix("file://")
    with ipc.open_file(path) as reader:
        return reader.read_all()


def rows_by_id(capsys):
    rows = {}
    for message in batch_messages(capsys):
        table = read_arrow_table(message)
        for row in table.to_pylist():
            rows[row["id"]] = row
    return rows


class TestFullTableArrow:
    def test_end_to_end(
        self, superuser_connection, tap_config, test_schema, emitted_messages, capsys, tmp_path
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".people (
                    id integer PRIMARY KEY, name text, height numeric(5, 2), born timestamptz
                );
                INSERT INTO "{test_schema}".people VALUES
                    (1, 'ann', 1.70, '1990-01-01T00:00:00Z'),
                    (2, 'bob', NULL, NULL);
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="FULL_TABLE")]}
        config = {**tap_config, "batch_config": {"storage": {"root": str(tmp_path)}}}

        state = sync.do_sync(config, catalog, {}, None)

        # The Singer message flow matches RECORD mode, minus the RECORDs.
        assert message_types(emitted_messages) == [
            "StateMessage",
            "SchemaMessage",
            "ActivateVersionMessage",
            "ActivateVersionMessage",
            "StateMessage",
            "StateMessage",
        ]

        messages = batch_messages(capsys)
        assert len(messages) >= 1
        for message in messages:
            assert message["type"] == "BATCH"
            assert message["stream"] == f"{test_schema}-people"
            assert message["encoding"] == {"format": "arrow"}

        [table] = [read_arrow_table(m) for m in messages]
        # The xmin watermark column never reaches the batch file.
        assert set(table.schema.names) == {"id", "name", "height", "born"}
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["ann", "bob"]

        bookmark = state["bookmarks"][f"{test_schema}-people"]
        assert bookmark["last_replication_method"] == "FULL_TABLE"
        assert "xmin" not in bookmark

    def test_date_column_lands_as_timestamp_not_date32(
        self, superuser_connection, tap_config, test_schema, emitted_messages, capsys, tmp_path
    ):
        # A `date` column's SCHEMA message declares `format: date-time` (discovery.py
        # groups `date` with the timestamp types), but ADBC's own Arrow type for
        # Postgres `date` is `date32` unless the extraction query itself casts it --
        # select_expression does that specifically so this mismatch can't reach a
        # downstream target that trusts the declared format literally (e.g. Snowflake
        # refusing to cast a semi-structured DATE value straight to TIMESTAMP_NTZ).
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".accounts (id integer PRIMARY KEY, signup_date date);
                INSERT INTO "{test_schema}".accounts VALUES (1, '2025-05-08'), (2, NULL);
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="FULL_TABLE")]}
        config = {**tap_config, "batch_config": {"storage": {"root": str(tmp_path)}}}

        sync.do_sync(config, catalog, {}, None)

        [message] = batch_messages(capsys)
        table = read_arrow_table(message)
        assert pa.types.is_timestamp(table.schema.field("signup_date").type)
        rows = {row["id"]: row["signup_date"] for row in table.to_pylist()}
        assert rows[1] == datetime.datetime(2025, 5, 8)
        assert rows[2] is None

    def test_view_sync(
        self, superuser_connection, tap_config, test_schema, emitted_messages, capsys, tmp_path
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".base (id integer PRIMARY KEY);
                INSERT INTO "{test_schema}".base SELECT generate_series(1, 5);
                CREATE VIEW "{test_schema}".base_view AS SELECT id FROM "{test_schema}".base;
                """
            )
        streams = discover_streams(tap_config)
        [view_stream] = [s for s in streams if s["stream"] == "base_view"]
        catalog = {
            "streams": [
                select_stream(
                    view_stream,
                    replication_method="FULL_TABLE",
                    extra={"view-key-properties": ["id"]},
                )
            ]
        }
        config = {**tap_config, "batch_config": {"storage": {"root": str(tmp_path)}}}

        sync.do_sync(config, catalog, {}, None)

        assert sorted(rows_by_id(capsys)) == [1, 2, 3, 4, 5]


class TestIncrementalArrow:
    def test_bookmarks_and_resumption(
        self, superuser_connection, tap_config, test_schema, emitted_messages, capsys, tmp_path
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".orders (
                    id integer PRIMARY KEY, updated_at timestamptz
                );
                INSERT INTO "{test_schema}".orders VALUES
                    (1, '2024-01-01T00:00:00Z'),
                    (2, '2024-01-02T00:00:00Z'),
                    (3, NULL);
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {
            "streams": [
                select_stream(
                    stream, replication_method="INCREMENTAL", replication_key="updated_at"
                )
            ]
        }
        config = {**tap_config, "batch_config": {"storage": {"root": str(tmp_path)}}}

        state = sync.do_sync(config, catalog, {}, None)

        assert "RecordMessage" not in message_types(emitted_messages)
        # NULL replication keys are captured while no bookmark exists (ASC
        # ordering puts them last) and never poison the bookmark.
        assert sorted(rows_by_id(capsys)) == [1, 2, 3]
        bookmark = state["bookmarks"][f"{test_schema}-orders"]
        assert bookmark["replication_key"] == "updated_at"
        assert bookmark["replication_key_value"] == "2024-01-02T00:00:00+00:00"

        with superuser_connection.cursor() as cur:
            cur.execute(f"INSERT INTO \"{test_schema}\".orders VALUES (4, '2024-01-03T00:00:00Z')")
        emitted_messages.clear()

        # The bookmark binds as a $1 prepared-statement parameter over ADBC;
        # inclusive >= re-emits the last-seen row and the NULL-key row is invisible.
        state = sync.do_sync(config, catalog, copy.deepcopy(state), None)
        assert sorted(rows_by_id(capsys)) == [2, 4]
        assert (
            state["bookmarks"][f"{test_schema}-orders"]["replication_key_value"]
            == "2024-01-03T00:00:00+00:00"
        )


class TestLogBasedStaysRecordMode:
    def test_snapshot_emits_records_not_batches(
        self, superuser_connection, tap_config, test_schema, emitted_messages, capsys, tmp_path
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".events (id integer PRIMARY KEY);
                INSERT INTO "{test_schema}".events VALUES (1), (2);
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="LOG_BASED")]}
        config = {**tap_config, "batch_config": {"storage": {"root": str(tmp_path)}}}

        state = sync.do_sync(config, catalog, {}, None)

        # The initial snapshot runs in RECORD mode despite batch_config.
        assert message_types(emitted_messages).count("RecordMessage") == 2
        assert batch_messages(capsys) == []
        assert state["bookmarks"][f"{test_schema}-events"]["lsn"] is not None
