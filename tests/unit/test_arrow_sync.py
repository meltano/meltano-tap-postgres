"""Arrow BATCH sync paths for FULL_TABLE and INCREMENTAL (MEL-541)."""

import contextlib
import datetime
import json

import pyarrow as pa
import pytest

from tap_postgres import adbc, full_table, incremental, sync
from tap_postgres.batch import ArrowBatchSource, BatchConfig
from tests.unit.conftest import make_stream
from tests.unit.test_batch import read_arrow_file

CONFIG = {"host": "db", "port": 5432, "user": "u", "password": "p", "dbname": "d"}


@pytest.fixture
def fake_reader(monkeypatch):
    """Replace adbc.stream_record_batches with a scripted reader; records SQL/params."""
    script = {"batches": [], "executed": []}

    @contextlib.contextmanager
    def stream_record_batches(config, sql, params=None, *, dbname=None):
        script["executed"].append((sql, params, dbname))
        yield iter(script["batches"])

    monkeypatch.setattr(adbc, "stream_record_batches", stream_record_batches)
    return script


def batch_messages(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def message_types(messages):
    return [type(m).__name__ for m in messages]


def states(messages):
    return [m.value for m in messages if type(m).__name__ == "StateMessage"]


class TestFullTableArrow:
    def _run(self, tmp_path, fake_reader, state=None, batch_size=100, first_run=True):
        stream = make_stream()
        source = ArrowBatchSource(BatchConfig(batch_size=batch_size, root_dir=str(tmp_path)))
        state = state if state is not None else {"bookmarks": {}}
        return full_table.sync_table(
            source,
            stream,
            state,
            CONFIG,
            ["id", "name"],
            version=7,
            first_run=first_run,
        )

    def test_emits_batch_files_without_the_xmin_column(
        self, tmp_path, fake_reader, emitted_messages, capsys
    ):
        fake_reader["batches"] = [
            pa.RecordBatch.from_pydict({"xmin": ["10", "11"], "id": [1, 2], "name": ["a", "b"]}),
            pa.RecordBatch.from_pydict({"xmin": ["12"], "id": [3], "name": ["c"]}),
        ]
        state = self._run(tmp_path, fake_reader)

        [message] = batch_messages(capsys)
        assert message["type"] == "BATCH"
        assert message["stream"] == "public-test_table"
        table = read_arrow_file(message)
        assert table.schema.names == ["id", "name"]
        assert table.column("id").to_pylist() == [1, 2, 3]

        # First-run ACTIVATE_VERSION, then the closing ACTIVATE_VERSION.
        assert message_types(emitted_messages) == [
            "ActivateVersionMessage",
            "ActivateVersionMessage",
        ]
        # The resume watermark is dropped once the copy completes.
        assert "xmin" not in state["bookmarks"]["public-test_table"]

    def test_checkpoints_state_only_after_a_published_batch(
        self, tmp_path, fake_reader, emitted_messages, capsys
    ):
        fake_reader["batches"] = [
            pa.RecordBatch.from_pydict({"xmin": ["10", "11"], "id": [1, 2], "name": ["a", "b"]}),
            pa.RecordBatch.from_pydict({"xmin": ["12"], "id": [3], "name": ["c"]}),
        ]
        self._run(tmp_path, fake_reader, batch_size=2)

        assert len(batch_messages(capsys)) == 2
        # One mid-run STATE: after the first batch flushed; its watermark
        # matches the last row of the published file, never ahead of it.
        [mid_state] = states(emitted_messages)
        assert mid_state["bookmarks"]["public-test_table"]["xmin"] == 11

    def test_resuming_binds_the_xmin_watermark_as_dollar_param(self, tmp_path, fake_reader):
        state = {"bookmarks": {"public-test_table": {"version": 7, "xmin": 11}}}
        self._run(tmp_path, fake_reader, state=state, first_run=False)

        [(sql, params, dbname)] = fake_reader["executed"]
        assert "WHERE age(xmin::xid) <= age($1::text::xid)" in sql
        assert params == ("11",)
        assert dbname is None

    def test_view_has_no_xmin_handling(self, tmp_path, fake_reader, emitted_messages, capsys):
        stream = make_stream(stream_metadata={"is-view": True, "view-key-properties": ["id"]})
        fake_reader["batches"] = [pa.RecordBatch.from_pydict({"id": [1], "name": ["a"]})]
        state = full_table.sync_table(
            ArrowBatchSource(BatchConfig(batch_size=100, root_dir=str(tmp_path))),
            stream,
            {"bookmarks": {}},
            CONFIG,
            ["id", "name"],
            version=7,
            first_run=True,
        )

        [(sql, _, _)] = fake_reader["executed"]
        assert "xmin" not in sql
        [message] = batch_messages(capsys)
        assert read_arrow_file(message).schema.names == ["id", "name"]
        assert "xmin" not in state["bookmarks"].get("public-test_table", {})


class TestIncrementalArrow:
    def _stream(self):
        return make_stream(
            properties={
                "id": {"type": ["integer"]},
                "updated_at": {"type": ["null", "string"], "format": "date-time"},
            },
            column_metadata={
                "id": {"sql-datatype": "integer", "inclusion": "automatic"},
                "updated_at": {
                    "sql-datatype": "timestamp with time zone",
                    "inclusion": "available",
                },
            },
            stream_metadata={"replication-key": "updated_at"},
        )

    def _run(self, tmp_path, state=None, batch_size=100):
        return incremental.sync_table(
            ArrowBatchSource(BatchConfig(batch_size=batch_size, root_dir=str(tmp_path))),
            self._stream(),
            state if state is not None else {"bookmarks": {}},
            CONFIG,
            ["id", "updated_at"],
        )

    def _batch(self, ids, timestamps):
        return pa.RecordBatch.from_pydict({
            "id": pa.array(ids, pa.int32()),
            "updated_at": pa.array(timestamps, pa.timestamp("us", tz="UTC")),
        })

    def test_emits_batches_and_converts_the_key_bookmark(
        self, tmp_path, fake_reader, emitted_messages, capsys
    ):
        fake_reader["batches"] = [
            self._batch(
                [1, 2],
                [
                    datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2026, 7, 2, tzinfo=datetime.timezone.utc),
                ],
            )
        ]
        state = self._run(tmp_path)

        [message] = batch_messages(capsys)
        assert read_arrow_file(message).column("id").to_pylist() == [1, 2]

        bookmark = state["bookmarks"]["public-test_table"]
        assert bookmark["replication_key"] == "updated_at"
        # Bookmarked through the Singer value conversion: an ISO-8601 string.
        assert bookmark["replication_key_value"] == "2026-07-02T00:00:00+00:00"

        # SCHEMA + initial STATE + ACTIVATE_VERSION as in RECORD mode.
        assert message_types(emitted_messages) == [
            "StateMessage",
            "SchemaMessage",
            "ActivateVersionMessage",
        ]

    def test_bookmark_survives_a_null_key_tail(self, tmp_path, fake_reader):
        fake_reader["batches"] = [
            self._batch([1], [datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)]),
            self._batch([2, 3], [None, None]),
        ]
        state = self._run(tmp_path)
        bookmark = state["bookmarks"]["public-test_table"]
        assert bookmark["replication_key_value"] == "2026-07-01T00:00:00+00:00"

    def test_existing_bookmark_binds_as_dollar_param(self, tmp_path, fake_reader):
        state = {
            "bookmarks": {
                "public-test_table": {
                    "version": 7,
                    "replication_key": "updated_at",
                    "replication_key_value": "2026-07-01T00:00:00+00:00",
                }
            }
        }
        self._run(tmp_path, state=state)

        [(sql, params, _)] = fake_reader["executed"]
        assert 'WHERE "updated_at" >= $1::timestamp with time zone' in sql
        assert params == ("2026-07-01T00:00:00+00:00",)


class TestSyncDispatch:
    def test_batch_mode_opens_no_psycopg2_connection(
        self, tmp_path, fake_reader, emitted_messages, capsys, monkeypatch
    ):
        def explode(*args, **kwargs):
            raise AssertionError("psycopg2 connection must not be opened in BATCH mode")

        monkeypatch.setattr(sync.db, "open_connection", explode)
        stream = make_stream(stream_metadata={"replication-method": "FULL_TABLE"})
        fake_reader["batches"] = [pa.RecordBatch.from_pydict({"xmin": ["1"], "id": [1]})]

        config = {**CONFIG, "batch_config": {"storage": {"root": str(tmp_path)}}}
        batch_config = BatchConfig.from_config(config)
        state = sync.sync_traditional_stream(
            config, stream, {"bookmarks": {}}, "FULL_TABLE", None, batch_config=batch_config
        )

        assert len(batch_messages(capsys)) == 1
        assert state["bookmarks"]["public-test_table"]["version"] is not None
