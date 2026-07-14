"""BATCH mode configuration and Arrow batch writing (MEL-541)."""

import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from tap_postgres import adbc
from tap_postgres.batch import DEFAULT_BATCH_SIZE, ArrowBatchWriter, BatchConfig, BatchConfigError


def read_arrow_file(batch_message):
    path = batch_message["manifest"][0].removeprefix("file://")
    with ipc.open_file(path) as reader:
        return reader.read_all()


class TestBatchConfig:
    def test_absent_key_means_record_mode(self):
        assert BatchConfig.from_config({"host": "db"}) is None

    def test_empty_dict_opts_in_with_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))
        batch_config = BatchConfig.from_config({"batch_config": {}})
        assert batch_config == BatchConfig(
            batch_size=DEFAULT_BATCH_SIZE, root_dir=str(tmp_path), format="arrow"
        )

    def test_nested_sdk_shape_is_parsed(self, tmp_path):
        batch_config = BatchConfig.from_config({
            "batch_config": {
                "encoding": {"format": "arrow"},
                "storage": {"root": str(tmp_path)},
                "batch_size": 1000,
            }
        })
        assert batch_config == BatchConfig(batch_size=1000, root_dir=str(tmp_path))

    def test_jsonl_format_is_deferred(self, tmp_path):
        with pytest.raises(BatchConfigError, match="deferred"):
            BatchConfig(batch_size=1, root_dir=str(tmp_path), format="jsonl")

    def test_non_positive_batch_size_is_rejected(self, tmp_path):
        with pytest.raises(BatchConfigError, match="positive integer"):
            BatchConfig(batch_size=0, root_dir=str(tmp_path))

    def test_missing_root_dir_is_rejected(self, tmp_path):
        with pytest.raises(BatchConfigError, match="not a directory"):
            BatchConfig(batch_size=1, root_dir=str(tmp_path / "nope"))


class TestArrowBatchWriter:
    def _batch(self, ids):
        return pa.RecordBatch.from_pydict({"id": ids})

    def _config(self, tmp_path, batch_size):
        return BatchConfig(batch_size=batch_size, root_dir=str(tmp_path))

    def test_buffers_until_batch_size_then_emits(self, tmp_path):
        output = io.StringIO()
        writer = ArrowBatchWriter("public-people", self._config(tmp_path, 4), output=output)

        assert writer.write(self._batch([1, 2])) is False
        assert output.getvalue() == ""
        assert writer.write(self._batch([3, 4])) is True

        [message] = [json.loads(line) for line in output.getvalue().splitlines()]
        assert message["type"] == "BATCH"
        assert message["stream"] == "public-people"
        assert message["encoding"] == {"format": "arrow"}
        assert read_arrow_file(message).column("id").to_pylist() == [1, 2, 3, 4]

    def test_flush_emits_partial_batch(self, tmp_path):
        output = io.StringIO()
        writer = ArrowBatchWriter("public-people", self._config(tmp_path, 100), output=output)
        writer.write(self._batch([1]))
        writer.flush()

        [message] = [json.loads(line) for line in output.getvalue().splitlines()]
        assert read_arrow_file(message).column("id").to_pylist() == [1]

    def test_flush_without_rows_is_a_no_op(self, tmp_path):
        output = io.StringIO()
        writer = ArrowBatchWriter("public-people", self._config(tmp_path, 100), output=output)
        writer.flush()
        assert output.getvalue() == ""
        assert list(tmp_path.iterdir()) == []

    def test_empty_record_batch_is_ignored(self, tmp_path):
        output = io.StringIO()
        writer = ArrowBatchWriter("public-people", self._config(tmp_path, 1), output=output)
        assert writer.write(self._batch([])) is False
        writer.flush()
        assert output.getvalue() == ""

    def test_writer_resets_between_batches(self, tmp_path):
        output = io.StringIO()
        writer = ArrowBatchWriter("public-people", self._config(tmp_path, 2), output=output)
        writer.write(self._batch([1, 2]))
        writer.write(self._batch([3, 4]))

        messages = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(messages) == 2
        assert [read_arrow_file(m).column("id").to_pylist() for m in messages] == [[1, 2], [3, 4]]


class TestConnectionUri:
    def _config(self, **overrides):
        return {
            "host": "db.example.com",
            "port": 5432,
            "user": "tap",
            "password": "secret",
            "dbname": "warehouse",
            **overrides,
        }

    def test_basic_uri(self):
        assert adbc.connection_uri(self._config()) == (
            "postgresql://tap:secret@db.example.com:5432/warehouse"
            "?application_name=tap-postgres&connect_timeout=30"
        )

    def test_credentials_are_percent_encoded(self):
        uri = adbc.connection_uri(self._config(user="t@p", password="p@ss/w:rd"))
        assert uri.startswith("postgresql://t%40p:p%40ss%2Fw%3Ard@db.example.com:5432/")

    def test_ssl_adds_sslmode(self):
        assert "sslmode=require" in adbc.connection_uri(self._config(ssl="true"))

    def test_use_secondary_routes_to_replica(self):
        uri = adbc.connection_uri(
            self._config(use_secondary=True, secondary_host="replica", secondary_port=5433)
        )
        assert "@replica:5433/" in uri

    def test_dbname_override(self):
        assert "/other_db?" in adbc.connection_uri(self._config(), dbname="other_db")
