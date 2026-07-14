"""FULL_TABLE / INCREMENTAL strategies against a mocked database (SPEC §8.2)."""

import datetime

import psycopg2.extras
import pytest
import singer

from tap_postgres import db, full_table, incremental
from tests.unit.conftest import FakeConnection, make_stream


def message_types(messages):
    return [type(message).__name__ for message in messages]


class TestFullTablePrepare:
    def test_first_run_picks_fresh_version(self, emitted_messages):
        state = {"bookmarks": {}}
        stream = make_stream()
        state, version, first_run = full_table.prepare(state, stream)
        assert first_run is True
        assert version == singer.get_bookmark(state, "public-test_table", "version")
        assert message_types(emitted_messages) == ["StateMessage", "SchemaMessage"]

    def test_interrupted_run_reuses_version(self, emitted_messages):
        state = {"bookmarks": {"public-test_table": {"version": 123, "xmin": 42}}}
        state, version, first_run = full_table.prepare(state, make_stream())
        assert version == 123
        assert first_run is False

    def test_completed_run_gets_new_version(self, emitted_messages):
        state = {"bookmarks": {"public-test_table": {"version": 123}}}
        state, version, first_run = full_table.prepare(state, make_stream())
        assert version != 123
        assert first_run is False


class TestFullTableSync:
    def _run(self, emitted_messages, rows, state=None, first_run=True):
        stream = make_stream()
        connection = FakeConnection(rows=rows)
        state = state or {"bookmarks": {}}
        state = full_table.sync_table(connection, stream, state, {}, ["id", "name"], 555, first_run)
        return state, connection

    def test_message_sequence_first_run(self, emitted_messages, no_db_side_effects):
        rows = [("101", 1, "a"), ("102", 2, "b")]
        _state, _ = self._run(emitted_messages, rows)
        assert message_types(emitted_messages) == [
            "ActivateVersionMessage",
            "RecordMessage",
            "RecordMessage",
            "ActivateVersionMessage",
        ]
        assert emitted_messages[1].record == {"id": 1, "name": "a"}
        assert emitted_messages[1].version == 555

    def test_no_initial_activate_version_on_resume(self, emitted_messages, no_db_side_effects):
        state = {"bookmarks": {"public-test_table": {"version": 555, "xmin": 100}}}
        state, connection = self._run(emitted_messages, [("101", 1, "a")], state, first_run=False)
        assert message_types(emitted_messages) == ["RecordMessage", "ActivateVersionMessage"]
        sql = connection.executed[-1][0]
        assert "age(xmin::xid) <= age(%s::text::xid)" in sql

    def test_xmin_cleared_when_copy_completes(self, emitted_messages, no_db_side_effects):
        state, _ = self._run(emitted_messages, [("101", 1, "a")])
        assert singer.get_bookmark(state, "public-test_table", "xmin") is None

    def test_state_cadence_every_1000_rows(self, emitted_messages, no_db_side_effects):
        rows = [(str(1000 + i), i, "x") for i in range(2500)]
        self._run(emitted_messages, rows)
        states = [m for m in emitted_messages if type(m).__name__ == "StateMessage"]
        assert len(states) == 2  # at exactly 1000 and 2000 rows
        assert states[0].value["bookmarks"]["public-test_table"]["xmin"] == 1999

    def test_view_is_plain_select_without_xmin(self, emitted_messages, no_db_side_effects):
        stream = make_stream(stream_metadata={"is-view": True, "view-key-properties": ["id"]})
        connection = FakeConnection(rows=[(1, "a")])  # no leading xmin column
        state = full_table.sync_table(
            connection, stream, {"bookmarks": {}}, {}, ["id", "name"], 555, True
        )
        sql = connection.executed[-1][0]
        assert "xmin" not in sql
        assert emitted_messages[1].record == {"id": 1, "name": "a"}
        assert "xmin" not in state["bookmarks"].get("public-test_table", {})


class TestHstoreRegistration:
    def test_registered_only_when_available(self, monkeypatch):
        calls = []
        monkeypatch.setattr(psycopg2.extras, "register_hstore", lambda conn: calls.append(conn))
        available = FakeConnection(results={"pg_type": [(1,)]})
        missing = FakeConnection(results={"pg_type": []})
        assert db.register_hstore_if_available(available) is True  # ty:ignore[invalid-argument-type]
        assert calls == [available]
        assert db.register_hstore_if_available(missing) is False  # ty:ignore[invalid-argument-type]
        assert calls == [available]


def incremental_stream():
    return make_stream(
        properties={"id": {}, "updated_at": {}},
        column_metadata={
            "id": {"sql-datatype": "integer", "inclusion": "automatic"},
            "updated_at": {
                "sql-datatype": "timestamp with time zone",
                "inclusion": "available",
            },
        },
        stream_metadata={"replication-method": "INCREMENTAL", "replication-key": "updated_at"},
    )


def ts(day):
    return datetime.datetime(2024, 1, day, tzinfo=datetime.timezone.utc)


class TestIncrementalSync:
    def _run(self, rows, state=None, config=None):
        state = state or {"bookmarks": {}}
        connection = FakeConnection(rows=rows)
        state = incremental.sync_table(
            connection, incremental_stream(), state, config or {}, ["id", "updated_at"]
        )
        return state, connection

    def test_message_sequence(self, emitted_messages, no_db_side_effects):
        _state, _ = self._run([(1, ts(1)), (2, ts(2))])
        assert message_types(emitted_messages) == [
            "StateMessage",
            "SchemaMessage",
            "ActivateVersionMessage",
            "RecordMessage",
            "RecordMessage",
        ]
        schema = emitted_messages[1]
        assert schema.bookmark_properties == ["updated_at"]

    def test_bookmark_is_last_emitted_key_value(self, emitted_messages, no_db_side_effects):
        state, _ = self._run([(1, ts(1)), (2, ts(2))])
        assert (
            singer.get_bookmark(state, "public-test_table", "replication_key_value")
            == "2024-01-02T00:00:00+00:00"
        )
        assert singer.get_bookmark(state, "public-test_table", "replication_key") == "updated_at"

    def test_null_key_never_bookmarked(self, emitted_messages, no_db_side_effects):
        state, _ = self._run([(1, ts(1)), (2, None)])
        assert (
            singer.get_bookmark(state, "public-test_table", "replication_key_value")
            == "2024-01-01T00:00:00+00:00"
        )

    def test_version_reused_across_runs(self, emitted_messages, no_db_side_effects):
        state = {"bookmarks": {"public-test_table": {"version": 777}}}
        state, _ = self._run([(1, ts(1))], state=state)
        assert singer.get_bookmark(state, "public-test_table", "version") == 777

    def test_bookmarked_run_filters_with_bound_parameter(
        self, emitted_messages, no_db_side_effects
    ):
        state = {
            "bookmarks": {
                "public-test_table": {
                    "version": 777,
                    "replication_key": "updated_at",
                    "replication_key_value": "2024-01-01T00:00:00+00:00",
                }
            }
        }
        state, connection = self._run([(2, ts(2))], state=state)
        sql, params = connection.executed[-1]
        assert 'WHERE "updated_at" >= %s::timestamp with time zone' in sql
        assert params == ("2024-01-01T00:00:00+00:00",)

    def test_unexpected_bookmark_keys_fatal(self, emitted_messages, no_db_side_effects):
        state = {"bookmarks": {"public-test_table": {"version": 1, "xmin": 5}}}
        with pytest.raises(incremental.ReplicationKeyError, match="xmin"):
            self._run([], state=state)

    def test_missing_replication_key_fatal(self, emitted_messages, no_db_side_effects):
        stream = make_stream()  # no replication-key metadata
        with pytest.raises(incremental.ReplicationKeyError, match="replication-key"):
            incremental.sync_table(FakeConnection(), stream, {"bookmarks": {}}, {}, ["id"])

    def test_replication_key_without_datatype_fatal(self, emitted_messages, no_db_side_effects):
        # The key exists in the schema but the catalog has no sql-datatype for it.
        stream = make_stream(stream_metadata={"replication-key": "ghost"})
        stream["schema"]["properties"]["ghost"] = {"type": ["null", "string"]}
        with pytest.raises(incremental.ReplicationKeyError, match="no sql-datatype"):
            incremental.sync_table(FakeConnection(), stream, {"bookmarks": {}}, {}, ["id"])

    def test_state_cadence_every_10000_rows(self, emitted_messages, no_db_side_effects):
        rows = [(i, ts(1)) for i in range(10000)]
        self._run(rows)
        states = [m for m in emitted_messages if type(m).__name__ == "StateMessage"]
        # one from preparation plus exactly one at the 10,000th row
        assert len(states) == 2
