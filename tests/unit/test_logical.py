"""Logical replication unit coverage (SPEC §8.2), no database required."""

import datetime
import json
from decimal import Decimal

import psycopg2
import pytest
import singer

from tap_postgres import db, discovery, logical
from tap_postgres.logical import (
    FALLBACK_DATE,
    FALLBACK_TIMESTAMP,
    LogicalReplicationError,
    LogicalSession,
    UnsupportedPayloadError,
    add_automatic_properties,
    build_add_tables,
    escape_wal2json_name,
    fetch_current_lsn,
    generate_slot_name,
    int_to_lsn,
    locate_replication_slot,
    lsn_to_int,
    parse_logical_date,
    parse_logical_timestamp,
    validate_server_version,
)
from tests.unit.conftest import FakeConnection, make_stream


class TestLsnConversion:
    @pytest.mark.parametrize(
        ("text", "integer"),
        [
            ("0/0", 0),
            ("0/1", 1),
            ("16/B374D848", 0x16B374D848),
            ("FF/FFFFFFFF", (0xFF << 32) + 0xFFFFFFFF),
        ],
    )
    def test_round_trip(self, text, integer):
        assert lsn_to_int(text) == integer
        assert lsn_to_int(int_to_lsn(integer)) == integer

    def test_null_values(self):
        assert lsn_to_int(None) is None
        assert int_to_lsn(None) is None


class TestSlotNames:
    def test_basic(self):
        assert generate_slot_name("some_db") == "tap_postgres_some_db"

    def test_lowercased_and_sanitized(self):
        assert generate_slot_name("Some-DB.9") == "tap_postgres_some_db_9"

    def test_tap_id_suffix(self):
        assert generate_slot_name("db", "My Pipe") == "tap_postgres_db_my_pipe"

    def _connection_with_slots(self, slots):
        return FakeConnection(results={"pg_replication_slots": [(s,) for s in slots]})

    def test_lookup_prefers_unsuffixed_name(self):
        class RecordingCursor:
            def __init__(self):
                self.queries = []
                self._hit = None

            def execute(self, sql, params):
                self.queries.append(params[0])
                self._hit = (params[0],) if params[0] == "tap_postgres_db" else None

            def fetchone(self):
                return self._hit

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        cursor = RecordingCursor()
        assert logical.locate_replication_slot_by_cur(cursor, "db", "pipe") == "tap_postgres_db"
        assert cursor.queries == ["tap_postgres_db"]

    def test_lookup_falls_back_to_suffixed_name(self):
        class RecordingCursor:
            def __init__(self):
                self.queries = []
                self._hit = None

            def execute(self, sql, params):
                self.queries.append(params[0])
                self._hit = (params[0],) if params[0] == "tap_postgres_db_pipe" else None

            def fetchone(self):
                return self._hit

        cursor = RecordingCursor()
        assert (
            logical.locate_replication_slot_by_cur(cursor, "db", "pipe") == "tap_postgres_db_pipe"
        )
        assert cursor.queries == ["tap_postgres_db", "tap_postgres_db_pipe"]

    def test_lookup_error_when_no_slot(self):
        connection = self._connection_with_slots([])
        with pytest.raises(LogicalReplicationError, match="replication slot"):
            locate_replication_slot(connection, "db")


class TestWal2JsonTables:
    def test_escaping(self):
        assert escape_wal2json_name("plain") == "plain"
        assert escape_wal2json_name("has space") == r"has\ space"
        assert escape_wal2json_name("a,b.c*d'e") == r"a\,b\.c\*d\'e"

    def test_build_add_tables(self):
        streams = [
            make_stream(schema_name="public", table_name="orders"),
            make_stream(schema_name="other schema", table_name="tab.le"),
        ]
        assert build_add_tables(streams) == r"public.orders,other\ schema.tab\.le"


class TestVersionGate:
    @pytest.mark.parametrize("version", [90399, 80400, 90000])
    def test_pre_94_unsupported(self, version):
        with pytest.raises(LogicalReplicationError, match=r"9.4 or newer"):
            validate_server_version(version)

    @pytest.mark.parametrize(
        "version", [90400, 90420, 90500, 90515, 90600, 90611, 100000, 100006, 110000, 110001]
    )
    def test_buggy_versions_refused(self, version):
        with pytest.raises(LogicalReplicationError, match="known WAL bug"):
            validate_server_version(version)

    @pytest.mark.parametrize("version", [90421, 90516, 90612, 100007, 110002, 120000, 160003])
    def test_fixed_versions_allowed(self, version):
        validate_server_version(version)

    def test_lsn_function_on_modern_servers(self, monkeypatch):
        connection = FakeConnection(
            results={"pg_current_wal_lsn": [("16/B374D848",)]}, server_version=160000
        )
        monkeypatch.setattr(db, "open_connection", lambda *a, **k: connection)
        monkeypatch.setattr(logical, "db", db)
        assert fetch_current_lsn({}) == 0x16B374D848

    def test_lsn_function_before_10(self, monkeypatch):
        connection = FakeConnection(
            results={"pg_current_xlog_location": [("0/A",)]}, server_version=90621
        )
        monkeypatch.setattr(db, "open_connection", lambda *a, **k: connection)
        assert fetch_current_lsn({}) == 10


class TestAutomaticProperties:
    def test_deleted_at_always_added(self):
        stream = make_stream()
        add_automatic_properties(stream, debug_lsn=False)
        assert stream["schema"]["properties"]["_sdc_deleted_at"] == {
            "type": ["null", "string"],
            "format": "date-time",
        }
        assert "_sdc_lsn" not in stream["schema"]["properties"]

    def test_lsn_added_with_debug_lsn(self):
        stream = make_stream()
        add_automatic_properties(stream, debug_lsn=True)
        assert stream["schema"]["properties"]["_sdc_lsn"] == {"type": ["null", "string"]}


class TestTimestampDecoding:
    def test_in_range_naive(self):
        assert (
            parse_logical_timestamp("2024-03-04 05:06:07.123456")
            == "2024-03-04T05:06:07.123456+00:00"
        )

    def test_in_range_with_offset(self):
        assert parse_logical_timestamp("2024-03-04 05:06:07+02") == "2024-03-04T05:06:07+02:00"

    def test_native_datetime(self):
        assert parse_logical_timestamp(datetime.datetime(2024, 1, 1)) == "2024-01-01T00:00:00+00:00"

    def test_min_value(self):
        assert parse_logical_timestamp("0001-01-01 00:00:00") == "0001-01-01T00:00:00+00:00"

    def test_max_value(self):
        assert (
            parse_logical_timestamp("9999-12-31 23:59:59.999") == "9999-12-31T23:59:59.999000+00:00"
        )

    def test_beyond_max_falls_back(self):
        assert parse_logical_timestamp("9999-12-31 23:59:59.9999") == FALLBACK_TIMESTAMP

    def test_year_10000_falls_back(self):
        assert parse_logical_timestamp("10000-01-01 00:00:00") == FALLBACK_TIMESTAMP

    def test_bc_era_falls_back(self):
        assert parse_logical_timestamp("0044-03-15 12:00:00 BC") == FALLBACK_TIMESTAMP

    def test_unparseable_falls_back(self):
        assert parse_logical_timestamp("not a timestamp") == FALLBACK_TIMESTAMP

    def test_offset_pushing_past_max_falls_back(self):
        assert parse_logical_timestamp("9999-12-31 23:30:00-05") == FALLBACK_TIMESTAMP


class TestDateDecoding:
    def test_in_range(self):
        assert parse_logical_date("2024-03-04") == "2024-03-04T00:00:00+00:00"

    def test_native_date(self):
        assert parse_logical_date(datetime.date(2024, 3, 4)) == "2024-03-04T00:00:00+00:00"

    def test_year_above_9999_falls_back(self):
        assert parse_logical_date("10000-01-01") == FALLBACK_DATE

    def test_other_parse_failures_fatal(self):
        with pytest.raises(ValueError, match="Invalid isoformat"):
            parse_logical_date("not-a-date")


class TestValueDecoding:
    def decode(self, value, datatype, helper=None):
        return logical.selected_value_to_singer_value(
            value, datatype, lambda: helper or FakeConnection()
        )

    def test_numeric_to_decimal(self):
        assert self.decode("1.500", "numeric") == Decimal("1.500")
        assert self.decode("NaN", "numeric") is None

    def test_money_string_as_is(self):
        assert self.decode("$1,001.00", "money") == "$1,001.00"

    def test_bit(self):
        assert self.decode("1", "bit") is True
        assert self.decode("0", "bit") is False

    def test_json_parsed(self):
        assert self.decode('{"a": 1}', "jsonb") == {"a": 1}

    def test_integer_from_string(self):
        assert self.decode("42", "bigint") == 42

    def test_time_with_tz(self):
        assert self.decode("13:14:15+05", "time with time zone") == "08:14:15"

    def test_hstore_reconstruction_via_server(self):
        helper = FakeConnection(results={"hstore_to_array": [(["k1", "v1", "k2", None],)]})
        assert self.decode("k1=>v1,k2=>NULL", "hstore", helper) == {"k1": "v1", "k2": None}

    def test_array_reconstruction_native_cast(self):
        helper = FakeConnection(results={"::integer[]": [([1, 2, 3],)]})
        assert self.decode("{1,2,3}", "integer[]", helper) == [1, 2, 3]
        assert "SELECT %s::integer[]" in helper.executed[-1][0]

    def test_array_reconstruction_falls_back_to_text(self):
        helper = FakeConnection(results={"::text[]": [(["1.5", "2.5"],)]})
        assert self.decode("{1.5,2.5}", "numeric[]", helper) == [Decimal("1.5"), Decimal("2.5")]
        assert "SELECT %s::text[]" in helper.executed[-1][0]

    def test_nested_array(self):
        helper = FakeConnection(results={"::integer[]": [([[1, 2], [3, 4]],)]})
        assert self.decode("{{1,2},{3,4}}", "integer[]", helper) == [[1, 2], [3, 4]]

    def test_unknown_class_raises(self):
        with pytest.raises(LogicalReplicationError, match="Cannot decode value"):
            self.decode(object(), "sometype")

    @pytest.mark.parametrize(
        ("value", "datatype", "expected"),
        [
            ("2024-01-01 00:00:00", "timestamp without time zone", "2024-01-01T00:00:00+00:00"),
            ("2024-01-01 00:00:00+02", "timestamp with time zone", "2024-01-01T00:00:00+02:00"),
            ("2024-01-01", "date", "2024-01-01T00:00:00+00:00"),
            ("13:14:15", "time without time zone", "13:14:15"),
            ("t", "boolean", True),
            ("false", "boolean", False),
            (True, "boolean", True),
            ("1.5", "double precision", 1.5),
            ("NaN", "real", None),
            ("Infinity", "double precision", None),
            ("12.34", "numeric(10,2)", Decimal("12.34")),
        ],
    )
    def test_dispatch_by_datatype(self, value, datatype, expected):
        assert self.decode(value, datatype) == expected

    def test_hstore_dict_passes_through(self):
        assert self.decode({"a": "b"}, "hstore") == {"a": "b"}

    def test_undecodable_numeric_raises(self):
        with pytest.raises(LogicalReplicationError, match="Cannot decode numeric"):
            self.decode("not-a-number", "numeric")

    def test_aware_timestamp_beyond_max_falls_back(self):
        # In range as a datetime, but past the sentinel ceiling once in UTC.
        assert (
            self.decode("9999-12-31 23:59:59.9999+00", "timestamp with time zone")
            == FALLBACK_TIMESTAMP
        )


def logical_stream(**kwargs):
    return make_stream(
        properties={
            "id": {"type": ["integer"]},
            "name": {"type": ["null", "string"]},
        },
        column_metadata={
            "id": {"sql-datatype": "integer", "inclusion": "automatic"},
            "name": {"sql-datatype": "text", "inclusion": "available"},
        },
        stream_metadata={"replication-method": "LOG_BASED"},
        **kwargs,
    )


def make_session(state=None, config=None, streams=None, state_file=None):
    streams = streams or [logical_stream()]
    state = state or {
        "bookmarks": {s["tap_stream_id"]: {"version": 111, "lsn": 1000} for s in streams}
    }
    return LogicalSession(
        config or {},
        streams,
        state,
        end_lsn=10_000,
        state_file=state_file,
        dbname="test_db",
    )


def payload(action="I", schema="public", table="test_table", columns=None, identity=None):
    message = {"action": action, "schema": schema, "table": table}
    if columns is not None:
        message["columns"] = columns
    if identity is not None:
        message["identity"] = identity
    return json.dumps(message)


class TestMessageConsumption:
    def test_non_json_payload_keeps_state(self, emitted_messages):
        session = make_session()
        before = json.loads(json.dumps(session.state))
        session.consume_message("not json{", 2000)
        assert session.state["bookmarks"] == before["bookmarks"]
        assert emitted_messages == []

    def test_unselected_stream_ignored(self, emitted_messages):
        session = make_session()
        session.consume_message(payload(table="other_table", columns=[]), 2000)
        assert emitted_messages == []

    def test_unsupported_action_raises(self):
        session = make_session()
        with pytest.raises(UnsupportedPayloadError, match="'T'"):
            session.consume_message(payload(action="T", columns=[]), 2000)

    def test_insert_record_assembly(self, emitted_messages):
        session = make_session()
        session.consume_message(
            payload(columns=[{"name": "id", "value": 1}, {"name": "name", "value": "a"}]), 2000
        )
        [record] = emitted_messages
        assert record.record == {"id": 1, "name": "a", "_sdc_deleted_at": None}
        assert record.version == 111
        assert singer.get_bookmark(session.state, "public-test_table", "lsn") == 2000

    def test_delete_uses_identity_and_sets_deleted_at(self, emitted_messages):
        session = make_session()
        session.consume_message(payload(action="D", identity=[{"name": "id", "value": 7}]), 2000)
        [record] = emitted_messages
        assert record.record["id"] == 7
        assert record.record["_sdc_deleted_at"] is not None

    def test_debug_lsn_adds_lsn_string(self, emitted_messages):
        session = make_session(config={"debug_lsn": "true"})
        session.consume_message(payload(columns=[{"name": "id", "value": 1}]), 2000)
        assert emitted_messages[0].record["_sdc_lsn"] == "2000"

    def test_missing_version_raises(self):
        state = {"bookmarks": {"public-test_table": {"lsn": 1000}}}
        session = make_session(state=state)
        with pytest.raises(LogicalReplicationError, match="version"):
            session.consume_message(payload(columns=[{"name": "id", "value": 1}]), 2000)

    def test_missing_column_datatype_raises(self):
        stream = logical_stream()
        # column present in the schema but stripped of sql-datatype metadata
        stream["schema"]["properties"]["ghost"] = {"type": ["null", "string"]}
        stream["metadata"].append({
            "breadcrumb": ["properties", "ghost"],
            "metadata": {"inclusion": "available"},
        })
        session = make_session(streams=[stream])
        with pytest.raises(LogicalReplicationError, match="sql-datatype"):
            session.consume_message(payload(columns=[{"name": "ghost", "value": "x"}]), 2000)

    def test_new_columns_trigger_schema_refresh(self, emitted_messages, monkeypatch):
        refreshed = []

        def fake_refresh(config, streams, dbname=None):
            refreshed.append(dbname)
            for stream in streams:
                stream["schema"]["properties"]["brand_new"] = {"type": ["null", "string"]}
                stream["metadata"].append({
                    "breadcrumb": ["properties", "brand_new"],
                    "metadata": {"sql-datatype": "text", "inclusion": "available"},
                })

        monkeypatch.setattr(discovery, "refresh_streams_schema", fake_refresh)
        session = make_session()
        session.consume_message(
            payload(columns=[{"name": "id", "value": 1}, {"name": "brand_new", "value": "x"}]),
            2000,
        )
        assert refreshed == ["test_db"]
        assert [type(m).__name__ for m in emitted_messages] == [
            "SchemaMessage",
            "RecordMessage",
        ]
        assert "_sdc_deleted_at" in emitted_messages[0].schema["properties"]
        assert emitted_messages[1].record["brand_new"] == "x"


class TestFlushControl:
    def test_lsn_fully_processed_only_after_higher_lsn(self, emitted_messages):
        session = make_session()
        session.record_processed(2000)
        assert session.last_fully_processed_lsn is None
        session.record_processed(2000)  # chunked output shares the LSN
        assert session.last_fully_processed_lsn is None
        session.record_processed(3000)
        assert session.last_fully_processed_lsn == 2000

    def test_finalize_writes_last_fully_processed(self, emitted_messages):
        session = make_session()
        session.record_processed(2000)
        session.record_processed(3000)
        state = session.finalize()
        assert singer.get_bookmark(state, "public-test_table", "lsn") == 2000
        assert type(emitted_messages[-1]).__name__ == "StateMessage"

    def test_finalize_never_regresses_below_committed(self, emitted_messages):
        session = make_session()
        session.committed_lsn = 5000
        session.record_processed(2000)
        session.record_processed(3000)
        state = session.finalize()
        assert singer.get_bookmark(state, "public-test_table", "lsn") == 5000

    def test_finalize_without_messages_keeps_bookmarks(self, emitted_messages):
        session = make_session()
        state = session.finalize()
        assert singer.get_bookmark(state, "public-test_table", "lsn") == 1000


class FakeMessage:
    def __init__(self, payload_text, lsn):
        self.payload = payload_text
        self.data_start = lsn


class FakeReplicationCursor:
    def __init__(self, script, fail_start=False):
        self.script = list(script)
        self.fail_start = fail_start
        self.start_kwargs = None
        self.feedback = []

    def start_replication(self, **kwargs):
        if self.fail_start:
            msg = "replication slot is active for PID 123"
            raise psycopg2.ProgrammingError(msg)
        self.start_kwargs = kwargs

    def read_message(self):
        if self.script:
            return self.script.pop(0)
        return None

    def send_feedback(self, write_lsn=0, flush_lsn=0, apply_lsn=0, reply=False, force=False):
        self.feedback.append({"flush_lsn": flush_lsn, "force": force})

    def close(self):
        pass


class FakeReplicationConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


@pytest.fixture
def fast_select(monkeypatch):
    monkeypatch.setattr(logical.select, "select", lambda *args: ([], [], []))


def run_session(
    monkeypatch,
    script,
    config=None,
    state=None,
    fail_start=False,
    end_lsn=10_000,
    state_file=None,
):
    stream = logical_stream()
    state = state or {"bookmarks": {"public-test_table": {"version": 111, "lsn": 1000}}}
    replication_cursor = FakeReplicationCursor(script, fail_start=fail_start)

    def fake_open_connection(
        cfg_dict, primary=False, dbname=None, connection_factory=None, extra_options=None
    ):
        if connection_factory is not None:
            return FakeReplicationConnection(replication_cursor)
        return FakeConnection(
            results={"pg_replication_slots": [("tap_postgres_test_db",)]},
            server_version=160000,
        )

    monkeypatch.setattr(logical.db, "open_connection", fake_open_connection)
    config = {"logical_poll_total_seconds": 0.4, **(config or {})}
    state = logical.sync_logical_streams(
        config,
        [stream],
        state,
        end_lsn,
        state_file=state_file,
        dbname="test_db",
    )
    return state, replication_cursor


class TestSessionLoop:
    def test_stops_on_idle_poll_timeout(self, emitted_messages, monkeypatch, fast_select):
        _state, _cursor = run_session(monkeypatch, script=[])
        assert type(emitted_messages[-1]).__name__ == "StateMessage"

    def test_stops_on_max_runtime(self, emitted_messages, monkeypatch, fast_select):
        _state, _cursor = run_session(
            monkeypatch,
            script=[],
            config={"max_run_seconds": 1, "logical_poll_total_seconds": 3600},
        )
        assert type(emitted_messages[-1]).__name__ == "StateMessage"

    def test_break_at_end_lsn_skips_message(self, emitted_messages, monkeypatch, fast_select):
        beyond = FakeMessage(payload(columns=[{"name": "id", "value": 1}]), 20_000)
        _state, _cursor = run_session(monkeypatch, script=[beyond])
        assert not any(type(m).__name__ == "RecordMessage" for m in emitted_messages)

    def test_replication_start_failure_propagates(self, monkeypatch, fast_select, emitted_messages):
        with pytest.raises(LogicalReplicationError, match="Unable to start replication"):
            run_session(monkeypatch, script=[], fail_start=True)

    def test_consumes_messages_and_writes_final_state(
        self, emitted_messages, monkeypatch, fast_select
    ):
        script = [
            FakeMessage(payload(columns=[{"name": "id", "value": 1}]), 2000),
            FakeMessage(payload(columns=[{"name": "id", "value": 2}]), 3000),
        ]
        state, cursor = run_session(monkeypatch, script=script)
        records = [m for m in emitted_messages if type(m).__name__ == "RecordMessage"]
        assert [r.record["id"] for r in records] == [1, 2]
        # 3000 was never followed by a higher LSN, so the final bookmark is 2000
        assert singer.get_bookmark(state, "public-test_table", "lsn") == 2000
        # first message flushed min(committed=1000, first message LSN=2000)
        assert cursor.feedback[0] == {"flush_lsn": 1000, "force": True}
        assert cursor.start_kwargs["slot_name"] == "tap_postgres_test_db"
        assert cursor.start_kwargs["options"]["format-version"] == "2"
        assert cursor.start_kwargs["options"]["add-tables"] == "public.test_table"
        assert cursor.start_kwargs["start_lsn"] == 1000


class TestUnselectedColumns:
    def test_payload_columns_not_selected_are_dropped(self, emitted_messages):
        stream = logical_stream()
        # In the schema (so no drift refresh fires) but never synced.
        stream["schema"]["properties"]["secret"] = {}
        stream["metadata"].append({
            "breadcrumb": ["properties", "secret"],
            "metadata": {"sql-datatype": "bytea", "inclusion": "unsupported"},
        })
        session = make_session(streams=[stream])
        session.consume_message(
            payload(columns=[{"name": "id", "value": 1}, {"name": "secret", "value": "x"}]),
            2000,
        )
        [record] = emitted_messages
        assert "secret" not in record.record
        assert record.record["id"] == 1


class TestBookmarkCadence:
    def test_state_written_every_n_fully_processed_advances(self, emitted_messages, monkeypatch):
        monkeypatch.setattr(logical, "UPDATE_BOOKMARK_PERIOD", 2)
        session = make_session()
        session.record_processed(2000)
        session.record_processed(3000)  # advance 1 (2000 fully processed)
        assert emitted_messages == []
        session.record_processed(4000)  # advance 2: write bookmarks + STATE
        [state_message] = emitted_messages
        assert type(state_message).__name__ == "StateMessage"
        assert state_message.value["bookmarks"]["public-test_table"]["lsn"] == 3000


class TestStateFileFlush:
    """Flush control (SPEC §6.3.7): only advance past target-committed data."""

    def _session(self, tmp_path, committed_lsn=1000):
        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps({"bookmarks": {"public-test_table": {"version": 111, "lsn": committed_lsn}}})
        )
        session = make_session(state_file=state_file)
        cursor = FakeReplicationCursor([])
        return session, state_file, cursor

    def test_no_state_file_never_flushes(self):
        session = make_session()
        cursor = FakeReplicationCursor([])
        session.refresh_committed_lsn_from_state_file(cursor)
        assert cursor.feedback == []

    def test_advanced_committed_lsn_is_flushed(self, tmp_path):
        session, state_file, cursor = self._session(tmp_path)
        session.record_processed(2000)
        state_file.write_text(
            json.dumps({"bookmarks": {"public-test_table": {"version": 111, "lsn": 1500}}})
        )
        session.refresh_committed_lsn_from_state_file(cursor)
        assert cursor.feedback == [{"flush_lsn": 1500, "force": True}]
        assert session.flushed_lsn == 1500
        assert session.committed_lsn == 1500

    def test_committed_ahead_of_current_message_not_flushed(self, tmp_path):
        session, state_file, cursor = self._session(tmp_path)
        session.record_processed(2000)
        state_file.write_text(
            json.dumps({"bookmarks": {"public-test_table": {"version": 111, "lsn": 3000}}})
        )
        session.refresh_committed_lsn_from_state_file(cursor)
        assert cursor.feedback == []
        # ...but the committed position is still remembered.
        assert session.committed_lsn == 3000

    def test_unchanged_committed_lsn_not_reflushed(self, tmp_path):
        session, state_file, cursor = self._session(tmp_path)
        session.record_processed(2000)
        state_file.write_text(
            json.dumps({"bookmarks": {"public-test_table": {"version": 111, "lsn": 1500}}})
        )
        session.refresh_committed_lsn_from_state_file(cursor)
        session.refresh_committed_lsn_from_state_file(cursor)
        assert len(cursor.feedback) == 1

    def test_unreadable_state_file_ignored_silently(self, tmp_path):
        session, state_file, cursor = self._session(tmp_path)
        state_file.write_text("{not json")
        session.refresh_committed_lsn_from_state_file(cursor)
        assert cursor.feedback == []

    def test_state_file_without_lsn_bookmarks_ignored(self, tmp_path):
        session, state_file, cursor = self._session(tmp_path)
        state_file.write_text(json.dumps({"bookmarks": {}}))
        session.refresh_committed_lsn_from_state_file(cursor)
        assert cursor.feedback == []


class TestSessionLoopFeedback:
    def test_periodic_feedback_flushes_state_file_progress(
        self, emitted_messages, monkeypatch, fast_select, tmp_path
    ):
        monkeypatch.setattr(logical, "FEEDBACK_INTERVAL_SECONDS", 0)
        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps({"bookmarks": {"public-test_table": {"version": 111, "lsn": 1200}}})
        )
        script = [FakeMessage(payload(columns=[{"name": "id", "value": 1}]), 2000)]
        _state, cursor = run_session(monkeypatch, script=script, state_file=state_file)
        # The periodic branch sent keepalives and flushed to the committed LSN.
        assert {"flush_lsn": 0, "force": True} in cursor.feedback
        assert {"flush_lsn": 1200, "force": True} in cursor.feedback
