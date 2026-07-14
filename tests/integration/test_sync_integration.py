"""End-to-end sync against a real PostgreSQL server (SPEC §8.3, §8.4)."""

import copy
import json

import psycopg2
import pytest

from tap_postgres import db, discovery, logical, sync
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


def records(messages):
    return [m for m in messages if type(m).__name__ == "RecordMessage"]


def final_state(messages):
    return [m for m in messages if type(m).__name__ == "StateMessage"][-1].value


class TestFullTable:
    def test_end_to_end(self, superuser_connection, tap_config, test_schema, emitted_messages):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".people (id integer PRIMARY KEY, name text);
                INSERT INTO "{test_schema}".people VALUES (1, 'ann'), (2, 'bob');
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="FULL_TABLE")]}

        state = sync.do_sync(tap_config, catalog, {}, None)

        assert message_types(emitted_messages) == [
            "StateMessage",
            "SchemaMessage",
            "ActivateVersionMessage",
            "RecordMessage",
            "RecordMessage",
            "ActivateVersionMessage",
            "StateMessage",
            "StateMessage",
        ]
        assert [r.record for r in records(emitted_messages)] == [
            {"id": 1, "name": "ann"},
            {"id": 2, "name": "bob"},
        ]
        assert records(emitted_messages)[0].stream == f"{test_schema}-people"
        bookmark = state["bookmarks"][f"{test_schema}-people"]
        assert bookmark["last_replication_method"] == "FULL_TABLE"
        assert "xmin" not in bookmark

        # A second run must not emit the initial ACTIVATE_VERSION.
        emitted_messages.clear()
        sync.do_sync(tap_config, catalog, copy.deepcopy(state), None)
        assert message_types(emitted_messages) == [
            "StateMessage",
            "SchemaMessage",
            "RecordMessage",
            "RecordMessage",
            "ActivateVersionMessage",
            "StateMessage",
            "StateMessage",
        ]


class TestIncremental:
    def test_inclusive_bound_and_bookmarks(
        self, superuser_connection, tap_config, test_schema, emitted_messages
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

        state = sync.do_sync(tap_config, catalog, {}, None)
        # NULL replication keys are only captured while no bookmark exists
        # (ASC ordering puts them last), and never poison the bookmark.
        assert [r.record["id"] for r in records(emitted_messages)] == [1, 2, 3]
        bookmark = state["bookmarks"][f"{test_schema}-orders"]
        assert bookmark["replication_key_value"] == "2024-01-02T00:00:00+00:00"

        with superuser_connection.cursor() as cur:
            cur.execute(f"INSERT INTO \"{test_schema}\".orders VALUES (4, '2024-01-03T00:00:00Z')")
        emitted_messages.clear()
        state = sync.do_sync(tap_config, catalog, copy.deepcopy(state), None)
        # Inclusive >= re-emits the last-seen row; the NULL-key row is invisible.
        assert [r.record["id"] for r in records(emitted_messages)] == [2, 4]
        assert (
            state["bookmarks"][f"{test_schema}-orders"]["replication_key_value"]
            == "2024-01-03T00:00:00+00:00"
        )

    def test_limit_setting(self, superuser_connection, tap_config, test_schema, emitted_messages):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".limited (id integer PRIMARY KEY);
                INSERT INTO "{test_schema}".limited SELECT generate_series(1, 10);
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {
            "streams": [
                select_stream(stream, replication_method="INCREMENTAL", replication_key="id")
            ]
        }
        sync.do_sync({**tap_config, "limit": 4}, catalog, {}, None)
        assert [r.record["id"] for r in records(emitted_messages)] == [1, 2, 3, 4]


class TestLogBased:
    @pytest.fixture
    def replication_slot(self, superuser_connection, tap_config, test_schema):
        tap_config["tap_id"] = test_schema
        slot = logical.generate_slot_name(tap_config["dbname"], test_schema)
        with superuser_connection.cursor() as cur:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'wal2json')", (slot,))
        yield slot
        with superuser_connection.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))

    def test_end_to_end(
        self,
        superuser_connection,
        tap_config,
        test_schema,
        emitted_messages,
        replication_slot,
        tmp_path,
    ):
        tap_config["logical_poll_total_seconds"] = 2
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".events (
                    id integer PRIMARY KEY, label text, tags text[]
                );
                INSERT INTO "{test_schema}".events VALUES (1, 'seed', '{{a,b}}');
                """
            )
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="LOG_BASED")]}
        stream_id = f"{test_schema}-events"

        # Phase 1: initial snapshot.
        state = sync.do_sync(tap_config, catalog, {}, None)
        snapshot_records = records(emitted_messages)
        assert [r.record["id"] for r in snapshot_records] == [1]
        bookmark = state["bookmarks"][stream_id]
        assert "lsn" in bookmark
        assert "xmin" not in bookmark
        committed_lsn = bookmark["lsn"]

        # Phase 2: apply changes, then stream them from the WAL.
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO "{test_schema}".events VALUES (2, 'born', '{{x y,z}}');
                UPDATE "{test_schema}".events SET label = 'renamed' WHERE id = 1;
                DELETE FROM "{test_schema}".events WHERE id = 2;
                """
            )
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(state))
        emitted_messages.clear()

        state = sync.do_sync(tap_config, catalog, copy.deepcopy(state), state_file)

        assert message_types(emitted_messages)[0] == "SchemaMessage"
        schema = emitted_messages[0].schema
        assert schema["properties"]["_sdc_deleted_at"]["format"] == "date-time"

        wal_records = records(emitted_messages)
        assert [r.record["id"] for r in wal_records] == [2, 1, 2]
        insert, update, delete = wal_records
        assert insert.record["label"] == "born"
        assert insert.record["tags"] == ["x y", "z"]
        assert insert.record["_sdc_deleted_at"] is None
        assert update.record["label"] == "renamed"
        assert delete.record["_sdc_deleted_at"] is not None
        assert "label" not in delete.record  # only replica-identity columns

        # LSN bookmarks move monotonically forward.
        lsns = [
            m.value["bookmarks"][stream_id]["lsn"]
            for m in emitted_messages
            if type(m).__name__ == "StateMessage"
        ]
        assert lsns == sorted(lsns)
        assert lsns[-1] >= committed_lsn

        # Flush safety (SPEC §8.4): the slot's confirmed position never
        # advances beyond what the committed state file says.
        with superuser_connection.cursor() as cur:
            cur.execute(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s",
                (replication_slot,),
            )
            confirmed = logical.lsn_to_int(cur.fetchone()[0])
        assert confirmed <= committed_lsn

    def test_log_based_on_view_is_fatal(
        self, superuser_connection, tap_config, test_schema, emitted_messages
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".base (id integer PRIMARY KEY);
                CREATE VIEW "{test_schema}".base_view AS SELECT id FROM "{test_schema}".base;
                """
            )
        streams = discover_streams(tap_config)
        view = next(s for s in streams if s["stream"] == "base_view")
        catalog = {"streams": [select_stream(view, replication_method="LOG_BASED")]}
        with pytest.raises(sync.SyncError, match="not supported for view"):
            sync.do_sync(tap_config, catalog, {}, None)


class TestUseSecondary:
    def _wait_for_replica_table(self, pg_stack, test_schema, table):
        import time

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            connection = psycopg2.connect(
                host=pg_stack["secondary_host"],
                port=pg_stack["secondary_port"],
                user=pg_stack["user"],
                password=pg_stack["password"],
                dbname=pg_stack["dbname"],
            )
            try:
                if db.fetch_scalar(
                    connection,
                    "SELECT to_regclass(%s)",
                    (f'"{test_schema}"."{table}"',),
                ):
                    return
            finally:
                connection.close()
            time.sleep(0.5)
        raise RuntimeError("table never appeared on the replica")

    def test_snapshot_reads_from_replica(
        self, superuser_connection, tap_config, pg_stack, test_schema, emitted_messages
    ):
        if not pg_stack.get("secondary_host"):
            pytest.skip("no replica configured")
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".repl_read (id integer PRIMARY KEY);
                INSERT INTO "{test_schema}".repl_read VALUES (1);
                """
            )
        self._wait_for_replica_table(pg_stack, test_schema, "repl_read")
        config = {
            **tap_config,
            "use_secondary": True,
            "secondary_host": pg_stack["secondary_host"],
            "secondary_port": pg_stack["secondary_port"],
        }
        [stream] = discover_streams(config)
        catalog = {"streams": [select_stream(stream, replication_method="FULL_TABLE")]}
        sync.do_sync(config, catalog, {}, None)
        assert [r.record["id"] for r in records(emitted_messages)] == [1]

    def test_reads_actually_route_to_secondary(
        self, superuser_connection, tap_config, pg_stack, test_schema, emitted_messages
    ):
        """A bogus replica address must break FULL_TABLE reads: proof of routing."""
        with superuser_connection.cursor() as cur:
            cur.execute(f'CREATE TABLE "{test_schema}".routed (id integer PRIMARY KEY)')
        [stream] = discover_streams(tap_config)
        catalog = {"streams": [select_stream(stream, replication_method="FULL_TABLE")]}
        config = {
            **tap_config,
            "use_secondary": True,
            "secondary_host": "localhost",
            "secondary_port": 59999,
        }
        with pytest.raises(psycopg2.OperationalError):
            sync.do_sync(config, catalog, {}, None)
