"""Sync orchestration against mocked strategies (SPEC §4.1, §4.3)."""

from unittest import mock

import pytest
import singer

from tap_postgres import discovery, logical, sync
from tests.unit.conftest import make_stream


class TestResolveReplicationMethod:
    def test_stream_metadata_wins(self):
        stream = make_stream(stream_metadata={"replication-method": "FULL_TABLE"})
        config = {"default_replication_method": "INCREMENTAL"}
        assert sync.resolve_replication_method(stream, config) == "FULL_TABLE"

    def test_falls_back_to_config_default(self):
        assert (
            sync.resolve_replication_method(
                make_stream(), {"default_replication_method": "LOG_BASED"}
            )
            == "LOG_BASED"
        )

    @pytest.mark.parametrize("method", [None, "MAGIC", "full_table"])
    def test_unrecognized_method_fatal(self, method):
        stream = make_stream(stream_metadata={"replication-method": method} if method else {})
        with pytest.raises(sync.SyncError, match="Unrecognized replication method"):
            sync.resolve_replication_method(stream, {})


class TestClassifyLogBasedWork:
    def _state(self, **bookmark):
        return {"bookmarks": {"public-t": bookmark}}

    def test_new_stream_gets_snapshot(self):
        assert sync.classify_log_based_work(self._state(), "public-t") == "snapshot"

    def test_interrupted_snapshot_resumes(self):
        state = self._state(xmin=42, lsn=1000)
        assert sync.classify_log_based_work(state, "public-t") == "snapshot"

    def test_lsn_only_streams_logically(self):
        assert sync.classify_log_based_work(self._state(lsn=1000), "public-t") == "logical"

    def test_xmin_without_lsn_is_inconsistent(self):
        with pytest.raises(sync.SyncError, match="inconsistent state"):
            sync.classify_log_based_work(self._state(xmin=42), "public-t")


class TestSyncTraditionalStream:
    def test_zero_selected_columns_skips_stream(self, emitted_messages, caplog):
        stream = make_stream(properties={}, column_metadata={})
        state = {"bookmarks": {}}
        with caplog.at_level("WARNING"):
            result = sync.sync_traditional_stream(
                {},
                stream,
                state,
                "FULL_TABLE",
                None,
            )
        assert result == state
        assert "No columns selected" in caplog.text
        assert emitted_messages == []

    def test_log_based_view_is_fatal(self, emitted_messages):
        stream = make_stream(stream_metadata={"is-view": True})
        with pytest.raises(sync.SyncError, match="not supported for view"):
            sync.sync_traditional_stream({}, stream, {"bookmarks": {}}, "LOG_BASED", 1000)


@pytest.fixture
def orchestrated(monkeypatch):
    """do_sync with every database touchpoint replaced by a recorder."""
    calls = {"traditional": [], "logical": []}

    def fake_traditional(config, stream, state, method, end_lsn, batch_config=None):
        calls["traditional"].append(stream["tap_stream_id"])
        return state

    def fake_logical(config, streams, state, end_lsn, state_file, dbname):
        calls["logical"].append({
            "streams": [s["tap_stream_id"] for s in streams],
            "dbname": dbname,
            "end_lsn": end_lsn,
        })
        return state

    monkeypatch.setattr(sync, "sync_traditional_stream", fake_traditional)
    monkeypatch.setattr(discovery, "refresh_streams_schema", lambda *a, **k: None)
    monkeypatch.setattr(logical, "fetch_current_lsn", lambda config: 99_999)
    monkeypatch.setattr(logical, "sync_logical_streams", fake_logical)
    return calls


CONFIG = {"dbname": "test_db"}


class TestDoSync:
    def test_nothing_selected(self, emitted_messages, caplog):
        catalog = {"streams": [make_stream(stream_metadata={"selected": False})]}
        with caplog.at_level("INFO"):
            state = sync.do_sync(CONFIG, catalog, {}, None)
        assert "No streams marked as selected" in caplog.text
        assert state == {"bookmarks": {}}

    def test_currently_syncing_moves_to_front(self, emitted_messages, orchestrated):
        catalog = {
            "streams": [
                make_stream(
                    table_name="alpha", stream_metadata={"replication-method": "FULL_TABLE"}
                ),
                make_stream(
                    table_name="omega", stream_metadata={"replication-method": "FULL_TABLE"}
                ),
            ]
        }
        state = {"bookmarks": {}, "currently_syncing": "public-omega"}
        sync.do_sync(CONFIG, catalog, state, None)
        assert orchestrated["traditional"] == ["public-omega", "public-alpha"]

    def test_currently_syncing_no_longer_selected_warns(
        self, emitted_messages, orchestrated, caplog
    ):
        catalog = {
            "streams": [
                make_stream(
                    table_name="alpha", stream_metadata={"replication-method": "FULL_TABLE"}
                ),
            ]
        }
        state = {"bookmarks": {}, "currently_syncing": "public-gone"}
        with caplog.at_level("WARNING"):
            sync.do_sync(CONFIG, catalog, state, None)
        assert "no longer selected" in caplog.text
        assert orchestrated["traditional"] == ["public-alpha"]

    def test_deselected_log_based_bookmarks_dropped_before_streaming(
        self, emitted_messages, orchestrated
    ):
        catalog = {
            "streams": [
                make_stream(
                    table_name="events", stream_metadata={"replication-method": "LOG_BASED"}
                ),
            ]
        }
        state = {
            "bookmarks": {
                # pure-logical stream: lsn bookmark only
                "public-events": {
                    "lsn": 5000,
                    "version": 1,
                    "last_replication_method": "LOG_BASED",
                },
                # de-selected LOG_BASED stream whose stale position must not
                # drag the slot's restart point backwards (SPEC §4.3)
                "public-gone": {"lsn": 10, "last_replication_method": "LOG_BASED"},
                # de-selected non-logical bookmarks are left alone
                "public-old-incremental": {"replication_key_value": "2024-01-01"},
            }
        }
        state = sync.do_sync(CONFIG, catalog, state, None)
        assert orchestrated["traditional"] == []
        assert orchestrated["logical"] == [
            {"streams": ["public-events"], "dbname": "test_db", "end_lsn": 99_999}
        ]
        assert "public-gone" not in state["bookmarks"]
        assert "public-old-incremental" in state["bookmarks"]

    def test_new_table_bookmark_survives_logical_phase(self, emitted_messages):
        """Regression test for https://github.com/transferwise/pipelinewise-tap-postgres/issues/107.

        A table added mid-run to an already-running LOG_BASED pipeline must keep
        its bookmark after the pre-existing tables' logical replication phase
        runs, rather than being silently dropped and re-snapshotted every run.
        """
        catalog = {
            "streams": [
                make_stream(
                    table_name="events", stream_metadata={"replication-method": "LOG_BASED"}
                ),
                make_stream(
                    table_name="orders", stream_metadata={"replication-method": "LOG_BASED"}
                ),
            ]
        }
        # "events" already completed its snapshot and streams logically;
        # "orders" was just added to the catalog and has no bookmark yet.
        state = {
            "bookmarks": {
                "public-events": {"lsn": 5000, "last_replication_method": "LOG_BASED"},
            }
        }

        def fake_traditional(config, stream, state, method, end_lsn, batch_config=None):
            # Mirrors sync_traditional_stream's LOG_BASED snapshot bookmark
            # write (tap_postgres/sync.py, `if ... lsn ... is None`): the new
            # table's initial snapshot bookmarks the end LSN before syncing.
            stream_id = stream["tap_stream_id"]
            if singer.get_bookmark(state, stream_id, "lsn") is None:
                state = singer.write_bookmark(state, stream_id, "lsn", end_lsn)
            return state

        def fake_logical(config, streams, state, end_lsn, state_file, dbname):
            # Mirrors LogicalSession.write_all_bookmarks/finalize
            # (tap_postgres/logical.py): only bookmarks the stream_ids it was
            # handed, it never rebuilds state["bookmarks"] from scratch.
            for stream in streams:
                state = singer.write_bookmark(state, stream["tap_stream_id"], "lsn", end_lsn)
            return state

        with (
            mock.patch.object(sync, "sync_traditional_stream", fake_traditional),
            mock.patch.object(discovery, "refresh_streams_schema", return_value=None),
            mock.patch.object(logical, "fetch_current_lsn", return_value=99_999),
            mock.patch.object(logical, "sync_logical_streams", fake_logical),
        ):
            state = sync.do_sync(CONFIG, catalog, state, None)

        assert state["bookmarks"]["public-orders"]["lsn"] == 99_999
        assert state["bookmarks"]["public-events"]["lsn"] == 99_999

        # A subsequent run must treat "orders" as pure-logical now, not
        # re-snapshot it.
        assert sync.classify_log_based_work(state, "public-orders") == "logical"
