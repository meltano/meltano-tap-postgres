"""State reset on replication-method / key change (SPEC §4.2, §8.2)."""

import itertools

import pytest

from tap_postgres.sync import reset_state_on_method_change

METHODS = ("FULL_TABLE", "INCREMENTAL", "LOG_BASED")

BOOKMARKS_BY_METHOD = {
    "FULL_TABLE": {"version": 1000, "xmin": 42},
    "INCREMENTAL": {
        "version": 1000,
        "replication_key": "updated_at",
        "replication_key_value": "2024-01-01T00:00:00+00:00",
    },
    "LOG_BASED": {"version": 1000, "lsn": 60071389168},
}


def make_state(method):
    return {
        "bookmarks": {
            "public-t": {"last_replication_method": method, **BOOKMARKS_BY_METHOD[method]}
        }
    }


class TestMethodChanges:
    @pytest.mark.parametrize("method", METHODS)
    def test_same_method_persists_bookmarks(self, method):
        state = make_state(method)
        expected = dict(state["bookmarks"]["public-t"])
        state = reset_state_on_method_change(state, "public-t", method, "updated_at")
        assert state["bookmarks"]["public-t"] == expected

    @pytest.mark.parametrize(
        ("old", "new"),
        [(a, b) for a, b in itertools.product(METHODS, METHODS) if a != b],
    )
    def test_all_six_directions_wipe_bookmarks(self, old, new):
        state = make_state(old)
        state = reset_state_on_method_change(state, "public-t", new, "updated_at")
        assert state["bookmarks"]["public-t"] == {"last_replication_method": new}

    def test_last_replication_method_recorded_on_fresh_stream(self):
        state = {"bookmarks": {}}
        state = reset_state_on_method_change(state, "public-t", "FULL_TABLE", None)
        assert state["bookmarks"]["public-t"] == {"last_replication_method": "FULL_TABLE"}


class TestIncrementalKeyChanges:
    def test_key_change_wipes_bookmarks(self):
        state = make_state("INCREMENTAL")
        state = reset_state_on_method_change(state, "public-t", "INCREMENTAL", "created_at")
        assert state["bookmarks"]["public-t"] == {"last_replication_method": "INCREMENTAL"}

    def test_key_change_wipes_mid_interruption(self):
        state = make_state("INCREMENTAL")
        # an interrupted run still has currently_syncing set; the wipe is unconditional
        state["currently_syncing"] = "public-t"
        state = reset_state_on_method_change(state, "public-t", "INCREMENTAL", "created_at")
        assert state["bookmarks"]["public-t"] == {"last_replication_method": "INCREMENTAL"}

    def test_same_key_persists_bookmarks(self):
        state = make_state("INCREMENTAL")
        expected = dict(state["bookmarks"]["public-t"])
        state = reset_state_on_method_change(state, "public-t", "INCREMENTAL", "updated_at")
        assert state["bookmarks"]["public-t"] == expected
