"""Fakes and fixtures for unit tests: no database required (SPEC §8.1)."""

import pytest
import singer


class FakeCursor:
    """A cursor that replays scripted rows and records executed SQL."""

    def __init__(self, connection, rows=None, results=None):
        self.connection = connection
        self.rows = rows if rows is not None else []
        # `results` maps a SQL substring to the fetchone()/fetchall() result rows.
        self.results = results or {}
        self.executed = []
        self.itersize = None
        self._current = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.connection.executed.append((sql, params))
        for fragment, rows in self.results.items():
            if fragment in sql:
                self._current = list(rows)
                return
        self._current = list(self.rows)

    def __iter__(self):
        return iter(self._current)

    def fetchone(self):
        return self._current.pop(0) if self._current else None

    def fetchall(self):
        current, self._current = self._current, []
        return current

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConnection:
    """Stands in for a psycopg2 connection in strategy tests."""

    def __init__(self, rows=None, results=None, server_version=160000):
        self.rows = rows if rows is not None else []
        self.results = results or {
            "server_encoding": [("UTF8",)],
            "client_encoding": [("UTF8",)],
            "pg_type": [],
        }
        self.executed = []
        self.server_version = server_version
        self.closed = False

    def cursor(self, name=None, cursor_factory=None):
        return FakeCursor(self, rows=self.rows, results=self.results)

    def close(self):
        self.closed = True


@pytest.fixture
def emitted_messages(monkeypatch):
    """Capture every Singer message written by the tap."""
    messages = []
    monkeypatch.setattr(singer, "write_message", messages.append)
    monkeypatch.setattr(singer.messages, "write_message", messages.append)
    return messages


@pytest.fixture
def no_db_side_effects(monkeypatch):
    """Neutralize db helpers that talk to a real server."""
    from tap_postgres import db

    monkeypatch.setattr(db, "log_encodings", lambda conn: None)
    monkeypatch.setattr(db, "register_hstore_if_available", lambda conn: False)


def make_stream(
    *,
    schema_name="public",
    table_name="test_table",
    properties=None,
    column_metadata=None,
    stream_metadata=None,
):
    """Build a minimal catalog stream entry."""
    if properties is None:
        properties = {
            "id": {"type": ["integer"]},
            "name": {"type": ["null", "string"]},
        }
    if column_metadata is None:
        column_metadata = {
            "id": {
                "sql-datatype": "integer",
                "inclusion": "automatic",
                "selected-by-default": True,
            },
            "name": {"sql-datatype": "text", "inclusion": "available", "selected-by-default": True},
        }
    metadata = [
        {
            "breadcrumb": [],
            "metadata": {
                "table-key-properties": ["id"],
                "schema-name": schema_name,
                "database-name": "test_db",
                "row-count": 0,
                "is-view": False,
                "selected": True,
                **(stream_metadata or {}),
            },
        }
    ]
    for column, md in column_metadata.items():
        metadata.append({"breadcrumb": ["properties", column], "metadata": md})
    return {
        "table_name": table_name,
        "stream": table_name,
        "tap_stream_id": f"{schema_name}-{table_name}",
        "schema": {"type": "object", "properties": properties, "definitions": {}},
        "metadata": metadata,
    }
