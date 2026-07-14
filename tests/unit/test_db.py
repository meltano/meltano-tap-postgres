"""Connection kwargs routing and cursor helpers (SPEC §2.3)."""

from tap_postgres import db
from tests.unit.conftest import FakeConnection

CONFIG = {
    "host": "primary.example.com",
    "port": 5432,
    "user": "u",
    "password": "p",
    "dbname": "db",
}

SECONDARY = {
    **CONFIG,
    "use_secondary": True,
    "secondary_host": "replica.example.com",
    "secondary_port": 5433,
}


class TestConnectionDsnKwargs:
    def test_defaults(self):
        kwargs = db.connection_dsn_kwargs(CONFIG)
        assert kwargs["host"] == "primary.example.com"
        assert kwargs["application_name"] == "tap-postgres"
        assert kwargs["connect_timeout"] == 30
        assert "sslmode" not in kwargs

    def test_ssl_mode_require(self):
        kwargs = db.connection_dsn_kwargs({**CONFIG, "ssl": "true"})
        assert kwargs["sslmode"] == "require"

    def test_use_secondary_routes_to_replica(self):
        kwargs = db.connection_dsn_kwargs(SECONDARY)
        assert kwargs["host"] == "replica.example.com"
        assert kwargs["port"] == 5433

    def test_primary_forced_for_wal_operations(self):
        kwargs = db.connection_dsn_kwargs(SECONDARY, primary=True)
        assert kwargs["host"] == "primary.example.com"
        assert kwargs["port"] == 5432

    def test_extra_options_passed_through(self):
        kwargs = db.connection_dsn_kwargs(CONFIG, extra_options="-c wal_sender_timeout=1000")
        assert kwargs["options"] == "-c wal_sender_timeout=1000"


class TestNamedCursor:
    def test_itersize_applied(self):
        cursor = db.named_cursor(FakeConnection(), itersize=500)
        assert cursor.itersize == 500
