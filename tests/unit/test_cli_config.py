"""CLI dispatch and configuration validation (SPEC §1.1, §2)."""

import json

import pytest

from tap_postgres import cli
from tap_postgres import config as cfg
from tap_postgres.config import ConfigurationError, validate_config

VALID = {
    "host": "localhost",
    "port": 5432,
    "user": "u",
    "password": "p",
    "dbname": "db",
}


class TestValidateConfig:
    def test_valid(self):
        validate_config(dict(VALID))

    @pytest.mark.parametrize("key", ["host", "port", "user", "password", "dbname"])
    def test_missing_required_key_fatal(self, key):
        config = {k: v for k, v in VALID.items() if k != key}
        with pytest.raises(ConfigurationError, match=key):
            validate_config(config)

    def test_invalid_default_replication_method(self):
        with pytest.raises(ConfigurationError, match="default_replication_method"):
            validate_config({**VALID, "default_replication_method": "MAGIC"})

    def test_use_secondary_requires_host_and_port(self):
        with pytest.raises(ConfigurationError, match="secondary_host, secondary_port"):
            validate_config({**VALID, "use_secondary": True})

    def test_use_secondary_with_replica_settings(self):
        validate_config({
            **VALID,
            "use_secondary": True,
            "secondary_host": "r",
            "secondary_port": 5433,
        })


class TestFlags:
    def test_legacy_string_true(self):
        assert cfg.use_ssl({"ssl": "true"}) is True
        assert cfg.debug_lsn({"debug_lsn": "true"}) is True

    def test_other_strings_false(self):
        assert cfg.use_ssl({"ssl": "yes"}) is False

    def test_real_booleans_accepted(self):
        assert cfg.use_ssl({"ssl": True}) is True
        assert cfg.debug_lsn({"debug_lsn": False}) is False

    def test_break_at_end_lsn_defaults_true(self):
        assert cfg.break_at_end_lsn({}) is True
        assert cfg.break_at_end_lsn({"break_at_end_lsn": False}) is False

    def test_break_at_end_lsn_accepts_strings(self):
        assert cfg.break_at_end_lsn({"break_at_end_lsn": "false"}) is False
        assert cfg.break_at_end_lsn({"break_at_end_lsn": "true"}) is True

    def test_numeric_defaults(self):
        assert cfg.itersize({}) == 20_000
        assert cfg.max_run_seconds({}) == 43_200
        # A configured 0 falls back to the default (SPEC §2.2).
        assert cfg.logical_poll_total_seconds({"logical_poll_total_seconds": 0}) == 10_800

    def test_filter_schemas_parsing(self):
        assert cfg.filter_schemas({}) is None
        assert cfg.filter_schemas({"filter_schemas": "a, b ,c"}) == ["a", "b", "c"]


class TestCli:
    def _config_file(self, tmp_path, config=None):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(config or VALID))
        return str(path)

    def test_discover_dispatch(self, tmp_path, monkeypatch):
        calls = {}
        monkeypatch.setattr(cli.db, "open_connection", lambda config: FakeConn())
        monkeypatch.setattr(
            cli.discovery,
            "do_discovery",
            lambda conn, itersize, filter_schemas: calls.update(
                itersize=itersize, filter_schemas=filter_schemas
            ),
        )
        monkeypatch.setattr("sys.argv", ["tap-postgres", "-c", self._config_file(tmp_path), "-d"])
        cli.main()
        assert calls == {"itersize": 20_000, "filter_schemas": None}

    def test_sync_dispatch_passes_state_path(self, tmp_path, monkeypatch):
        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps({"streams": []}))
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps({"bookmarks": {}}))
        calls = {}
        monkeypatch.setattr(
            cli.sync,
            "do_sync",
            lambda config, catalog, state, state_file: calls.update(
                catalog=catalog, state=state, state_file=state_file
            ),
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "tap-postgres",
                "-c",
                self._config_file(tmp_path),
                "--catalog",
                str(catalog_path),
                "-s",
                str(state_path),
            ],
        )
        cli.main()
        assert calls["catalog"] == {"streams": []}
        assert calls["state"] == {"bookmarks": {}}
        # The state *path* is retained for flush control (SPEC §1.1, §6.3.7).
        assert calls["state_file"] == state_path

    def test_no_mode_exits_cleanly(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr("sys.argv", ["tap-postgres", "-c", self._config_file(tmp_path)])
        with caplog.at_level("INFO"):
            cli.main()
        assert "No properties were selected" in caplog.text

    def test_fatal_errors_logged_critical_and_reraised(self, tmp_path, monkeypatch, caplog):
        config = {k: v for k, v in VALID.items() if k != "password"}
        monkeypatch.setattr(
            "sys.argv", ["tap-postgres", "-c", self._config_file(tmp_path, config), "-d"]
        )
        with caplog.at_level("CRITICAL"), pytest.raises(ConfigurationError):
            cli.main()
        assert "password" in caplog.text


class FakeConn:
    def close(self):
        pass
