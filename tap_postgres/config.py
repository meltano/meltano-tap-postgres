"""Configuration validation and defaults (SPEC §2)."""

from typing import Any

REQUIRED_KEYS = ("host", "port", "user", "password", "dbname")

REPLICATION_METHODS = ("LOG_BASED", "INCREMENTAL", "FULL_TABLE")

DEFAULT_ITERSIZE = 20_000
DEFAULT_MAX_RUN_SECONDS = 43_200
DEFAULT_LOGICAL_POLL_TOTAL_SECONDS = 10_800


class ConfigurationError(Exception):
    """Invalid or incomplete tap configuration."""


def validate_config(config: dict[str, Any]) -> None:
    """Check required keys and cross-field constraints; raise ConfigurationError."""
    missing = [key for key in REQUIRED_KEYS if config.get(key) is None]
    if missing:
        msg = f"Missing required configuration keys: {', '.join(missing)}"
        raise ConfigurationError(msg)

    method = config.get("default_replication_method")
    if method is not None and method not in REPLICATION_METHODS:
        msg = (
            f"Invalid default_replication_method {method!r}; "
            f"expected one of {', '.join(REPLICATION_METHODS)}"
        )
        raise ConfigurationError(msg)

    if use_secondary(config):
        missing_secondary = [
            key for key in ("secondary_host", "secondary_port") if not config.get(key)
        ]
        if missing_secondary:
            raise ConfigurationError(
                "use_secondary requires additional configuration keys: "
                + ", ".join(missing_secondary)
            )


def _flag(config: dict[str, Any], key: str) -> bool:
    """Read a boolean setting, accepting real booleans and the legacy string "true".

    The reference implementation only honoured the exact string "true" for some
    flags (SPEC §10.2.2); we accept both real JSON booleans and that string.
    """
    value = config.get(key, False)
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def use_ssl(config: dict[str, Any]) -> bool:
    return _flag(config, "ssl")


def debug_lsn(config: dict[str, Any]) -> bool:
    return _flag(config, "debug_lsn")


def use_secondary(config: dict[str, Any]) -> bool:
    return _flag(config, "use_secondary")


def break_at_end_lsn(config: dict[str, Any]) -> bool:
    value = config.get("break_at_end_lsn", True)
    return value.lower() == "true" if isinstance(value, str) else bool(value)


def itersize(config: dict[str, Any]) -> int:
    return int(config.get("itersize") or DEFAULT_ITERSIZE)


def max_run_seconds(config: dict[str, Any]) -> int:
    return int(config.get("max_run_seconds") or DEFAULT_MAX_RUN_SECONDS)


def logical_poll_total_seconds(config: dict[str, Any]) -> float:
    # A configured value of 0/absent falls back to the default (SPEC §2.2).
    return float(config.get("logical_poll_total_seconds") or DEFAULT_LOGICAL_POLL_TOTAL_SECONDS)


def filter_schemas(config: dict[str, Any]) -> list[str] | None:
    raw = config.get("filter_schemas")
    if not raw:
        return None
    return [schema.strip() for schema in raw.split(",") if schema.strip()]
