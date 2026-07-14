"""Helpers shared by the sync strategies: metadata access and message emission (SPEC §4.4)."""

import copy
from typing import Any

import singer
from singer import metadata as metadata_module

LOGGER = singer.get_logger()


class InvalidCatalogError(Exception):
    """The input catalog is missing fields the tap needs to sync a stream."""


def metadata_map(stream: dict[str, Any]) -> dict[Any, Any]:
    return metadata_module.to_map(stream.get("metadata", []))


def stream_metadata(stream: dict[str, Any]) -> dict[str, Any]:
    return metadata_map(stream).get((), {})


def write_stream_metadata(stream: dict[str, Any], key: str, value: Any) -> None:  # ruff:ignore[any-type]
    """Set one stream-level (empty breadcrumb) metadata entry in place."""
    for entry in stream.setdefault("metadata", []):
        if not entry.get("breadcrumb"):
            entry.setdefault("metadata", {})[key] = value
            return
    stream["metadata"].append({"breadcrumb": [], "metadata": {key: value}})


def schema_name(stream: dict[str, Any]) -> str:
    """The stream's `schema-name` metadata; validated at sync start (SPEC §4.4)."""
    name = stream_metadata(stream).get("schema-name")
    if not name:
        stream_id = stream.get("tap_stream_id", "<unknown>")
        msg = f"Stream {stream_id} has no schema-name metadata"
        raise InvalidCatalogError(msg)
    return name


def validate_and_normalize_stream(stream: dict[str, Any]) -> None:
    """Check a selected catalog entry and fill in derivable fields, in place.

    Consumers occasionally hand-write catalogs, so anything derivable is
    repaired (`table_name` from `stream`, `schema-name` from `tap_stream_id`);
    anything else missing is a descriptive fatal error (SPEC §9).
    """
    missing = [field for field in ("tap_stream_id", "stream") if not stream.get(field)]
    if missing:
        msg = f"Catalog stream entry is missing required fields: {', '.join(missing)}"
        raise InvalidCatalogError(msg)
    stream_id = stream["tap_stream_id"]

    if not stream.get("table_name"):
        stream["table_name"] = stream["stream"]

    json_schema = stream.get("schema")
    if not isinstance(json_schema, dict) or not isinstance(json_schema.get("properties"), dict):
        msg = f"Stream {stream_id} has no JSON schema with a properties map"
        raise InvalidCatalogError(msg)

    if not stream_metadata(stream).get("schema-name"):
        # tap_stream_id is `<schema_name>-<table_name>` (SPEC §3.2), so the
        # schema name is recoverable when the id follows that shape.
        suffix = f"-{stream['table_name']}"
        if stream_id.endswith(suffix) and len(stream_id) > len(suffix):
            derived = stream_id[: -len(suffix)]
            LOGGER.info(
                "Stream %s has no schema-name metadata; derived %r from tap_stream_id",
                stream_id,
                derived,
            )
            write_stream_metadata(stream, "schema-name", derived)
        else:
            msg = (
                f"Stream {stream_id} has no schema-name metadata and it cannot be "
                "derived from tap_stream_id; re-run discovery or add it to the catalog"
            )
            raise InvalidCatalogError(msg)


def is_view(stream: dict[str, Any]) -> bool:
    return bool(stream_metadata(stream).get("is-view"))


def dest_stream_name(stream: dict[str, Any]) -> str:
    """Destination stream name: `<schema-name>-<stream>` (SPEC §4.4)."""
    return f"{schema_name(stream)}-{stream['stream']}"


def key_properties(stream: dict[str, Any]) -> list[str]:
    """`table-key-properties`, or `view-key-properties` for views (SPEC §4.4)."""
    md = stream_metadata(stream)
    if md.get("is-view"):
        return md.get("view-key-properties") or []
    return md.get("table-key-properties") or []


def _truthy_metadata(value: Any, default: bool) -> bool:  # ruff:ignore[any-type]
    if value is None:
        return default
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def is_stream_selected(stream: dict[str, Any]) -> bool:
    return _truthy_metadata(stream_metadata(stream).get("selected"), default=False)


def selected_columns(stream: dict[str, Any]) -> list[str]:
    """Columns to sync, in alphabetical order (SPEC §4.1 step 5).

    `automatic` columns are always synced; `available` columns default to
    selected when no `selected` metadata is present; `unsupported` never sync.
    """
    md_map = metadata_map(stream)
    columns = []
    for column in stream["schema"].get("properties", {}):
        column_md = md_map.get(("properties", column), {})
        inclusion = column_md.get("inclusion")
        if inclusion == "automatic" or (
            inclusion == "available" and _truthy_metadata(column_md.get("selected"), default=True)
        ):
            columns.append(column)
    return sorted(columns)


def sql_datatype_for_column(stream: dict[str, Any], column: str) -> str | None:
    return metadata_map(stream).get(("properties", column), {}).get("sql-datatype")


def replication_key(stream: dict[str, Any]) -> str | None:
    return stream_metadata(stream).get("replication-key")


def write_schema_message(stream: dict[str, Any], bookmark_properties: list[str]) -> None:
    singer.write_message(
        singer.SchemaMessage(
            stream=dest_stream_name(stream),
            schema=stream["schema"],
            key_properties=key_properties(stream),
            bookmark_properties=bookmark_properties,
        )
    )


def write_state_message(state: dict[str, Any]) -> None:
    """STATE messages carry a deep copy of the complete state (SPEC §4.4)."""
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
