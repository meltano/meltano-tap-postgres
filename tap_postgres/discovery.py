"""Catalog discovery (SPEC §3)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import simplejson
import singer
from singer import metadata as metadata_module

from tap_postgres import config as cfg
from tap_postgres import db, stream_utils

LOGGER = singer.get_logger()

MAX_NUMERIC_PRECISION = 100
MAX_NUMERIC_SCALE = 38

INTEGER_TYPES = {"smallint", "integer", "bigint"}
FLOAT_TYPES = {"real", "double precision"}
JSON_TYPES = {"json", "jsonb"}
STRING_TYPES = {
    "character varying",
    "character",
    "text",
    "citext",
    "uuid",
    "money",
    "cidr",
    "inet",
    "macaddr",
}
TIMESTAMP_TYPES = {"date", "timestamp without time zone", "timestamp with time zone"}
TIME_TYPES = {"time without time zone", "time with time zone"}


class DiscoveryError(Exception):
    """Discovery could not produce a catalog."""


@dataclass
class ColumnInfo:
    column_name: str
    data_type: str  # base/element type name, domains resolved (e.g. "integer")
    is_array: bool
    is_enum: bool
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    is_primary_key: bool

    @property
    def sql_datatype(self) -> str:
        """The value published as `sql-datatype` metadata (SPEC §3.3)."""
        if self.is_array:
            return f"{self.data_type}[]"
        if (
            self.data_type == "bit"
            and self.character_maximum_length is not None
            and self.character_maximum_length > 1
        ):
            return f"bit({self.character_maximum_length})"
        return self.data_type


# One catalog row per visible column of every discoverable relation (SPEC §3.1).
COLUMN_SCAN_SQL = """
SELECT
  n.nspname                                                     AS schema_name,
  c.relname                                                     AS table_name,
  c.relkind                                                     AS relkind,
  c.reltuples::bigint                                           AS row_count,
  a.attname                                                     AS column_name,
  format_type(COALESCE(elem_base.oid, elem_t.oid, base_t.oid, t.oid), NULL) AS data_type,
  (elem_t.oid IS NOT NULL)                                      AS is_array,
  (COALESCE(elem_base.typtype, elem_t.typtype,
            base_t.typtype, t.typtype) = 'e')                   AS is_enum,
  information_schema._pg_char_max_length(
      COALESCE(elem_base.oid, elem_t.oid, base_t.oid, t.oid),
      CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END
  )                                                             AS character_maximum_length,
  information_schema._pg_numeric_precision(
      COALESCE(elem_base.oid, elem_t.oid, base_t.oid, t.oid),
      CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END
  )                                                             AS numeric_precision,
  information_schema._pg_numeric_scale(
      COALESCE(elem_base.oid, elem_t.oid, base_t.oid, t.oid),
      CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END
  )                                                             AS numeric_scale,
  (pk.indkey IS NOT NULL AND a.attnum = ANY (pk.indkey::int2[])) AS is_primary_key
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_type base_t
       ON t.typtype = 'd' AND base_t.oid = t.typbasetype
LEFT JOIN pg_catalog.pg_type elem_t
       ON COALESCE(base_t.typcategory, t.typcategory) = 'A'
      AND elem_t.oid = COALESCE(base_t.typelem, CASE WHEN t.typtype <> 'd' THEN t.typelem END)
LEFT JOIN pg_catalog.pg_type elem_base
       ON elem_t.typtype = 'd' AND elem_base.oid = elem_t.typbasetype
LEFT JOIN pg_catalog.pg_index pk
       ON pk.indrelid = c.oid AND pk.indisprimary
WHERE c.relkind IN ('r', 'v', 'm', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname !~ '^pg_toast'
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND has_column_privilege(c.oid, a.attnum, 'SELECT')
"""


def base_recursive_definitions() -> dict[str, Any]:
    """The self-referential array item schemas present in every stream (SPEC §3.5)."""
    definitions: dict[str, Any] = {}
    for name, json_type in (
        ("integer", "integer"),
        ("number", "number"),
        ("string", "string"),
        ("boolean", "boolean"),
        ("object", "object"),
    ):
        definition_name = f"sdc_recursive_{name}_array"
        definitions[definition_name] = {
            "type": ["null", json_type, "array"],
            "items": {"$ref": f"#/definitions/{definition_name}"},
        }
    definitions["sdc_recursive_timestamp_array"] = {
        "type": ["null", "string", "array"],
        "format": "date-time",
        "items": {"$ref": "#/definitions/sdc_recursive_timestamp_array"},
    }
    return definitions


def capped_precision_and_scale(
    numeric_precision: int | None, numeric_scale: int | None
) -> tuple[int, int]:
    """Default unconstrained numerics to 100/38 and clamp declared values (SPEC §3.4)."""
    precision = MAX_NUMERIC_PRECISION if numeric_precision is None else numeric_precision
    scale = MAX_NUMERIC_SCALE if numeric_scale is None else numeric_scale
    if precision > MAX_NUMERIC_PRECISION:
        LOGGER.warning(
            "Capping decimal precision %s to %s; values may be truncated",
            precision,
            MAX_NUMERIC_PRECISION,
        )
        precision = MAX_NUMERIC_PRECISION
    if scale > MAX_NUMERIC_SCALE:
        LOGGER.warning(
            "Capping decimal scale %s to %s; values may be truncated", scale, MAX_NUMERIC_SCALE
        )
        scale = MAX_NUMERIC_SCALE
    return precision, scale


def numeric_constraints(precision: int, scale: int) -> dict[str, Decimal]:
    return {
        "exclusiveMinimum": -(Decimal(10) ** (precision - scale)),
        "exclusiveMaximum": Decimal(10) ** (precision - scale),
        "multipleOf": Decimal(10) ** -scale,
    }


def _with_nullability(types: list[str], is_primary_key: bool) -> list[str]:
    """PK columns omit "null"; everything else is nullable, "null" first (SPEC §3.4)."""
    if is_primary_key:
        return types
    return ["null", *types]


def _scalar_column_schema(column: ColumnInfo) -> dict[str, Any]:
    data_type = column.data_type
    pk = column.is_primary_key

    if data_type in INTEGER_TYPES:
        bits = {"smallint": 16, "integer": 32, "bigint": 64}[data_type]
        return {
            "type": _with_nullability(["integer"], pk),
            "minimum": -(2 ** (bits - 1)),
            "maximum": 2 ** (bits - 1) - 1,
        }
    if data_type in ("numeric", "decimal"):
        precision, scale = capped_precision_and_scale(
            column.numeric_precision, column.numeric_scale
        )
        return {
            "type": _with_nullability(["number"], pk),
            **numeric_constraints(precision, scale),
        }
    if data_type in FLOAT_TYPES:
        return {"type": _with_nullability(["number"], pk)}
    if data_type == "boolean":
        return {"type": _with_nullability(["boolean"], pk)}
    if data_type == "bit":
        if column.character_maximum_length is not None and column.character_maximum_length > 1:
            return {}  # unsupported (SPEC §3.4)
        return {"type": _with_nullability(["boolean"], pk)}
    if data_type in ("character varying", "character"):
        schema: dict[str, Any] = {"type": _with_nullability(["string"], pk)}
        if column.character_maximum_length is not None:
            schema["maxLength"] = column.character_maximum_length
        return schema
    if data_type in STRING_TYPES or column.is_enum:
        return {"type": _with_nullability(["string"], pk)}
    if data_type in JSON_TYPES:
        return {"type": _with_nullability(["object", "array"], pk)}
    if data_type == "hstore":
        return {"type": _with_nullability(["object"], pk), "properties": {}}
    if data_type in TIMESTAMP_TYPES:
        return {"type": _with_nullability(["string"], pk), "format": "date-time"}
    if data_type in TIME_TYPES:
        return {"type": _with_nullability(["string"], pk), "format": "time"}
    return {}  # unmapped: unsupported


def _array_column_schema(column: ColumnInfo, definitions: dict[str, Any]) -> dict[str, Any]:
    """Route the element type to a recursive definition (SPEC §3.5)."""
    element_type = column.data_type
    if element_type in ("numeric", "decimal"):
        precision, scale = capped_precision_and_scale(
            column.numeric_precision, column.numeric_scale
        )
        definition_name = f"sdc_recursive_decimal_{precision}_{scale}_array"
        definitions[definition_name] = {
            "type": ["null", "number", "array"],
            **numeric_constraints(precision, scale),
            "items": {"$ref": f"#/definitions/{definition_name}"},
        }
    elif element_type in INTEGER_TYPES:
        definition_name = "sdc_recursive_integer_array"
    elif element_type in FLOAT_TYPES:
        definition_name = "sdc_recursive_number_array"
    elif element_type in ("boolean", "bit"):
        definition_name = "sdc_recursive_boolean_array"
    elif element_type in JSON_TYPES or element_type == "hstore":
        definition_name = "sdc_recursive_object_array"
    elif element_type in TIMESTAMP_TYPES:
        definition_name = "sdc_recursive_timestamp_array"
    else:
        definition_name = "sdc_recursive_string_array"

    return {
        "type": _with_nullability(["array"], column.is_primary_key),
        "items": {"$ref": f"#/definitions/{definition_name}"},
    }


def column_schema(column: ColumnInfo, definitions: dict[str, Any]) -> dict[str, Any]:
    if column.is_array:
        return _array_column_schema(column, definitions)
    return _scalar_column_schema(column)


def build_stream_entry(
    schema_name: str,
    table_name: str,
    is_view: bool,
    row_count: int,
    database_name: str,
    columns: list[ColumnInfo],
) -> dict[str, Any]:
    definitions = base_recursive_definitions()
    properties: dict[str, Any] = {}
    mdata: dict[Any, Any] = {}

    primary_keys: list[str] = []
    for column in columns:
        schema = column_schema(column, definitions)
        properties[column.column_name] = schema
        unsupported = schema == {}
        if column.is_primary_key:
            primary_keys.append(column.column_name)
        if unsupported:
            inclusion = "unsupported"
        elif column.is_primary_key:
            inclusion = "automatic"
        else:
            inclusion = "available"
        mdata["properties", column.column_name] = {
            "sql-datatype": column.sql_datatype,
            "inclusion": inclusion,
            "selected-by-default": not unsupported,
        }

    mdata[()] = {
        "table-key-properties": [] if is_view else primary_keys,
        "schema-name": schema_name,
        "database-name": database_name,
        "row-count": row_count,
        "is-view": is_view,
    }

    return {
        "table_name": table_name,
        "stream": table_name,
        "tap_stream_id": f"{schema_name}-{table_name}",
        "schema": {
            "type": "object",
            "properties": properties,
            "definitions": definitions,
        },
        "metadata": metadata_module.to_list(mdata),
    }


def discover_streams(
    connection: db.ConnectionProtocol,
    *,
    itersize: int,
    filter_schemas: list[str] | None = None,
    tables: list[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Inspect the database and return catalog stream entries (SPEC §3).

    ``tables`` limits the scan to specific ``(schema_name, table_name)`` pairs,
    which is how sync refreshes the schemas of selected streams (SPEC §4.1).
    """
    database_name = db.fetch_scalar(connection, "SELECT current_database()")

    sql = COLUMN_SCAN_SQL
    params: list[Any] = []
    if filter_schemas:
        sql += "  AND n.nspname = ANY (%s)\n"
        params.append(filter_schemas)
    if tables:
        sql += "  AND (n.nspname, c.relname) IN %s\n"
        params.append(tuple(tables))
    sql += "ORDER BY n.nspname, c.relname, a.attname"

    streams: list[dict[str, Any]] = []
    current_key: tuple[str, str] | None = None
    current_columns: list[ColumnInfo] = []
    current_relkind = ""
    current_row_count = 0

    def flush() -> None:
        if current_key is None:
            return
        schema_name, table_name = current_key
        streams.append(
            build_stream_entry(
                schema_name=schema_name,
                table_name=table_name,
                is_view=current_relkind in ("v", "m"),
                row_count=current_row_count,
                database_name=database_name,
                columns=current_columns,
            )
        )

    cursor = db.named_cursor(connection, itersize)
    try:
        cursor.execute(sql, params or None)
        for (
            schema_name,
            table_name,
            relkind,
            row_count,
            column_name,
            data_type,
            is_array,
            is_enum,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_primary_key,
        ) in cursor:
            key = (schema_name, table_name)
            if key != current_key:
                flush()
                current_key = key
                current_columns = []
                current_relkind = relkind
                current_row_count = row_count
            current_columns.append(
                ColumnInfo(
                    column_name=column_name,
                    data_type=data_type,
                    is_array=is_array,
                    is_enum=is_enum,
                    character_maximum_length=character_maximum_length,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    is_primary_key=is_primary_key,
                )
            )
        flush()
    finally:
        cursor.close()

    return streams


def do_discovery(
    connection: db.ConnectionProtocol,
    *,
    itersize: int,
    filter_schemas: list[str] | None,
) -> None:
    """Discovery mode: write the catalog to stdout (SPEC §3.1)."""
    streams = discover_streams(connection, itersize=itersize, filter_schemas=filter_schemas)
    if not streams:
        msg = "No tables discovered. Check database permissions and the filter_schemas setting."
        raise DiscoveryError(msg)
    dump_catalog(streams)


# Metadata keys produced by discovery; anything else is consumer-added
# (selected, replication-method, replication-key, view-key-properties, ...)
# and must survive a schema refresh (SPEC §4.1 step 3).
STREAM_DISCOVERABLE_METADATA = {
    "table-key-properties",
    "schema-name",
    "database-name",
    "row-count",
    "is-view",
}
COLUMN_DISCOVERABLE_METADATA = {"sql-datatype", "inclusion", "selected-by-default"}


def merge_refreshed_metadata(
    old_metadata: list[dict[str, Any]], fresh_metadata: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Fresh discoverable metadata, overlaid with the old non-discoverable metadata."""
    old_map = metadata_module.to_map(old_metadata)
    new_map = metadata_module.to_map(fresh_metadata)
    for breadcrumb, entry in old_map.items():
        discoverable = (
            STREAM_DISCOVERABLE_METADATA if breadcrumb == () else COLUMN_DISCOVERABLE_METADATA
        )
        for key, value in entry.items():
            if key in discoverable:
                continue
            if breadcrumb == () or breadcrumb in new_map:
                new_map.setdefault(breadcrumb, {})[key] = value
    return metadata_module.to_list(new_map)


def refresh_streams_schema(
    config: dict[str, Any], streams: list[dict[str, Any]], dbname: str | None = None
) -> None:
    """Replace each stream's schema and discoverable metadata in place (SPEC §4.1 step 3).

    Guarantees the sync works against the live table structure rather than a
    stale catalog, while preserving selection/replication metadata.
    """
    tables = []
    for stream in streams:
        tables.append((stream_utils.schema_name(stream), stream["table_name"]))

    connection = db.open_connection(config, dbname=dbname)
    try:
        fresh_streams = discover_streams(connection, itersize=cfg.itersize(config), tables=tables)
    finally:
        connection.close()
    fresh_by_id = {fresh["tap_stream_id"]: fresh for fresh in fresh_streams}

    for stream in streams:
        fresh = fresh_by_id.get(stream["tap_stream_id"])
        if fresh is None:
            msg = f"Selected stream {stream['tap_stream_id']} no longer exists in the database"
            raise DiscoveryError(msg)
        stream["schema"] = fresh["schema"]
        stream["metadata"] = merge_refreshed_metadata(stream.get("metadata", []), fresh["metadata"])


def dump_catalog(streams: list[dict[str, Any]]) -> None:
    # simplejson keeps Decimal schema constraints (e.g. multipleOf) precise.
    simplejson.dump({"streams": streams}, sys.stdout, indent=2, use_decimal=True)
    sys.stdout.write("\n")
    sys.stdout.flush()
