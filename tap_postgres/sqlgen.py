"""Extraction SQL generation (SPEC §5, §6.1, §6.2)."""

import re
from typing import Any

from tap_postgres.db import fully_qualified_table_name, quote_ident

TIMESTAMP_MIN_LITERAL = "0001-01-01 00:00:00.000"
TIMESTAMP_MAX_LITERAL = "9999-12-31 23:59:59.999"

_SCALAR_TIMESTAMP_TYPES = ("timestamp without time zone", "timestamp with time zone")

# sql-datatype values come from discovery metadata but appear verbatim in SQL
# casts, so constrain them to a safe identifier-ish alphabet.
_SAFE_DATATYPE = re.compile(r'^[A-Za-z_][A-Za-z0-9_ ."]*(\(\d+(,\s*\d+)?\))?(\[\])?$')


class UnsafeDatatypeError(Exception):
    """A sql-datatype string is not safe to interpolate into a cast."""


def validate_datatype(sql_datatype: str) -> str:
    if not _SAFE_DATATYPE.match(sql_datatype):
        msg = f"Refusing to interpolate sql-datatype {sql_datatype!r}"
        raise UnsafeDatatypeError(msg)
    return sql_datatype


def select_expression(column: str, sql_datatype: str | None) -> str:
    """Render one column for the select list.

    Scalar timestamp columns are clamped in the query itself: any value outside
    0001-01-01..9999-12-31 23:59:59.999 becomes the *maximum* sentinel — the
    below-range direction is deliberate (SPEC §5, §10.2.3). Timestamp arrays and
    columns missing from metadata pass through unwrapped.
    """
    quoted = quote_ident(column)
    if sql_datatype in _SCALAR_TIMESTAMP_TYPES:
        return (
            f"CASE "
            f"WHEN {quoted} < '{TIMESTAMP_MIN_LITERAL}' "
            f"OR {quoted} > '{TIMESTAMP_MAX_LITERAL}' "
            f"THEN '{TIMESTAMP_MAX_LITERAL}' "
            f"ELSE {quoted} END AS {quoted}"
        )
    return quoted


def select_list(stream: dict[str, Any], columns: list[str]) -> str:
    from tap_postgres import stream_utils

    return ", ".join(
        select_expression(column, stream_utils.sql_datatype_for_column(stream, column))
        for column in columns
    )


def full_table_sql(
    stream: dict[str, Any],
    schema_name: str,
    columns: list[str],
    *,
    is_view: bool,
    resuming: bool,
) -> str:
    """FULL_TABLE extraction query (SPEC §6.1).

    Tables are ordered by xmin rendered as text, with each row's xmin selected
    so it can be bookmarked; a resuming run restricts by xmin age. Views get a
    plain unordered SELECT. The xmin bookmark is bound as a query parameter.
    """
    table = fully_qualified_table_name(schema_name, stream["table_name"])
    select = select_list(stream, columns)
    if is_view:
        return f"SELECT {select} FROM {table}"
    where = "WHERE age(xmin::xid) <= age(%s::text::xid) " if resuming else ""
    return f"SELECT xmin::text, {select} FROM {table} {where}ORDER BY xmin::text ASC"


def incremental_sql(
    stream: dict[str, Any],
    schema_name: str,
    columns: list[str],
    replication_key: str,
    replication_key_datatype: str,
    *,
    has_bookmark: bool,
    limit: int | None,
) -> str:
    """INCREMENTAL extraction query (SPEC §6.2).

    The chosen columns are selected from a subquery that filters by
    `key >= <bookmark>` (inclusive — at-least-once delivery) and orders by the
    key ascending; the bookmark value is bound as a query parameter and cast to
    the key's SQL datatype.
    """
    table = fully_qualified_table_name(schema_name, stream["table_name"])
    select = select_list(stream, columns)
    key = quote_ident(replication_key)
    where = ""
    if has_bookmark:
        datatype = validate_datatype(replication_key_datatype)
        where = f"WHERE {key} >= %s::{datatype} "
    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    return (
        f"SELECT {select} FROM ("
        f"SELECT * FROM {table} {where}ORDER BY {key} ASC{limit_clause}"
        f") pg_speedup_trick"
    )
