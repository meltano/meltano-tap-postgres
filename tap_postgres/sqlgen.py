"""Extraction SQL generation (SPEC §5, §6.1, §6.2)."""

import re
from typing import Any

from tap_postgres.db import fully_qualified_table_name, quote_ident

TIMESTAMP_MIN_LITERAL = "0001-01-01 00:00:00.000"
TIMESTAMP_MAX_LITERAL = "9999-12-31 23:59:59.999"

_SCALAR_TIMESTAMP_TYPES = ("timestamp without time zone", "timestamp with time zone")
_DATE_TYPE = "date"

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
    0001-01-01..9999-12-31 23:59:59.999 becomes the *maximum* sentinel - the
    below-range direction is deliberate (SPEC §5, §10.2.3). Timestamp arrays and
    columns missing from metadata pass through unwrapped.

    ``date`` columns are cast to ``timestamp`` here too (unclamped - Postgres's
    `date` max year, 5874897, is far past `timestamp`'s, so an in-range-for-`date`
    value could in principle overflow this cast; not handled, see SPEC §5/§10.2.3
    for the equivalent timestamp clamp this doesn't yet have a `date` counterpart
    of). discovery.py declares `date` columns with `format: date-time` same as a
    real timestamp column (SPEC §5), and conversion.py's RECORD-mode conversion
    (`convert_date`) honors that by appending a midnight time component - but nothing
    upstream of the driver does the same for the ADBC BATCH path, which hands off
    the driver's own Arrow type unchanged. Without this cast, a `date` column's Arrow
    BATCH file keeps Postgres's native `date32` physical type, silently mismatching
    the declared JSON Schema format; some downstream consumers trust that promise
    literally (e.g. Snowflake refuses to cast a semi-structured `DATE` value straight
    to `TIMESTAMP_NTZ`, even though the same value round-trips fine as a `TIMESTAMP`).
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
    if sql_datatype == _DATE_TYPE:
        return f"{quoted}::timestamp AS {quoted}"
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
    placeholder: str = "%s",
) -> str:
    """FULL_TABLE extraction query (SPEC §6.1).

    Tables are ordered by xmin rendered as text, with each row's xmin selected
    so it can be bookmarked; a resuming run restricts by xmin age. Views get a
    plain unordered SELECT. The xmin bookmark is bound as a query parameter -
    ``placeholder`` selects its style (psycopg2's ``%s``, or ``$1`` for the
    server-side prepared statements of the ADBC BATCH path).
    """
    table = fully_qualified_table_name(schema_name, stream["table_name"])
    select = select_list(stream, columns)
    if is_view:
        return f"SELECT {select} FROM {table}"
    where = f"WHERE age(xmin::xid) <= age({placeholder}::text::xid) " if resuming else ""
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
    placeholder: str = "%s",
) -> str:
    """INCREMENTAL extraction query (SPEC §6.2).

    The chosen columns are selected from a subquery that filters by
    `key >= <bookmark>` (inclusive - at-least-once delivery) and orders by the
    key ascending; the bookmark value is bound as a query parameter and cast to
    the key's SQL datatype. ``placeholder`` selects the parameter style
    (psycopg2's ``%s``, or ``$1`` for the ADBC BATCH path).
    """
    table = fully_qualified_table_name(schema_name, stream["table_name"])
    select = select_list(stream, columns)
    key = quote_ident(replication_key)
    where = ""
    if has_bookmark:
        datatype = validate_datatype(replication_key_datatype)
        where = f"WHERE {key} >= {placeholder}::{datatype} "
    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    return (
        f"SELECT {select} FROM ("
        f"SELECT * FROM {table} {where}ORDER BY {key} ASC{limit_clause}"
        f") pg_speedup_trick"
    )
