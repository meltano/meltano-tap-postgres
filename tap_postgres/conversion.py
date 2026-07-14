"""Value conversion for FULL_TABLE / INCREMENTAL rows (SPEC §5)."""

import datetime
import json
import math
import sys
from decimal import Decimal
from typing import Any

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()


class UnsupportedValueError(Exception):
    """A value's Python class has no mapping to a Singer record value."""


def is_array_datatype(sql_datatype: str) -> bool:
    return sql_datatype.endswith("[]")


def array_element_datatype(sql_datatype: str) -> str:
    return sql_datatype[:-2]


def fix_hour_24(time_string: str) -> str:
    """Map PostgreSQL's legal 24:00:00 to 00:00:00 (SPEC §5, §10.2.4)."""
    if time_string.startswith("24"):
        return time_string.replace("24", "00", 1)
    return time_string


def convert_time(value: Any, with_timezone: bool) -> str:  # ruff:ignore[any-type]
    """Times become HH:MM:SS strings; tz-aware times convert to UTC first (SPEC §5)."""
    if isinstance(value, datetime.time):
        parsed = value
    else:
        parsed = datetime.time.fromisoformat(fix_hour_24(str(value)))
    if with_timezone and parsed.utcoffset() is not None:
        as_datetime = datetime.datetime.combine(datetime.date(2000, 1, 1), parsed)
        parsed = as_datetime.astimezone(datetime.timezone.utc).time()
    return parsed.strftime("%H:%M:%S")


def convert_timestamp(value: datetime.datetime) -> str:
    """Naive timestamps are assumed UTC (+00:00 appended); offsets are preserved."""
    if value.tzinfo is None:
        return value.isoformat() + "+00:00"
    return value.isoformat()


def convert_date(value: datetime.date) -> str:
    return value.isoformat() + "T00:00:00+00:00"


def selected_value_to_singer_value(value: Any, sql_datatype: str | None) -> Any:  # ruff:ignore[any-type]
    """Convert one column value from the driver's native typing (SPEC §5).

    Columns missing from the catalog metadata (``sql_datatype`` is None) fall
    through to the Python-class rules, mirroring how they pass through the
    extraction query unwrapped (SPEC §8.2).
    """
    if value is None:
        return None

    if sql_datatype and is_array_datatype(sql_datatype):
        if not isinstance(value, list):
            msg = f"Expected a list for array datatype {sql_datatype}, got {type(value).__name__}"
            raise UnsupportedValueError(msg)
        element_type = array_element_datatype(sql_datatype)
        return [
            # PostgreSQL does not enforce array dimensionality: nested lists
            # recurse as arrays, scalars use the element type's rule (SPEC §3.5, §5).
            selected_value_to_singer_value(
                element,
                sql_datatype if isinstance(element, list) else element_type,
            )
            for element in value
        ]

    if sql_datatype in ("json", "jsonb"):
        return json.loads(value) if isinstance(value, str) else value
    if sql_datatype == "bit":
        # bit(1) only; longer bit strings are unsupported and never synced.
        return value == "1" if isinstance(value, str) else bool(value)
    if sql_datatype == "time with time zone":
        return convert_time(value, with_timezone=True)
    if sql_datatype == "time without time zone":
        return convert_time(value, with_timezone=False)

    if isinstance(value, datetime.datetime):
        return convert_timestamp(value)
    if isinstance(value, datetime.date):
        return convert_date(value)
    if isinstance(value, datetime.timedelta):
        msg = "Cannot marshall a value of class datetime.timedelta"
        raise UnsupportedValueError(msg)
    if isinstance(value, Decimal):
        return None if value.is_nan() else value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, dict):
        # hstore arrives as a dict when the driver mapping is registered.
        return value

    msg = f"Cannot marshall a value of class {type(value).__name__}"
    raise UnsupportedValueError(msg)
