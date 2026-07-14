"""Value conversion unit coverage (SPEC §8.2, traditional paths)."""

import datetime
import math
from decimal import Decimal

import pytest

from tap_postgres.conversion import UnsupportedValueError, selected_value_to_singer_value

convert = selected_value_to_singer_value


class TestJson:
    def test_null(self):
        assert convert(None, "json") is None
        assert convert(None, "jsonb") is None

    def test_empty_object(self):
        assert convert("{}", "json") == {}
        assert convert("{}", "jsonb") == {}

    def test_populated_object_round_trips(self):
        assert convert('{"a": [1, {"b": null}]}', "jsonb") == {"a": [1, {"b": None}]}

    def test_top_level_array(self):
        assert convert("[1, 2]", "json") == [1, 2]


class TestTimes:
    def test_time_without_tz(self):
        assert convert("13:14:15", "time without time zone") == "13:14:15"

    def test_time_without_tz_native(self):
        assert convert(datetime.time(13, 14, 15), "time without time zone") == "13:14:15"

    def test_hour_24_becomes_00(self):
        assert convert("24:00:00", "time without time zone") == "00:00:00"

    def test_hour_24_only_fixes_leading_24(self):
        # 23:24:00 contains "24" but does not start with it.
        assert convert("23:24:00", "time without time zone") == "23:24:00"

    def test_time_with_tz_converted_to_utc_and_dropped(self):
        assert convert("13:14:15+05", "time with time zone") == "08:14:15"

    def test_time_with_tz_hour_24(self):
        assert convert("24:00:00+00", "time with time zone") == "00:00:00"


class TestTimestamps:
    def test_naive_assumed_utc(self):
        value = datetime.datetime(2024, 3, 4, 5, 6, 7, 123456)
        assert convert(value, "timestamp without time zone") == "2024-03-04T05:06:07.123456+00:00"

    def test_aware_offset_preserved(self):
        tz = datetime.timezone(datetime.timedelta(hours=2))
        value = datetime.datetime(2024, 3, 4, 5, 6, 7, tzinfo=tz)
        assert convert(value, "timestamp with time zone") == "2024-03-04T05:06:07+02:00"

    def test_date(self):
        assert convert(datetime.date(2024, 3, 4), "date") == "2024-03-04T00:00:00+00:00"


class TestBitAndNumbers:
    def test_bit_one_true(self):
        assert convert("1", "bit") is True

    def test_bit_zero_false(self):
        assert convert("0", "bit") is False

    def test_nan_decimal_is_null(self):
        assert convert(Decimal("NaN"), "numeric") is None

    def test_decimal_full_precision(self):
        value = Decimal("1234567890.123456789012345678")
        assert convert(value, "numeric") == value

    def test_nan_float_is_null(self):
        assert convert(float("nan"), "double precision") is None

    def test_positive_infinity_is_null(self):
        assert convert(float("inf"), "double precision") is None

    def test_negative_infinity_is_null(self):
        assert convert(float("-inf"), "real") is None

    def test_plain_float(self):
        assert math.isclose(convert(1.5, "real"), 1.5)


class TestArrays:
    def test_elementwise_conversion(self):
        assert convert([Decimal("1.1"), None, Decimal("NaN")], "numeric[]") == [
            Decimal("1.1"),
            None,
            None,
        ]

    def test_nested_lists(self):
        value = [[datetime.date(2024, 1, 1)], [datetime.date(2024, 6, 1)]]
        assert convert(value, "date[]") == [
            ["2024-01-01T00:00:00+00:00"],
            ["2024-06-01T00:00:00+00:00"],
        ]

    def test_null_array(self):
        assert convert(None, "integer[]") is None

    def test_non_list_for_array_datatype_raises(self):
        with pytest.raises(UnsupportedValueError, match="Expected a list"):
            convert(5, "integer[]")


class TestPassthroughAndErrors:
    def test_scalars_pass_through(self):
        assert convert(42, "integer") == 42
        assert convert("x", "text") == "x"
        assert convert(True, "boolean") is True

    def test_column_missing_from_metadata_uses_class_rules(self):
        # sql_datatype is None when the catalog has no metadata for the column.
        assert convert(42, None) == 42
        assert convert(float("nan"), None) is None
        assert convert(datetime.date(2024, 1, 1), None) == "2024-01-01T00:00:00+00:00"

    def test_money_string_passes_through(self):
        assert convert("$1,001.00", "money") == "$1,001.00"

    def test_hstore_dict_passes_through(self):
        assert convert({"k": "v", "n": None}, "hstore") == {"k": "v", "n": None}

    def test_unmarshallable_class_raises(self):
        with pytest.raises(UnsupportedValueError, match="timedelta"):
            convert(datetime.timedelta(days=1), "interval")

    def test_unknown_class_raises_with_type_name(self):
        class Widget:
            pass

        with pytest.raises(UnsupportedValueError, match="Widget"):
            convert(Widget(), "widget")
