"""Discovery math: precision/scale capping (SPEC §8.2)."""

from decimal import Decimal

import pytest

from tap_postgres.discovery import (
    ColumnInfo,
    capped_precision_and_scale,
    column_schema,
    numeric_constraints,
)


class TestPrecisionScaleCapping:
    def test_declared_values_kept(self):
        assert capped_precision_and_scale(10, 2) == (10, 2)

    def test_unconstrained_defaults_to_100_38(self):
        assert capped_precision_and_scale(None, None) == (100, 38)

    def test_precision_capped_at_100_with_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert capped_precision_and_scale(1000, 2) == (100, 2)
        assert "Capping decimal precision" in caplog.text

    def test_scale_capped_at_38_with_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert capped_precision_and_scale(100, 50) == (100, 38)
        assert "Capping decimal scale" in caplog.text

    def test_zero_scale_is_preserved(self):
        assert capped_precision_and_scale(10, 0) == (10, 0)


class TestNumericConstraints:
    def test_constraints_are_exact_decimals(self):
        constraints = numeric_constraints(10, 2)
        assert constraints["exclusiveMinimum"] == Decimal("-1e8")
        assert constraints["exclusiveMaximum"] == Decimal("1e8")
        assert constraints["multipleOf"] == Decimal("0.01")

    def test_default_constraints(self):
        constraints = numeric_constraints(100, 38)
        assert constraints["multipleOf"] == Decimal(10) ** -38


def _column(**kwargs):
    defaults = {
        "column_name": "c",
        "data_type": "numeric",
        "is_array": False,
        "is_enum": False,
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "is_primary_key": False,
    }
    defaults.update(kwargs)
    return ColumnInfo(**defaults)


class TestNumericColumnSchemas:
    def test_numeric_array_definition_carries_precision(self):
        definitions = {}
        schema = column_schema(
            _column(is_array=True, numeric_precision=12, numeric_scale=3), definitions
        )
        assert schema["items"]["$ref"] == "#/definitions/sdc_recursive_decimal_12_3_array"
        definition = definitions["sdc_recursive_decimal_12_3_array"]
        assert definition["multipleOf"] == Decimal("0.001")
        assert definition["type"] == ["null", "number", "array"]

    def test_primary_key_numeric_not_nullable(self):
        schema = column_schema(_column(is_primary_key=True, numeric_precision=6), {})
        assert schema["type"] == ["number"]

    def test_bit_longer_than_one_is_unsupported(self):
        assert column_schema(_column(data_type="bit", character_maximum_length=5), {}) == {}

    def test_bit_one_is_boolean(self):
        schema = column_schema(_column(data_type="bit", character_maximum_length=1), {})
        assert schema["type"] == ["null", "boolean"]


class TestScalarTypeRouting:
    @pytest.mark.parametrize("data_type", ["real", "double precision"])
    def test_floats_are_numbers(self, data_type):
        assert column_schema(_column(data_type=data_type), {}) == {"type": ["null", "number"]}

    def test_boolean(self):
        assert column_schema(_column(data_type="boolean"), {}) == {"type": ["null", "boolean"]}


class TestArrayElementRouting:
    """Element-type routing to the recursive definitions (SPEC §3.5)."""

    @pytest.mark.parametrize(
        ("element_type", "definition"),
        [
            ("smallint", "sdc_recursive_integer_array"),
            ("real", "sdc_recursive_number_array"),
            ("double precision", "sdc_recursive_number_array"),
            ("boolean", "sdc_recursive_boolean_array"),
            ("bit", "sdc_recursive_boolean_array"),
            ("json", "sdc_recursive_object_array"),
            ("jsonb", "sdc_recursive_object_array"),
            ("hstore", "sdc_recursive_object_array"),
            ("date", "sdc_recursive_timestamp_array"),
            ("timestamp with time zone", "sdc_recursive_timestamp_array"),
            ("text", "sdc_recursive_string_array"),
            ("interval", "sdc_recursive_string_array"),  # unknown types become strings
        ],
    )
    def test_element_type_routing(self, element_type, definition):
        schema = column_schema(_column(data_type=element_type, is_array=True), {})
        assert schema == {
            "type": ["null", "array"],
            "items": {"$ref": f"#/definitions/{definition}"},
        }
