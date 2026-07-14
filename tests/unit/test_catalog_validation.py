"""Input-catalog validation and normalization, plus the fetch_scalar wrapper."""

import pytest

from tap_postgres import db, stream_utils
from tap_postgres.stream_utils import InvalidCatalogError, validate_and_normalize_stream
from tests.unit.conftest import FakeConnection, make_stream


class TestFetchScalar:
    def test_returns_first_column(self):
        connection = FakeConnection(results={"SHOW server_encoding": [("UTF8",)]})
        assert db.fetch_scalar(connection, "SHOW server_encoding") == "UTF8"

    def test_no_rows_raises_descriptively(self):
        connection = FakeConnection(results={"SHOW something": []})
        with pytest.raises(db.EmptyResultError, match="SHOW something"):
            db.fetch_scalar(connection, "SHOW something")


class TestValidateAndNormalize:
    def test_valid_stream_untouched(self):
        stream = make_stream()
        before = dict(stream)
        validate_and_normalize_stream(stream)
        assert stream == before

    @pytest.mark.parametrize("field", ["tap_stream_id", "stream"])
    def test_missing_identity_fields_fatal(self, field):
        stream = make_stream()
        del stream[field]
        with pytest.raises(InvalidCatalogError, match=field):
            validate_and_normalize_stream(stream)

    def test_table_name_defaults_to_stream(self):
        stream = make_stream()
        del stream["table_name"]
        validate_and_normalize_stream(stream)
        assert stream["table_name"] == stream["stream"]

    def test_missing_schema_fatal(self):
        stream = make_stream()
        del stream["schema"]
        with pytest.raises(InvalidCatalogError, match="JSON schema"):
            validate_and_normalize_stream(stream)

    def _drop_schema_name(self, stream):
        for entry in stream["metadata"]:
            if not entry["breadcrumb"]:
                del entry["metadata"]["schema-name"]

    def test_schema_name_derived_from_tap_stream_id(self):
        stream = make_stream(schema_name="my-schema", table_name="my-table")
        self._drop_schema_name(stream)
        validate_and_normalize_stream(stream)
        assert stream_utils.schema_name(stream) == "my-schema"

    def test_schema_name_derived_with_no_metadata_at_all(self):
        stream = make_stream()
        stream["metadata"] = []
        validate_and_normalize_stream(stream)
        assert stream_utils.schema_name(stream) == "public"

    def test_underivable_schema_name_fatal(self):
        stream = make_stream()
        stream["tap_stream_id"] = "does-not-match"  # not <schema>-<table_name>
        self._drop_schema_name(stream)
        with pytest.raises(InvalidCatalogError, match="cannot be derived"):
            validate_and_normalize_stream(stream)

    def test_schema_name_accessor_raises_when_missing(self):
        stream = make_stream()
        self._drop_schema_name(stream)
        with pytest.raises(InvalidCatalogError, match="schema-name"):
            stream_utils.schema_name(stream)


class TestMetadataAccessors:
    def test_view_key_properties_used_for_views(self):
        stream = make_stream(stream_metadata={"is-view": True, "view-key-properties": ["order_id"]})
        assert stream_utils.key_properties(stream) == ["order_id"]

    def test_view_without_key_properties_defaults_empty(self):
        stream = make_stream(stream_metadata={"is-view": True})
        assert stream_utils.key_properties(stream) == []

    def test_string_selected_metadata(self):
        assert stream_utils.is_stream_selected(make_stream(stream_metadata={"selected": "true"}))
        assert not stream_utils.is_stream_selected(
            make_stream(stream_metadata={"selected": "false"})
        )
