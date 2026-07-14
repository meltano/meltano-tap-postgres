"""SQL generation unit coverage (SPEC §8.2)."""

import pytest

from tap_postgres.db import fully_qualified_table_name, quote_ident
from tap_postgres.sqlgen import (
    UnsafeDatatypeError,
    full_table_sql,
    incremental_sql,
    select_expression,
    validate_datatype,
)
from tests.unit.conftest import make_stream

CLAMPED = (
    "CASE WHEN \"{col}\" < '0001-01-01 00:00:00.000' "
    "OR \"{col}\" > '9999-12-31 23:59:59.999' "
    "THEN '9999-12-31 23:59:59.999' "
    'ELSE "{col}" END AS "{col}"'
)


class TestIdentifierQuoting:
    def test_plain(self):
        assert quote_ident("simple") == '"simple"'

    def test_embedded_quotes_doubled(self):
        assert quote_ident('strange "Name" col') == '"strange ""Name"" col"'

    def test_fully_qualified_with_quotes_and_spaces(self):
        assert fully_qualified_table_name('my "schema"', "my table") == '"my ""schema"""."my table"'


class TestTimestampClamp:
    def test_wraps_timestamp_without_tz(self):
        assert select_expression("ts", "timestamp without time zone") == CLAMPED.format(col="ts")

    def test_wraps_timestamp_with_tz(self):
        assert select_expression("ts", "timestamp with time zone") == CLAMPED.format(col="ts")

    def test_does_not_wrap_timestamp_arrays(self):
        assert select_expression("ts", "timestamp with time zone[]") == '"ts"'

    def test_does_not_wrap_non_timestamp(self):
        assert select_expression("num", "numeric") == '"num"'

    def test_column_missing_from_metadata_passes_through(self):
        assert select_expression("mystery", None) == '"mystery"'


class TestFullTableSql:
    def _stream(self):
        return make_stream(
            properties={"id": {}, "ts": {}},
            column_metadata={
                "id": {"sql-datatype": "integer", "inclusion": "automatic"},
                "ts": {"sql-datatype": "timestamp with time zone", "inclusion": "available"},
            },
        )

    def test_table_orders_by_xmin_text(self):
        sql = full_table_sql(self._stream(), "public", ["id", "ts"], is_view=False, resuming=False)
        assert sql.startswith('SELECT xmin::text, "id", CASE WHEN "ts"')
        assert sql.endswith('FROM "public"."test_table" ORDER BY xmin::text ASC')

    def test_resume_restricts_by_xmin_age(self):
        sql = full_table_sql(self._stream(), "public", ["id"], is_view=False, resuming=True)
        assert "WHERE age(xmin::xid) <= age(%s::text::xid)" in sql

    def test_view_is_plain_unordered_select(self):
        sql = full_table_sql(self._stream(), "public", ["id"], is_view=True, resuming=False)
        assert sql == 'SELECT "id" FROM "public"."test_table"'


class TestIncrementalSql:
    def _stream(self):
        return make_stream(
            properties={"id": {}, "updated_at": {}},
            column_metadata={
                "id": {"sql-datatype": "integer", "inclusion": "automatic"},
                "updated_at": {
                    "sql-datatype": "timestamp with time zone",
                    "inclusion": "available",
                },
            },
        )

    def test_no_bookmark_no_where(self):
        sql = incremental_sql(
            self._stream(),
            "public",
            ["id", "updated_at"],
            "updated_at",
            "timestamp with time zone",
            has_bookmark=False,
            limit=None,
        )
        assert "WHERE" not in sql
        assert 'ORDER BY "updated_at" ASC' in sql
        assert sql.startswith("SELECT ")
        assert sql.endswith(") pg_speedup_trick")

    def test_bookmark_is_bound_parameter_with_cast(self):
        sql = incremental_sql(
            self._stream(),
            "public",
            ["id"],
            "updated_at",
            "timestamp with time zone",
            has_bookmark=True,
            limit=None,
        )
        assert 'WHERE "updated_at" >= %s::timestamp with time zone' in sql

    def test_limit_rendered(self):
        sql = incremental_sql(
            self._stream(),
            "public",
            ["id"],
            "updated_at",
            "timestamp with time zone",
            has_bookmark=False,
            limit=500,
        )
        assert sql.count("LIMIT 500") == 1


class TestDatatypeValidation:
    @pytest.mark.parametrize(
        "datatype",
        ["integer", "timestamp with time zone", "numeric(10,2)", "character varying", "citext"],
    )
    def test_accepts_real_datatypes(self, datatype):
        assert validate_datatype(datatype) == datatype

    @pytest.mark.parametrize("datatype", ["integer; DROP TABLE x", "int--", "ts')::text"])
    def test_rejects_injection_shapes(self, datatype):
        with pytest.raises(UnsafeDatatypeError):
            validate_datatype(datatype)
