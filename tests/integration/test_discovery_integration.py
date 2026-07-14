"""Discovery against a real PostgreSQL server (SPEC §8.3)."""

from decimal import Decimal

import pytest

from tap_postgres import db, discovery

pytestmark = pytest.mark.integration


def discover(config, tables=None):
    connection = db.open_connection(config)
    try:
        return discovery.discover_streams(
            connection,
            itersize=1000,
            filter_schemas=[config["filter_schemas"]] if config.get("filter_schemas") else None,
            tables=tables,
        )
    finally:
        connection.close()


def stream_by_name(streams, name):
    return next(s for s in streams if s["stream"] == name)


def column_metadata(stream):
    return {
        tuple(entry["breadcrumb"])[1]: entry["metadata"]
        for entry in stream["metadata"]
        if entry["breadcrumb"]
    }


def stream_metadata(stream):
    return next(e["metadata"] for e in stream["metadata"] if not e["breadcrumb"])


class TestTypeFamilies:
    def test_canonical_all_types_table(self, superuser_connection, tap_config, test_schema):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TYPE "{test_schema}".mood AS ENUM ('sad', 'happy');
                CREATE TABLE "{test_schema}".all_types (
                    id           bigserial PRIMARY KEY,
                    small_int    smallint,
                    a_numeric    numeric(10, 2),
                    free_numeric numeric,
                    a_varchar    varchar(33),
                    bit_one      bit(1),
                    bit_five     bit(5),
                    a_ts         timestamp,
                    a_tstz       timestamptz,
                    a_time       time,
                    a_uuid       uuid,
                    some_money   money,
                    a_json       jsonb,
                    an_hstore    hstore,
                    a_mood       "{test_schema}".mood,
                    int_array    integer[],
                    num_array    numeric(12, 3)[],
                    mood_array   "{test_schema}".mood[],
                    a_bytea      bytea
                )
                """
            )
        [stream] = discover(tap_config)

        assert stream["tap_stream_id"] == f"{test_schema}-all_types"
        assert stream["stream"] == "all_types"
        assert stream["table_name"] == "all_types"

        smd = stream_metadata(stream)
        assert smd["table-key-properties"] == ["id"]
        assert smd["schema-name"] == test_schema
        assert smd["database-name"] == tap_config["dbname"]
        assert smd["is-view"] is False
        assert isinstance(smd["row-count"], int)

        props = stream["schema"]["properties"]
        cmd = column_metadata(stream)

        assert props["id"] == {
            "type": ["integer"],
            "minimum": -(2**63),
            "maximum": 2**63 - 1,
        }
        assert cmd["id"] == {
            "sql-datatype": "bigint",
            "inclusion": "automatic",
            "selected-by-default": True,
        }
        assert props["small_int"] == {
            "type": ["null", "integer"],
            "minimum": -32768,
            "maximum": 32767,
        }
        assert props["a_numeric"] == {
            "type": ["null", "number"],
            "exclusiveMinimum": Decimal("-1e8"),
            "exclusiveMaximum": Decimal("1e8"),
            "multipleOf": Decimal("0.01"),
        }
        assert props["free_numeric"]["multipleOf"] == Decimal(10) ** -38
        assert props["a_varchar"] == {"type": ["null", "string"], "maxLength": 33}
        assert props["bit_one"] == {"type": ["null", "boolean"]}
        assert cmd["bit_one"]["sql-datatype"] == "bit"
        assert props["bit_five"] == {}
        assert cmd["bit_five"] == {
            "sql-datatype": "bit(5)",
            "inclusion": "unsupported",
            "selected-by-default": False,
        }
        assert props["a_ts"] == {"type": ["null", "string"], "format": "date-time"}
        assert cmd["a_ts"]["sql-datatype"] == "timestamp without time zone"
        assert cmd["a_tstz"]["sql-datatype"] == "timestamp with time zone"
        assert props["a_time"] == {"type": ["null", "string"], "format": "time"}
        assert props["a_uuid"] == {"type": ["null", "string"]}
        assert props["some_money"] == {"type": ["null", "string"]}
        assert props["a_json"] == {"type": ["null", "object", "array"]}
        assert props["an_hstore"] == {"type": ["null", "object"], "properties": {}}
        assert props["a_mood"] == {"type": ["null", "string"]}
        assert props["int_array"] == {
            "type": ["null", "array"],
            "items": {"$ref": "#/definitions/sdc_recursive_integer_array"},
        }
        assert cmd["int_array"]["sql-datatype"] == "integer[]"
        assert props["num_array"]["items"]["$ref"] == (
            "#/definitions/sdc_recursive_decimal_12_3_array"
        )
        assert props["mood_array"]["items"]["$ref"] == "#/definitions/sdc_recursive_string_array"
        assert props["a_bytea"] == {}
        assert cmd["a_bytea"]["inclusion"] == "unsupported"

        definitions = stream["schema"]["definitions"]
        for name in (
            "sdc_recursive_integer_array",
            "sdc_recursive_number_array",
            "sdc_recursive_string_array",
            "sdc_recursive_boolean_array",
            "sdc_recursive_object_array",
            "sdc_recursive_timestamp_array",
        ):
            assert definitions[name]["items"]["$ref"] == f"#/definitions/{name}"
        assert definitions["sdc_recursive_decimal_12_3_array"]["multipleOf"] == Decimal("0.001")

    def test_exotic_identifiers(self, superuser_connection, tap_config, test_schema):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f'''
                CREATE TABLE "{test_schema}"."strange ""Name"" table" (
                    "id col" integer PRIMARY KEY,
                    "select" text,
                    "MixedCase" timestamp
                )
                '''
            )
        [stream] = discover(tap_config)
        assert stream["stream"] == 'strange "Name" table'
        assert stream["tap_stream_id"] == f'{test_schema}-strange "Name" table'
        assert set(stream["schema"]["properties"]) == {"id col", "select", "MixedCase"}
        assert stream_metadata(stream)["table-key-properties"] == ["id col"]

    def test_views_and_materialized_views(self, superuser_connection, tap_config, test_schema):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".base (id integer PRIMARY KEY, name text);
                CREATE VIEW "{test_schema}".a_view AS SELECT id, name FROM "{test_schema}".base;
                CREATE MATERIALIZED VIEW "{test_schema}".a_matview AS
                    SELECT id FROM "{test_schema}".base;
                """
            )
        streams = discover(tap_config)
        view = stream_by_name(streams, "a_view")
        matview = stream_by_name(streams, "a_matview")
        assert stream_metadata(view)["is-view"] is True
        assert stream_metadata(view)["table-key-properties"] == []
        assert stream_metadata(matview)["is-view"] is True

    def test_unsupported_primary_key_type(self, superuser_connection, tap_config, test_schema):
        with superuser_connection.cursor() as cur:
            cur.execute(f'CREATE TABLE "{test_schema}".bytea_pk (id bytea PRIMARY KEY, name text)')
        [stream] = discover(tap_config)
        cmd = column_metadata(stream)
        assert cmd["id"]["inclusion"] == "unsupported"
        assert cmd["id"]["selected-by-default"] is False
        assert stream["schema"]["properties"]["id"] == {}


class TestColumnPrivileges:
    def test_only_granted_columns_discovered(
        self, superuser_connection, tap_config, pg_stack, test_schema
    ):
        role = f"limited_{test_schema}"
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".secrets (
                    id integer PRIMARY KEY, visible text, hidden text
                );
                CREATE ROLE "{role}" WITH LOGIN PASSWORD 'limited';
                GRANT USAGE ON SCHEMA "{test_schema}" TO "{role}";
                GRANT SELECT (id, visible) ON "{test_schema}".secrets TO "{role}";
                """
            )
        try:
            limited_config = {**tap_config, "user": role, "password": "limited"}
            [stream] = discover(limited_config)
            assert set(stream["schema"]["properties"]) == {"id", "visible"}
        finally:
            with superuser_connection.cursor() as cur:
                cur.execute(f'DROP OWNED BY "{role}"; DROP ROLE "{role}"')


class TestSchemaRefresh:
    def test_refresh_updates_schema_and_preserves_selection(
        self, superuser_connection, tap_config, test_schema
    ):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{test_schema}".evolving (id integer PRIMARY KEY, old_col text)'
            )
        [stream] = discover(tap_config)
        for entry in stream["metadata"]:
            if not entry["breadcrumb"]:
                entry["metadata"]["selected"] = True
                entry["metadata"]["replication-method"] = "INCREMENTAL"
                entry["metadata"]["replication-key"] = "id"

        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                ALTER TABLE "{test_schema}".evolving ADD COLUMN new_col numeric(6,2);
                ALTER TABLE "{test_schema}".evolving DROP COLUMN old_col;
                """
            )
        discovery.refresh_streams_schema(tap_config, [stream])

        assert set(stream["schema"]["properties"]) == {"id", "new_col"}
        smd = stream_metadata(stream)
        assert smd["selected"] is True
        assert smd["replication-method"] == "INCREMENTAL"
        assert smd["replication-key"] == "id"
        assert column_metadata(stream)["new_col"]["sql-datatype"] == "numeric"

    def test_zero_tables_is_an_error(self, tap_config, test_schema):
        connection = db.open_connection(tap_config)
        try:
            with pytest.raises(discovery.DiscoveryError, match="No tables discovered"):
                discovery.do_discovery(connection, itersize=1000, filter_schemas=[test_schema])
        finally:
            connection.close()


class TestDomainResolution:
    def test_domain_resolves_to_base_type(self, superuser_connection, tap_config, test_schema):
        with superuser_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE DOMAIN "{test_schema}".positive_int AS integer CHECK (VALUE > 0);
                CREATE TABLE "{test_schema}".with_domain (
                    id integer PRIMARY KEY,
                    quantity "{test_schema}".positive_int
                );
                """
            )
        [stream] = discover(tap_config)
        assert stream["schema"]["properties"]["quantity"] == {
            "type": ["null", "integer"],
            "minimum": -(2**31),
            "maximum": 2**31 - 1,
        }
        assert column_metadata(stream)["quantity"]["sql-datatype"] == "integer"
