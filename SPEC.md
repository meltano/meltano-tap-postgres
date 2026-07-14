# tap-postgres — Functional Specification

A specification for a clean-room implementation of a [Singer](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md) tap that extracts data from PostgreSQL. It describes observable behavior only: configuration, catalog discovery, the three replication methods, state management, emitted messages, dependencies, and the test strategy. It intentionally does not reference any existing source code. Sections 1–9 describe the reference behavior as-is; §10 separates the behaviors that are contractual from those that are incidental and may be improved.

---

## 1. Overview

The tap connects to a single PostgreSQL database, and operates in one of two modes:

- **Discovery mode**: inspects the database and writes a Singer *catalog* (JSON) to stdout.
- **Sync mode**: given a catalog with selection/replication metadata (and optionally a state file), extracts data and writes Singer `SCHEMA`, `RECORD`, `STATE`, and `ACTIVATE_VERSION` messages to stdout.

Three replication methods are supported per stream: `FULL_TABLE`, `INCREMENTAL`, and `LOG_BASED` (logical replication via the `wal2json` output plugin).

### 1.1 Command-line interface

| Flag | Meaning |
|---|---|
| `-c, --config <file>` | Required. JSON configuration file. |
| `-s, --state <file>` | Optional. JSON state file with bookmarks from a previous run. The *path* itself must be retained, because during log-based sync the tap periodically re-reads this file (see §6.3.7). |
| `--catalog <file>` | Catalog file with stream selection metadata (triggers sync mode). |
| `-d, --discover` | Run discovery mode. |

The reference implementation also accepts `-p, --properties <file>` as a deprecated alias for `--catalog`; a clean-room implementation must **not** carry it forward.

If neither `--discover` nor a catalog file is given, the tap logs that nothing was selected and exits successfully. Fatal errors are logged at critical level and re-raised (non-zero exit).

---

## 2. Configuration

All settings live in the JSON config file. Required keys are validated at startup; a missing key is a fatal error.

### 2.1 Required settings

| Setting | Type | Description |
|---|---|---|
| `host` | string | PostgreSQL host. |
| `port` | integer | PostgreSQL port. |
| `user` | string | PostgreSQL user. |
| `password` | string | PostgreSQL password. |
| `dbname` | string | Database name to connect to (also used for discovery). |

### 2.2 Optional settings

| Setting | Type | Default | Description |
|---|---|---|---|
| `filter_schemas` | string | none | Comma-separated schema names. Discovery only inspects these schemas. |
| `ssl` | string | none | When the *string* `"true"`, connect with SSL mode `require`. Any other value means no SSL requirement. |
| `default_replication_method` | string | none | `LOG_BASED`, `INCREMENTAL`, or `FULL_TABLE`. Used for any selected stream that has no `replication-method` metadata of its own. |
| `tap_id` | string | none | Pipeline identifier; appended to the logical replication slot name (§6.3.2). |
| `itersize` | integer | 20000 | Server-side cursor fetch batch size for FULL_TABLE and INCREMENTAL queries. |
| `limit` | integer | none | Adds a row limit to INCREMENTAL extraction queries (per run). |
| `max_run_seconds` | integer | 43200 | Log-based sync: stop after this many seconds of total runtime. |
| `logical_poll_total_seconds` | number | 10800 | Log-based sync: stop after this many seconds without receiving any WAL data. (A configured value of `0`/absent falls back to 10800.) |
| `break_at_end_lsn` | boolean | true | Log-based sync: stop as soon as a received WAL message is beyond the server's current LSN captured at startup. |
| `debug_lsn` | string | none | When the *string* `"true"`, every log-based record gets an additional `_sdc_lsn` string property holding the WAL position that produced it. |
| `use_secondary` | boolean | false | When true, FULL_TABLE and INCREMENTAL reads go to a replica. Logical replication and all "must be primary" operations still use the primary. |
| `secondary_host` | string | — | Replica host. **Required** when `use_secondary` is true (startup error otherwise). |
| `secondary_port` | integer | — | Replica port. **Required** when `use_secondary` is true. |

### 2.3 Connection behavior

Every connection the tap opens must:

- set a fixed application name identifying the tap (an arbitrary, implementation-chosen constant — useful for spotting the tap's sessions in `pg_stat_activity`);
- use a connection timeout of 30 seconds;
- pass SSL mode `require` when configured (§2.2).

Connection routing:

- **Discovery, FULL_TABLE, INCREMENTAL reads**: the secondary (replica) when `use_secondary` is enabled, otherwise the primary.
- **Always the primary**, regardless of `use_secondary`: fetching the current WAL LSN, server version checks, locating the replication slot, the streaming replication connection itself, and the helper queries used during log-based value decoding (array and hstore reconstruction, §6.3.6).

Bulk reads (FULL_TABLE / INCREMENTAL / discovery's column scan) must use server-side (named) cursors that stream rows in batches of `itersize` rows, so arbitrarily large tables can be extracted with bounded memory.

---

## 3. Catalog discovery

### 3.1 Scope

Discovery inspects the configured database and produces one catalog entry (stream) per relation. Included relation kinds: ordinary tables, views, materialized views, and partitioned tables. Excluded:

- everything in the system schemas `pg_catalog`, `information_schema`, and TOAST internals;
- dropped columns;
- columns the connecting user lacks `SELECT` privilege on (the relation is still discovered with its remaining visible columns);
- schemas not listed in `filter_schemas` when that setting is present.

Discovery must resolve *domain types* to their base type, and detect for each column: data type name, primary-key membership, character maximum length, numeric precision and scale, whether it is an array, and whether it is an enum (including enum arrays).

If zero tables are discovered overall, discovery fails with an error.

The catalog is written to stdout as a JSON object: `{"streams": [...]}` (pretty-printed).

### 3.2 Stream entry shape

Each stream entry contains:

| Field | Value |
|---|---|
| `table_name` | The relation name. |
| `stream` | Same as the relation name. |
| `tap_stream_id` | `<schema_name>-<table_name>`. |
| `schema` | JSON Schema for the row (see §3.4). Uses `type: object`, a `properties` map, and a `definitions` map for array item schemas. |
| `metadata` | Singer metadata list (see §3.3). |

### 3.3 Metadata

Stream-level metadata (empty breadcrumb):

| Key | Value |
|---|---|
| `table-key-properties` | List of primary-key column names (empty for views). |
| `schema-name` | The schema the relation lives in. |
| `database-name` | The database discovered from (the current database of the connection). |
| `row-count` | Approximate row count from the server's relation statistics (not an exact count). |
| `is-view` | True for views and materialized views. |

Column-level metadata (breadcrumb `["properties", <column>]`):

| Key | Value |
|---|---|
| `sql-datatype` | The column's PostgreSQL type name (e.g. `character varying`, `timestamp with time zone`, `integer[]`). For fixed-length bit strings longer than one bit, the length is included, e.g. `bit(5)`. |
| `inclusion` | `automatic` for primary-key columns, `unsupported` for unmapped types (§3.4, last row), otherwise `available`. |
| `selected-by-default` | `false` for unsupported columns, `true` otherwise. |

Consumers add non-discoverable metadata to the catalog before sync: `selected` (stream and column level), `replication-method`, `replication-key`, and optionally `view-key-properties` for views.

### 3.4 Type mapping (scalar columns)

Primary-key columns omit `"null"` from their type list; all other columns are nullable (`"null"` first in the type array).

| PostgreSQL type | JSON Schema |
|---|---|
| `smallint`, `integer`, `bigint` | `integer`, with `minimum` = −2^(p−1) and `maximum` = 2^(p−1)−1 where p is the type's bit width (16/32/64). |
| `numeric`/`decimal` | `number` with `exclusiveMinimum` = −10^(precision−scale), `exclusiveMaximum` = 10^(precision−scale), `multipleOf` = 10^(−scale). Unconstrained numerics default to precision 100 / scale 38; declared values above those caps are clamped (with a warning) — this can truncate. |
| `real`, `double precision` | `number`. |
| `boolean` | `boolean`. |
| `bit(1)` | `boolean`. |
| `bit(n)`, n > 1 | **Unsupported** (no type; column marked unsupported). |
| `character varying`, `character` | `string`, plus `maxLength` when a length is declared. |
| `text`, `citext`, `uuid`, `money`, `cidr`, `inet`, `macaddr`, any enum | `string`. |
| `json`, `jsonb` | `["null", "object", "array"]` (non-null for PKs). |
| `hstore` | `object` with empty `properties`. |
| `date`, `timestamp with/without time zone` | `string` with `format: date-time`. |
| `time with/without time zone` | `string` with `format: time`. |
| Anything else (e.g. `bytea`, `interval`, geometric types) | Empty schema `{}`; column metadata marks it `unsupported` / not selected by default. |

### 3.5 Type mapping (array columns)

PostgreSQL does not enforce array dimensionality, so array columns are typed recursively: an array item may be a scalar or another array, to any depth. Array columns get schema `{"type": ["null", "array"], "items": {"$ref": "#/definitions/<name>"}}`, and the stream schema's `definitions` always includes this family of self-referential schemas:

- `sdc_recursive_integer_array` — `["null","integer","array"]`, items self-reference;
- `sdc_recursive_number_array`, `sdc_recursive_string_array`, `sdc_recursive_boolean_array`, `sdc_recursive_object_array` — same pattern with the respective scalar type;
- `sdc_recursive_timestamp_array` — string variant with `format: date-time`.

Element-type routing: integer-family arrays → integer definition; `real[]`/`double precision[]` → number; `boolean[]`/`bit[]` → boolean; `json[]`/`jsonb[]`/`hstore[]` → object; `date[]`/`timestamp...[]` → timestamp; everything else (text, varchar, citext, uuid, money, time, network types, enums, unknown) → string.

`numeric[]` is special: a per-column definition named `sdc_recursive_decimal_<precision>_<scale>_array` is added, carrying the same `multipleOf` / `exclusiveMinimum` / `exclusiveMaximum` constraints as scalar numerics plus the recursive-array shape.

---

## 4. Sync orchestration

### 4.1 Stream selection and preparation

1. Filter catalog streams to those whose stream-level metadata has `selected: true`. Sort by `tap_stream_id`.
2. If any selected stream resolves to `LOG_BASED`, capture the server's **current WAL LSN** once, up front (the "end LSN" for this run, §6.3.4). This also performs the server version validation (§6.3.1).
3. **Refresh schemas**: re-run discovery for exactly the selected tables and replace each stream's `schema` and discoverable metadata in memory, preserving non-discoverable metadata (`selected`, `replication-method`, `replication-key`, etc.). This guarantees the sync always works against the live table structure rather than a stale catalog.
4. For each stream, resolve its replication method: stream metadata `replication-method`, else `default_replication_method`. An unrecognized method is a fatal error. `LOG_BASED` on a view is a fatal error.
5. Determine selected columns per stream: a column is synced if its inclusion is `automatic`, or it is `available` **and** selected (columns default to selected when unspecified). `unsupported` columns are never synced. Column order in records is alphabetical. A stream with zero selected columns is skipped with a warning.

### 4.2 State reset on replication-method change

For every stream, before syncing, compare against the `last_replication_method` bookmark:

- If the method changed since the last run, wipe that stream's bookmarks entirely.
- If the method is `INCREMENTAL` and the replication key changed, wipe that stream's bookmarks.
- Then record the current method in the `last_replication_method` bookmark.

### 4.3 Scheduling

Streams are partitioned into "traditional" work (FULL_TABLE, INCREMENTAL, and the initial full-table phase of LOG_BASED) and "pure logical" work, based on method and existing bookmarks:

| Method | Bookmarks present | Assigned work |
|---|---|---|
| FULL_TABLE | — | full table sync |
| INCREMENTAL | — | incremental sync |
| LOG_BASED | neither `xmin` nor `lsn` | initial full-table phase (new stream) |
| LOG_BASED | both `xmin` and `lsn` | resume an interrupted initial full-table phase |
| LOG_BASED | `lsn` only | pure logical streaming |
| LOG_BASED | `xmin` only | fatal error (inconsistent state) |

Traditional streams run first, one at a time. If the incoming state has `currently_syncing`, that stream is moved to the front (if it is no longer selected, log a warning and proceed). Each traditional stream sets `currently_syncing` in state while it runs, clears it when done, and emits a `STATE` message afterwards.

After all traditional work, pure-logical streams are grouped by their `database-name` metadata and each group is streamed together in one replication session (§6.3). Before starting a group, any bookmark belonging to a de-selected LOG_BASED stream is dropped from state, so stale positions can't drag the slot's restart point backwards.

### 4.4 Emitted message conventions

- **Destination stream name** in `SCHEMA`/`RECORD`/`ACTIVATE_VERSION` messages is `<schema-name>-<stream>` (schema name from metadata).
- **`SCHEMA` messages** carry: the stream's JSON schema; `key_properties` from `table-key-properties` (or `view-key-properties` for views, defaulting to empty); and `bookmark_properties` — empty for FULL_TABLE, the replication key for INCREMENTAL, `lsn` for LOG_BASED. Schema serialization must be decimal-precision-safe (numeric constraints like `multipleOf: 1e-38` must not lose precision).
- **`RECORD` messages** carry `version` (the stream's table version) and `time_extracted`.
- **`STATE` messages** carry the complete state object (deep copy at time of emission).

### 4.5 State shape

```json
{
  "currently_syncing": "<tap_stream_id or null>",
  "bookmarks": {
    "<tap_stream_id>": {
      "last_replication_method": "...",
      "version": 1234567890123,
      "xmin": 123456,
      "lsn": 60071389168,
      "replication_key": "updated_at",
      "replication_key_value": "2024-01-01T00:00:00+00:00"
    }
  }
}
```

Only the keys relevant to a stream's method are present. `version` is a millisecond epoch timestamp chosen when a new table version starts. LSNs are stored as integers (§6.3.3). For INCREMENTAL streams, any bookmark key outside {`replication_key`, `replication_key_value`, `version`, `last_replication_method`} is a fatal error.

---

## 5. Value conversion (FULL_TABLE / INCREMENTAL)

Rows come from the client driver with native typing; conversions applied before writing records:

| Source | Record value |
|---|---|
| NULL | `null` |
| `money` | string, as formatted by the server (e.g. `$1,001.00`) |
| `json` / `jsonb` | parsed into a JSON object/array (driver-level JSON parsing must be disabled so the raw text can be parsed once, predictably) |
| timestamps (no tz) | ISO-8601 with `+00:00` appended |
| timestamps (with tz) | ISO-8601 with offset preserved |
| **timestamp range clamp** | in the extraction query itself, any timestamp column value outside `0001-01-01 00:00:00.000` – `9999-12-31 23:59:59.999` is replaced by `9999-12-31 23:59:59.999` (applies to scalar timestamp columns only, not arrays) |
| `date` | ISO date + `T00:00:00+00:00` |
| `time without time zone` | `HH:MM:SS` string; a leading hour of 24 is replaced with 00 |
| `time with time zone` | converted to UTC, timezone dropped, `HH:MM:SS` string; hour-24 handled the same way |
| `bit(1)` | boolean (`'1'` → true) |
| numeric/decimal | decimal value with full precision (JSON serialization must be decimal-aware, not float-rounding); `NaN` → `null` |
| float | value; `NaN`, `+Inf`, `-Inf` → `null` |
| `hstore` | JSON object (the driver's hstore mapping is enabled when the server has the extension available) |
| arrays | element-wise recursive conversion using the element type's rule |
| int/string/boolean | as-is |
| any other class | fatal error naming the type |

Identifier safety: schema, table, and column names are always double-quoted in generated SQL, with embedded double quotes doubled. Column names, schema names, and table names containing spaces, mixed case, quotes, etc. must round-trip correctly.

---

## 6. Replication methods

### 6.1 FULL_TABLE

Every run extracts the whole table, with resumability inside a single table copy.

**Table versioning / message order:**

1. Determine whether this is the stream's first-ever run (no `version` bookmark).
2. Pick the table version: a fresh millisecond timestamp, **unless** an `xmin` bookmark exists (meaning the previous copy was interrupted), in which case reuse the bookmarked version.
3. Write the `version` bookmark, emit `STATE`, and emit the `SCHEMA` message (orchestrator does this before the strategy runs).
4. If first run: emit `ACTIVATE_VERSION` immediately (lets the target start a clean slate).
5. Stream all rows as `RECORD`s.
6. Emit a final `ACTIVATE_VERSION` at the end of every run, signaling the target may discard rows from older versions.

**Extraction & resume:** rows are selected ordered by the table's transaction-id system column (`xmin`) rendered as text, and each row's `xmin` value is written to the stream's `xmin` bookmark as it goes (a `STATE` message every 1,000 rows). If a run starts with an `xmin` bookmark present, the query restricts to rows whose `xmin` age is less than or equal to the bookmarked transaction's age — i.e., resume from where the previous copy stopped (age comparison makes this robust against transaction-id wraparound). When the copy finishes, the `xmin` bookmark is cleared. Note: resume is best-effort; text-ordering of xmin and concurrent writes make it approximate, which is acceptable because a full-table version is only activated once complete.

**Views** are synced with a plain unordered `SELECT` of the selected columns — no xmin ordering, no resume bookmark; the version is always fresh. Key properties come from `view-key-properties` metadata.

Before extraction, the strategy logs the server and client encodings, registers hstore support when available, and applies the timestamp clamp (§5) to scalar timestamp columns.

### 6.2 INCREMENTAL

Extracts only rows whose replication-key value is **greater than or equal to** the bookmarked value. The inclusive comparison guarantees no gaps at the cost of re-emitting the last-seen row(s) each run.

**Requirements:** stream metadata must provide `replication-key`, an available (or automatic) selected column. The key column's SQL datatype is taken from metadata and used to cast the bookmark literal in the WHERE clause.

**Behavior:**

1. Reuse the `version` bookmark if present, else pick a fresh one (an interrupted incremental run keeps the same version — resumption relies on the key bookmark, not versioning).
2. Write `version` and `replication_key` bookmarks; emit `STATE`; emit `SCHEMA` (with the replication key as bookmark property); emit `ACTIVATE_VERSION` (every run — incremental never truncates the target because the version doesn't change across runs).
3. Query: select the chosen columns from a subquery that selects the whole row set filtered by `key >= '<bookmark>'::<key-datatype>` (omitted entirely when no bookmark exists), ordered by the key ascending, with `LIMIT <limit>` when the `limit` setting is present. (The subquery-with-inner-order shape is a deliberate query-planner optimization.)
4. For each row: emit the `RECORD`, then set the `replication_key_value` bookmark to that record's converted key value — but **never** bookmark a NULL key value. Emit `STATE` every 10,000 rows.

Rows with a NULL replication key are only ever captured while no bookmark exists (they are permanently invisible to bookmarked runs); a NULL never poisons the state.

### 6.3 LOG_BASED (logical replication with wal2json)

Change data capture from the write-ahead log. A stream's lifecycle: (a) one-off full-table snapshot, then (b) continuous consumption of inserts/updates/deletes from a logical replication slot.

#### 6.3.1 Server prerequisites (operator setup, documented — not performed by the tap)

- PostgreSQL 9.4 or newer, connecting to the **primary**.
- The tap refuses to run (fatal error) on versions affected by a known WAL bug — minimum minor versions: 9.4.21, 9.5.16, 9.6.12, 10.7, 11.2; anything below 9.4 is unsupported.
- `wal2json` output plugin ≥ 2.3 installed on the server (the tap uses its format version 2).
- Server configuration: `wal_level=logical`, and `max_replication_slots` / `max_wal_senders` sized appropriately (≥ 5 recommended).
- A logical replication slot created per database with the `wal2json` plugin: `SELECT pg_create_logical_replication_slot('<slot_name>', 'wal2json');`
- The connecting user needs replication privileges.

#### 6.3.2 Slot naming and lookup

Slot name pattern: `<prefix>_<dbname>_<tap_id>` (or `<prefix>_<dbname>` when no `tap_id` is configured), lowercased, with every character outside `[a-z0-9_]` replaced by `_`. The prefix is an arbitrary constant the implementation chooses; it must be fixed and documented, since operators create the slot manually with that exact name. At startup the tap looks for the name without the tap-id suffix first (backward compatibility with older deployments), then the tap-id-suffixed name; if neither exists it fails with a "replication slot not found" error.

#### 6.3.3 LSN arithmetic

LSNs are converted between PostgreSQL's `file/offset` hex notation (e.g. `16/B374D848`) and a single integer (`file × 2^32 + offset`). State stores the integer form; logs display the hex form. The current server LSN is read with the version-appropriate function (`pg_current_wal_lsn()` on 10+, `pg_current_xlog_location()` before).

#### 6.3.4 Initial full-table phase

For a LOG_BASED stream with no bookmarks:

1. Set the stream's `lsn` bookmark to the **end LSN captured at run start** — everything after this point will be replayed from the WAL, so the snapshot and the log stream dovetail without loss (overlap is resolved by target upserts).
2. Run a standard full-table sync (§6.1), including xmin-based resumability.
3. Clear the `xmin` bookmark when the copy completes.

If a run finds both `xmin` and `lsn` bookmarks, the snapshot was interrupted: resume the full-table copy (keeping the existing `lsn` bookmark). Once only `lsn` remains, the stream graduates to pure logical streaming.

#### 6.3.5 Streaming session

All pure-logical streams of one database are consumed in a single replication session:

- Start position: the **minimum** `lsn` bookmark across the participating streams.
- Each stream gets two automatic schema additions before its `SCHEMA` message: `_sdc_deleted_at` (nullable date-time), and `_sdc_lsn` (nullable string) when `debug_lsn` is on.
- On servers ≥ 12, set the session's `wal_sender_timeout` to 3 hours.
- Begin replication on the slot with wal2json options: format version 2, transactions excluded, timestamps included, type info excluded, actions limited to insert/update/delete, and a table allowlist (`add-tables`) of the participating `schema.table` pairs — with space, comma, quote, period and asterisk backslash-escaped per wal2json rules. A failure to start replication (e.g. slot in use) is fatal.
- Send periodic keepalive/status updates (10-second interval).

**Loop:** read messages; for each, process it (§6.3.6) and advance bookmarks. Exit when any of these hold:

- no data received for `logical_poll_total_seconds` (default 3 hours);
- total runtime exceeds `max_run_seconds`;
- `break_at_end_lsn` is on and a message's LSN is beyond the end LSN captured at startup (the message is *not* processed).

When idle, block on the socket for up to 1 second, then loop.

#### 6.3.6 Message processing

Each wal2json v2 message is a JSON payload with an `action` (`I`nsert / `U`pdate / `D`elete — anything else is a fatal "unsupported payload" error), `schema`, `table`, and either `columns` (I/U: name/type/value triples) or `identity` (D: replica-identity columns). Unparseable payloads are silently skipped; payloads for streams not in this session are skipped.

- **Schema drift**: for I/U, if the payload contains column names absent from the stream's schema, re-run discovery for that table, refresh the stream's schema/metadata in place, re-add the automatic properties, and emit a fresh `SCHEMA` message before the record.
- **Record assembly**: keep only selected columns. For I/U, `_sdc_deleted_at` is `null`; for D, the identity columns are used and `_sdc_deleted_at` is set to the extraction timestamp. With `debug_lsn`, `_sdc_lsn` carries the message's LSN as a string. The `RECORD` uses the version bookmarked for the stream (missing version = fatal error) and the session's extraction timestamp.
- **Value decoding** (wal2json delivers most values as text):
  - `json`/`jsonb`: parsed.
  - `timestamp without/with time zone`: parsed to ISO-8601 (naive values get `+00:00` appended). Values that are out of range, later than `9999-12-31 23:59:59.999` UTC, BC-era, or otherwise unparseable become the fallback `9999-12-31T23:59:59.999+00:00`.
  - `date`: ISO date + `T00:00:00+00:00`; years above 9999 become fallback `9999-12-31T00:00:00+00:00`; other parse failures are fatal.
  - `time with/without time zone`: as in §5 (hour-24 fix-up, UTC conversion, tz dropped).
  - `bit`: `'1'`/true → boolean.
  - `numeric` (including in arrays): decimal-precise value.
  - `money`: string as-is.
  - `hstore`: the text form is converted into a JSON object by asking the server to explode it into a key/value array (primary connection).
  - **Arrays**: wal2json emits the PostgreSQL array literal as a string; the tap reconstructs the array by having the server cast the literal to a suitable array type (element types with lossless casts keep their native type — integer, boolean, varchar, cidr, inet, macaddr, real, smallint, double precision; everything else casts to a text array), then converts elements recursively.
  - `null` stays `null`; ints/floats/strings pass through; unknown value classes are fatal errors.
- **Bookmark**: after emitting the record, set that stream's `lsn` bookmark to the message's LSN.

#### 6.3.7 Flush control (never lose data the target hasn't stored)

The WAL position confirmed ("flushed") back to the server must only advance past data the *downstream target* has durably committed. The mechanism:

- The tap treats the state file given via `--state` as the source of truth for what has been committed downstream (the orchestrating process is expected to update it with the target's emitted state).
- On the first message received, flush the minimum of (the committed LSN from the state file at startup, the first message's LSN).
- Every 10 seconds, re-read the state file; if the committed minimum LSN across the session's streams advanced (and is behind the message currently being processed), send feedback flushing to it. Read failures are ignored silently.
- Because multiple wal2json messages can share one LSN (chunked output), a message's LSN is only considered "fully processed" once a message with a *higher* LSN arrives.
- Every 10,000 fully-processed LSN advances, write all session streams' `lsn` bookmarks to the last fully-processed LSN and emit `STATE`.
- On session end (any exit reason), set every session stream's `lsn` bookmark to the last fully-processed LSN (never regressing below the committed LSN) and emit a final `STATE`.

### 6.4 Driver type registrations

Before traditional syncs, the tap must configure driver-level decoding so that: `citext[]`, `bit[]`, `uuid[]`, `money[]`, and enum-array values arrive as string arrays; and `json`/`jsonb` arrive as raw strings (parsed by the tap itself, §5). This requires querying the server's type catalog for the relevant array-type OIDs (including one per enum type) at runtime.

---

## 7. Dependencies

### 7.1 Language, packaging, and tooling requirements

A clean-room implementation must:

- **Support Python 3.10 and newer** (the reference implementation was pinned to 3.7–3.10; do not carry that ceiling forward).
- **Use the latest released version of every direct dependency** at the time of implementation, pinned in the package metadata to the major.minor release with a floating patch level (e.g. `==2.5.*`), so bugfix releases are picked up automatically but minor/major upgrades are deliberate. In particular, prefer the current PostgreSQL driver generation (e.g. psycopg 3.x) over the reference's psycopg2, provided it satisfies the capabilities in §7.2.
- **Package with PEP 621 metadata in `pyproject.toml`** (project name, version, dependencies, and the `tap-postgres` console entry point declared under `[project]`), with **uv** as the package/environment manager (`uv.lock` committed for reproducible dev/CI environments, `uv build`/`uv publish` for releases).
- Use current-generation dev tooling in the same spirit: e.g. `ruff` for linting/formatting and `pytest` with coverage, run via `uv run`.

### 7.2 Runtime capabilities

Runtime requirements, described by capability (reference choices shown for orientation only — see §7.1 for version policy):

| Capability | Reference choice | Notes |
|---|---|---|
| Singer toolkit: message types (`SCHEMA`/`RECORD`/`STATE`/`ACTIVATE_VERSION`), catalog & metadata helpers, bookmark helpers, metrics counters, RFC-3339 utilities, logging | a singer-python v1.x-compatible library | Message serialization **must preserve decimal precision** (no float round-tripping of numeric values or schema constraints). |
| PostgreSQL client | psycopg2 (2.9.x, binary) | Must support: server-side named cursors with configurable fetch size, the streaming logical-replication protocol (start replication on a slot, read messages, send standby feedback), custom type/OID registration, and hstore mapping. |
| RFC-3339 validation | strict-rfc3339 | |
| Timezone/date parsing | pytz + dateutil (transitive) | Lenient timestamp parsing with control over BC-era/unknown-timezone edge cases. |

License of the reference implementation: AGPL v3 (a clean-room implementation may choose its own).

External *server-side* dependency for LOG_BASED only: the `wal2json` plugin ≥ 2.3 (§6.3.1).

---

## 8. Testing

### 8.1 Test infrastructure

Integration tests need a real PostgreSQL primary (version 12 in the reference setup) built with the wal2json plugin, configured with `wal_level=logical` and generous `max_wal_senders`, plus a streaming-replication read replica for `use_secondary` coverage. The reference setup provisions both via Docker Compose and passes connection details through environment variables (`TAP_POSTGRES_HOST`, `TAP_POSTGRES_PORT`, `TAP_POSTGRES_USER`, `TAP_POSTGRES_PASSWORD`, `TAP_POSTGRES_SECONDARY_HOST`, `TAP_POSTGRES_SECONDARY_PORT`, plus a superuser password for fixture setup). Test fixtures create/drop tables and logical replication slots per test class.

Unit tests run without any database: connections and replication cursors are mocked/faked (e.g. a fake replication cursor that replays a scripted sequence of wal2json payloads).

Coverage gates from the reference project: ≥ 58% from unit tests alone, ≥ 63% from integration tests alone, ≥ 85% combined. Lint the codebase as part of CI.

### 8.2 Unit test coverage (what to verify)

**Value conversion (traditional):**
- json/jsonb: null → null, `{}` → `{}`, populated objects round-trip;
- times with/without timezone including the hour-24 edge case; timestamps with/without tz to ISO-8601;
- bit → boolean; NaN/±Inf floats and NaN decimals → null;
- array values converted element-wise (nested lists);
- error raised on unmarshallable classes.

**SQL generation:**
- identifier quoting: doubled quotes, fully-qualified names with embedded quotes/spaces;
- the timestamp clamp wraps scalar timestamp columns (both tz and non-tz) but not timestamp arrays or non-timestamp columns, and columns missing from metadata pass through unwrapped;
- schema/database filter clauses render correct IN lists.

**Discovery math:** precision/scale capping at 100/38 with warnings.

**State transitions:** for each replication method — same method persists bookmarks; switching methods (all six directions) wipes the stream's bookmarks; INCREMENTAL key change wipes bookmarks, including mid-interruption; `last_replication_method` is recorded.

**FULL_TABLE / INCREMENTAL strategies (mocked DB):** message sequences (state → schema → activate-version → records → state cadence), version selection/reuse, hstore registration only when available, bookmark cadence at exact multiples of the update period, max-replication-key lookup.

**Logical replication unit coverage:**
- LSN ↔ integer conversion both ways, including null and boundary values;
- slot name generation: lowercasing, invalid-character replacement, tap-id suffixing; slot lookup preferring the un-suffixed name, then the suffixed name, then error;
- wal2json table-list building with special-character escaping;
- version-gate errors for each buggy minor-version range, and correct LSN function per version;
- automatic property injection with `debug_lsn` on and off;
- message consumption: non-JSON payloads keep state unchanged; payloads for unselected streams are ignored; unsupported actions raise; new columns in payloads trigger a schema refresh and new `SCHEMA` message; insert/update vs delete record assembly (including `_sdc_deleted_at` semantics); missing version or missing column datatype raises;
- value decoding: full matrix of timestamp/date/time in-range, out-of-range, BC-era, min/max, string vs native inputs, and the fallbacks; hstore and array reconstruction (with the server round-trip mocked); numerics to decimals; unknown types raising;
- session loop: stops on idle-poll timeout, stops on max runtime, propagates replication-start failures and read errors; final state/bookmark write on exit.

### 8.3 Integration test coverage (real database)

- **Discovery per type family**: create tables covering strings (with quoted/exotic identifiers), integers, numerics (as PKs, with/without declared precision), dates/times, floats, bools and bits, json/jsonb, uuid, hstore, enums, money, arrays (many element types), array-like quirks, and a canonical "all-types" table; assert the exact stream entry produced — schema, metadata (`table-key-properties`, `schema-name`, `database-name`, `row-count`, `is-view`, per-column `sql-datatype`/`inclusion`/`selected-by-default`), `tap_stream_id`, and definitions.
- **Column privileges**: a user granted `SELECT` on a subset of columns only discovers those columns.
- **Unsupported PK types**: tables whose PKs are unmapped types are discovered with those columns marked unsupported.
- **Schema refresh**: after altering a table, the refresh step updates stream schema/metadata while preserving selection and replication metadata.
- **End-to-end logical replication**: create a table and a replication slot, run the initial snapshot, apply inserts/updates/deletes, run the streaming phase, and assert the full emitted message sequence (schemas, records including `_sdc_deleted_at` for deletes, state contents and LSN monotonicity), including the `use_secondary` split (snapshot reads from replica, WAL from primary).

### 8.4 Suggested additional acceptance tests for a new implementation

- Interrupted FULL_TABLE resume: kill mid-copy, restart, assert no version change and that the copy completes from the xmin watermark.
- INCREMENTAL: inclusive lower bound (last row re-emitted), NULL replication-key rows never bookmarked, `limit` honored, bookmark equals the last emitted key value.
- LOG_BASED flush safety: verify the slot's `confirmed_flush_lsn` never advances beyond the committed state file's LSN.
- Multi-database catalogs: logical streams grouped per database, connection database switched per group.
- A target-agnostic golden-output test: fixed fixture data in, byte-comparable (modulo timestamps/versions) Singer message stream out.

---

## 9. Non-functional requirements

- **Memory**: extraction must stream; no full-table materialization client-side (server-side cursors for snapshots, message-at-a-time for WAL).
- **Fidelity**: numeric values must never pass through binary floats; timestamps outside the representable range must degrade to the documented sentinel values rather than crash.
- **Idempotence/at-least-once**: the design guarantees at-least-once delivery (inclusive incremental bounds, snapshot/WAL overlap); targets are expected to dedupe by primary key and version.
- **Observability**: log the resolved sync method per stream, generated SQL for snapshots, WAL start/end positions, flush positions, and row-count metrics per stream.
- **Failure posture**: configuration errors, inconsistent state, unsupported replication situations (views + LOG_BASED, unknown methods, missing slots, buggy server versions) fail fast with descriptive errors; transient decode issues on non-selected data are ignored.

---

## 10. Known deviations

Everything above documents the reference implementation faithfully — including behaviors that are accidents of implementation rather than design. This section separates the two so a clean-room implementation knows what it must preserve and where it is free (or encouraged) to do better.

### 10.1 Contractual — preserve exactly

These are load-bearing for downstream targets and orchestrators:

- The state shape (§4.5), bookmark key names, and the state-reset rules on method/key change (§4.2).
- Destination stream naming (`<schema-name>-<stream>`), `tap_stream_id` format, and metadata key names (§3.2–§3.3).
- ACTIVATE_VERSION semantics per method (§6.1–§6.2) and the snapshot→WAL handoff via end-LSN (§6.3.4).
- The at-least-once posture: inclusive `>=` incremental bounds, snapshot/WAL overlap, never bookmarking a NULL replication key.
- Sentinel fallbacks for out-of-range temporal values (`9999-12-31T23:59:59.999+00:00` / `9999-12-31T00:00:00+00:00`) and NaN/±Inf → `null` (chosen deliberately for consistency with what wal2json can represent).
- `_sdc_deleted_at` on log-based streams, `_sdc_lsn` under `debug_lsn`.
- The flush-control invariant (§6.3.7): the slot's confirmed position must never pass data the target hasn't committed. The *mechanism* (re-reading the state file) is negotiable; the invariant is not.
- Slot name shape `<prefix>_<dbname>[_<tap_id>]` with the sanitization rules — operators create slots by hand, so whatever the implementation picks must be fixed and documented.
- The console entry point name `tap-postgres` and the CLI flags in §1.1 (minus the deprecated `--properties`, which is dropped deliberately) — orchestrators invoke the tap by these.

### 10.2 Incidental — replicate or fix, but decide consciously

Behaviors a clean-room implementation may improve. Each entry: reference behavior → recommendation.

1. **SQL built by string interpolation.** The incremental WHERE clause embeds the bookmark value as an unescaped string literal; `filter_schemas` and the table allowlist build `IN (...)` lists by concatenation; slot lookup interpolates the slot name. A bookmark value containing a single quote breaks the query (identifiers, by contrast, are quoted correctly). → Use bound parameters / proper literal escaping throughout.
2. **String-typed booleans in config.** `ssl` and `debug_lsn` only activate on the exact string `"true"`; a JSON `true` is silently ignored. `break_at_end_lsn` meanwhile accepts any truthy value. → Accept real booleans (and the legacy strings, if migration compatibility matters); validate types at startup.
3. **Timestamp clamp direction.** The extraction-query clamp (§5) replaces values *below* `0001-01-01` with the **maximum** sentinel `9999-12-31 23:59:59.999`, not a minimum one. Logical replication does the same via its fallback constant. → Preserving this asymmetry keeps snapshot and CDC output consistent; fixing it to a low sentinel is defensible but must be done in both paths at once.
4. **Hour-24 time fix-up.** Times beginning with `24` (PostgreSQL allows `24:00:00`) are rewritten by replacing the first occurrence of the substring `24` with `00`, mapping end-of-day to start-of-day. Safe only because of the `startswith` guard. → Handle `24:00` explicitly; keep the semantic mapping (24:00 → 00:00) since targets can't represent hour 24.
5. **xmin-based full-table resume.** Ordering by `xmin::text` (lexicographic, not numeric) with `age()`-based restriction is approximate: concurrent writes, freezes, and text ordering can re-emit or skip rows mid-copy. Tolerable only because a version is activated solely after a complete pass. → A PK-keyset-paginated resume is strictly better where a PK exists; keep the "activate only when complete" rule regardless. Do not treat xmin bookmark values as portable across implementations.
6. **Silently dropped WAL payloads.** Messages whose payload fails JSON parsing are skipped without logging, yet their LSN still advances the bookmark — undetectable data loss if wal2json ever emits something unexpected. → Log loudly (or fail) on unparseable payloads; skipping silently should not be replicated.
7. **Per-value server round trips in CDC decoding.** Every array value is reconstructed by sending the literal back to the server inside a fixed dollar-quote tag (a value containing that tag breaks the query), and every hstore value costs another round trip; both open a fresh connection per value. → Parse array/hstore literals client-side; this is a pure implementation detail invisible in output.
8. **Delete timestamps.** `_sdc_deleted_at` is the session's extraction timestamp, not the transaction commit time — wal2json's included timestamps are ignored, and all records in one session share a single `time_extracted`. → Using the payload's commit timestamp is more truthful; note it changes observable output.
9. **`money` as locale-formatted string.** Values pass through with the server's `lc_monetary` formatting (`$1,001.00`), so output depends on server locale. → Faithful but fragile; a numeric-with-currency representation is cleaner if you accept the schema change.
10. **`bit(n>1)` and other unmapped types dropped.** Multi-bit strings, `bytea`, `interval`, geometric types, etc. are marked `unsupported` and never synced. → A new implementation may map more types (e.g. bit strings/`bytea` as strings, `interval` as string); anything mapped must then be supported in *all three* replication paths.
11. **Legacy slot-name fallback.** The un-suffixed slot name is probed before the `tap_id`-suffixed one, purely for compatibility with old deployments. → Optional; drop it if there is no legacy fleet, but document the lookup order you keep.
12. **Naive timestamps assumed UTC.** `timestamp without time zone` values get `+00:00` appended in every path, regardless of the server's actual timezone semantics. This is a documented assumption targets rely on — keep it, but state it prominently in user docs.
13. **State-file feedback loop assumes an orchestrator.** Flush progress during a long-running session depends on some external process writing target-committed state back into the `--state` file; under a plain Singer runner the file never changes and the slot only advances between runs (still correct, just unbounded WAL retention within a run). → Consider an explicit alternative (e.g. reading target state from a pipe/socket) but keep the file mechanism for orchestrator compatibility.
14. **Query-shape quirks.** The incremental subquery-with-inner-order wrapper and the fixed cursor names exist only to coax the query planner / driver; none of it is observable in output. → Free to change.
