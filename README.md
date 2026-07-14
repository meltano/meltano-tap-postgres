# tap-postgres

A [Singer](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md) tap that
extracts data from PostgreSQL. A clean-room implementation of the behavior specified in
[SPEC.md](SPEC.md), supporting three replication methods per stream:

- **`FULL_TABLE`** — every run extracts the whole table, resumably (xmin watermark), with
  `ACTIVATE_VERSION` semantics so targets can atomically swap in the new copy.
- **`INCREMENTAL`** — extracts rows whose replication key is `>=` the bookmarked value
  (inclusive: at-least-once delivery, the last-seen row is re-emitted each run).
- **`LOG_BASED`** — change data capture from the write-ahead log via the
  [`wal2json`](https://github.com/eulerto/wal2json) logical decoding plugin (format version 2):
  a one-off full-table snapshot, then continuous consumption of inserts/updates/deletes.

## Usage

```bash
# Discovery: write a catalog of every discoverable relation to stdout
tap-postgres --config config.json --discover > catalog.json

# Sync: extract data for the streams selected in the catalog
tap-postgres --config config.json --catalog catalog.json --state state.json
```

| Flag | Meaning |
|---|---|
| `-c, --config <file>` | Required. JSON configuration file. |
| `-s, --state <file>` | Optional. State file with bookmarks from a previous run. The path itself is retained: during log-based sync the tap periodically re-reads it (see *Flush control* below). |
| `--catalog <file>` | Catalog file with stream selection metadata (triggers sync mode). |
| `-d, --discover` | Run discovery mode. |

## Configuration

### Required

| Setting | Type | Description |
|---|---|---|
| `host` | string | PostgreSQL host. |
| `port` | integer | PostgreSQL port. |
| `user` | string | PostgreSQL user. |
| `password` | string | PostgreSQL password. |
| `dbname` | string | Database name to connect to (also used for discovery). |

### Optional

| Setting | Type | Default | Description |
|---|---|---|---|
| `filter_schemas` | string | none | Comma-separated schema names; discovery only inspects these. |
| `ssl` | bool/string | false | Connect with SSL mode `require`. Accepts `true` or the legacy string `"true"`. |
| `default_replication_method` | string | none | `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`, used for selected streams without their own `replication-method` metadata. |
| `tap_id` | string | none | Pipeline identifier; appended to the replication slot name. |
| `itersize` | integer | 20000 | Server-side cursor fetch batch size. |
| `limit` | integer | none | Row limit for INCREMENTAL extraction queries (per run). |
| `max_run_seconds` | integer | 43200 | Log-based: stop after this much total runtime. |
| `logical_poll_total_seconds` | number | 10800 | Log-based: stop after this long without WAL data. |
| `break_at_end_lsn` | boolean | true | Log-based: stop when a message is beyond the end LSN captured at startup. |
| `debug_lsn` | bool/string | false | Add an `_sdc_lsn` property to every log-based record. |
| `use_secondary` | boolean | false | Route FULL_TABLE/INCREMENTAL/discovery reads to a replica. WAL and LSN operations always use the primary. |
| `secondary_host` | string | — | Replica host (required with `use_secondary`). |
| `secondary_port` | integer | — | Replica port (required with `use_secondary`). |

## LOG_BASED setup (operator checklist)

1. PostgreSQL **9.4 or newer**, connecting to the **primary**. Versions affected by a known
   WAL bug are refused: upgrade to at least 9.4.21 / 9.5.16 / 9.6.12 / 10.7 / 11.2.
2. Install **wal2json ≥ 2.3** on the server.
3. Set `wal_level=logical`, and size `max_replication_slots` / `max_wal_senders` (≥ 5 recommended).
4. Create a logical replication slot per database. The tap looks for
   **`tap_postgres_<dbname>`** first, then **`tap_postgres_<dbname>_<tap_id>`**
   (lowercased, with every character outside `[a-z0-9_]` replaced by `_`):

   ```sql
   SELECT pg_create_logical_replication_slot('tap_postgres_mydb_mypipeline', 'wal2json');
   ```

5. The connecting user needs replication privileges.

### Flush control

The WAL position confirmed back to the server only advances past data the *downstream target*
has durably committed. The tap treats the `--state` file as the source of truth for what has
been committed downstream and re-reads it every 10 seconds; the orchestrating process
(e.g. Meltano/PipelineWise) is expected to keep it updated with the target's emitted state.
Under a plain Singer runner the slot still only advances between runs — correct, just with
unbounded WAL retention within a run.

### Semantics targets can rely on

- Destination stream names are `<schema_name>-<table_name>` (also the `tap_stream_id` format).
- `_sdc_deleted_at` is added to every log-based stream (set on deletes); `_sdc_lsn` under `debug_lsn`.
- Naive timestamps are assumed UTC (`+00:00` appended) in every path.
- Out-of-range temporal values degrade to sentinels rather than crash:
  `9999-12-31T23:59:59.999+00:00` for timestamps (including *below*-range values — deliberate),
  `9999-12-31T00:00:00+00:00` for dates. `NaN`/`±Inf` floats and `NaN` numerics become `null`.
- Numeric values never pass through binary floats; message serialization is decimal-precise.
- Delivery is at-least-once: inclusive incremental bounds, snapshot/WAL overlap resolved by
  target upserts, NULL replication keys are never bookmarked.

## Deviations from the reference implementation

Conscious decisions on the incidental behaviors catalogued in SPEC.md §10.2:

- **Bound parameters everywhere** (§10.2.1): bookmark values, slot names and schema filters are
  passed as query parameters, never interpolated.
- **Real JSON booleans accepted** (§10.2.2): `ssl`/`debug_lsn` accept `true` as well as the
  legacy string `"true"`.
- **Unparseable WAL payloads are logged loudly** (§10.2.6) instead of silently skipped.
- **One reused helper connection** for array/hstore reconstruction (§10.2.7) instead of one
  connection per value; literals are passed as bound parameters.
- The deprecated `-p/--properties` flag is **not** carried forward.
- **psycopg2** (2.9.x) remains the driver: psycopg 3 does not yet implement the streaming
  logical-replication protocol required by LOG_BASED (SPEC §7.2).
- Slot prefix is **`tap_postgres`**; the un-suffixed name is probed before the
  `tap_id`-suffixed one (documented lookup order, §10.2.11).

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker (for integration tests).

```bash
uv sync                                  # create the environment
uv run ruff check tap_postgres tests     # lint
uv run ruff format tap_postgres tests    # format
uv run pytest tests/unit                 # unit tests (no database needed)
uv run pytest tests/integration          # integration tests (testcontainers)
uv run pytest tests --cov=tap_postgres   # everything, with coverage
```

Integration tests provision a PostgreSQL primary (built with wal2json,
`wal_level=logical`) and a streaming read replica automatically via
[testcontainers](https://testcontainers-python.readthedocs.io/). To run them against an
externally managed server instead, set `TAP_POSTGRES_HOST`, `TAP_POSTGRES_PORT`,
`TAP_POSTGRES_USER`, `TAP_POSTGRES_PASSWORD`, `TAP_POSTGRES_DBNAME`,
`TAP_POSTGRES_SECONDARY_HOST` and `TAP_POSTGRES_SECONDARY_PORT` — for example after
`docker compose up -d --wait`, which starts the same two servers on ports 5433/5434.

Coverage gates (SPEC §8.1): ≥ 58% from unit tests alone, ≥ 63% from integration tests alone,
≥ 85% combined.

## License

MIT
