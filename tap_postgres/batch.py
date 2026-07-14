"""Singer BATCH message support (MEL-541).

Setting the ``batch_config`` key in the tap configuration opts FULL_TABLE and
INCREMENTAL streams into Singer BATCH message mode: rows are written to local
Apache Arrow IPC files and a BATCH message referencing each file is emitted
instead of per-row RECORD messages. The configuration shape follows the
Meltano Singer SDK's ``batch_config`` setting (``singer_sdk.helpers._batch``),
with one deliberate narrowing: only ``encoding.format: "arrow"`` is supported
for now - ``jsonl``/``parquet`` are deferred (MEL-541).

LOG_BASED streams (including their initial snapshot) always emit RECORD
messages regardless of ``batch_config``.
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
import tempfile
import uuid
from typing import IO, TYPE_CHECKING, Any

import singer

if TYPE_CHECKING:
    import pyarrow as pa

LOGGER = singer.get_logger()

BATCH_FORMATS = frozenset({"arrow"})

# Matches the batch_size default of the tap-mysql Arrow BATCH implementation
# this feature follows (MEL-539).
DEFAULT_BATCH_SIZE = 500_000

if sys.version_info >= (3, 14):
    # Monotonic UUIDs (UUIDv7) sort by creation time, which keeps batch file
    # listings in emission order.
    _uuid = uuid.uuid7
else:
    _uuid = uuid.uuid4


class BatchConfigError(Exception):
    """Invalid ``batch_config`` in the tap configuration."""


@dataclasses.dataclass(frozen=True)
class BatchConfig:
    """Validated configuration for Singer BATCH message mode.

    Constructing an instance validates the field values and that Arrow/ADBC
    support is importable, so a misconfiguration fails at startup rather than
    mid-sync. ``None`` from :meth:`from_config` means BATCH mode is disabled.
    """

    batch_size: int
    root_dir: str
    format: str = "arrow"

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            msg = f"batch_config.batch_size must be a positive integer, got {self.batch_size}"
            raise BatchConfigError(msg)
        if not os.path.isdir(self.root_dir):
            msg = (
                f"batch_config.storage.root does not exist or is not a directory: {self.root_dir!r}"
            )
            raise BatchConfigError(msg)
        if self.format not in BATCH_FORMATS:
            msg = (
                f"batch_config.encoding.format must be one of {sorted(BATCH_FORMATS)}, "
                f"got {self.format!r} (other formats such as 'jsonl' are deferred, MEL-541)"
            )
            raise BatchConfigError(msg)

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> BatchConfig | None:
        """Build a BatchConfig from the tap config, or None (RECORD mode) when absent.

        All sub-keys are optional: ``{"batch_config": {}}`` is enough to opt in
        with the defaults (arrow, batch_size=500000, root=the OS temp directory).
        """
        raw = config.get("batch_config")
        if raw is None:
            return None
        encoding = raw.get("encoding") or {}
        storage = raw.get("storage") or {}
        batch_config = cls(
            batch_size=int(raw.get("batch_size", DEFAULT_BATCH_SIZE)),
            root_dir=storage.get("root", tempfile.gettempdir()),
            format=encoding.get("format", "arrow"),
        )
        LOGGER.info(
            "Using batch_config: format=%s, batch_size=%d, root=%s",
            batch_config.format,
            batch_config.batch_size,
            batch_config.root_dir,
        )
        return batch_config


@dataclasses.dataclass(frozen=True)
class ArrowBatchSource:
    """Selects the Arrow BATCH read path where a psycopg2 connection would otherwise go.

    The sync strategies read rows from a single ``source``: a psycopg2
    connection (RECORD mode) or one of these (BATCH mode). Making the source a
    union keeps the illegal states — connection and batch config both present,
    or both absent — unrepresentable. ``dbname`` overrides the config dbname
    for streams whose catalog carries ``database-name`` metadata.
    """

    batch_config: BatchConfig
    dbname: str | None = None


class ArrowBatchWriter:
    """Writes Singer BATCH messages backed by Arrow IPC files.

    Buffers ``pyarrow.RecordBatch`` objects (as received from ADBC, with no
    per-row Python materialization) until the accumulated rows reach
    ``batch_size``, then writes them to one Arrow IPC file and emits a BATCH
    message. Call :meth:`flush` at the end of a query to emit any partial batch.

    ``output`` (any file-like with ``write``/``flush``) makes the BATCH message
    destination injectable for tests; it defaults to ``sys.stdout``.
    """

    def __init__(
        self, stream_name: str, batch_config: BatchConfig, output: IO[str] | None = None
    ) -> None:
        self.stream_name = stream_name
        self._batch_config = batch_config
        self._output = output if output is not None else sys.stdout
        self._batches: list[pa.RecordBatch] = []
        self._row_count = 0
        self._schema: pa.Schema | None = None

    def write(self, record_batch: pa.RecordBatch) -> bool:
        """Buffer *record_batch*; True when this write flushed a full batch."""
        if record_batch.num_rows == 0:
            return False
        if self._schema is None:
            self._schema = record_batch.schema
        self._batches.append(record_batch)
        self._row_count += record_batch.num_rows
        if self._row_count >= self._batch_config.batch_size:
            self.flush()
            return True
        return False

    def flush(self) -> None:
        """Emit a BATCH message for any buffered batches, then reset (no-op if empty)."""
        if not self._batches:
            return
        import pyarrow.ipc as ipc  # local import: only reached in BATCH mode

        path = os.path.join(self._batch_config.root_dir, f"tap-postgres-{_uuid().hex}.arrow")
        assert self._schema is not None
        with ipc.new_file(path, self._schema) as writer:
            for batch in self._batches:
                writer.write_batch(batch)
        LOGGER.info(
            "Wrote Arrow batch file %s (%d rows, %d bytes)",
            path,
            self._row_count,
            os.path.getsize(path),
        )
        message = {
            "type": "BATCH",
            "stream": self.stream_name,
            "encoding": {"format": "arrow"},
            "manifest": [f"file://{path}"],
        }
        self._output.write(json.dumps(message) + "\n")
        self._output.flush()
        self._batches = []
        self._row_count = 0
        self._schema = None
