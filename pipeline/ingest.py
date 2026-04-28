"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).
"""

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json

from pipeline.utils import load_config, profile_stage, write_delta

_TS_FIELD = pa.field("ingestion_timestamp", pa.timestamp("us"))
_JSONL_BATCH_LINES = 100_000


def _align_batch(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    """Align tbl to schema: fill missing columns with null, drop extra columns."""
    cols = {}
    for field in schema:
        if field.name in tbl.schema.names:
            col = tbl.column(field.name)
            cols[field.name] = col.cast(field.type) if col.type != field.type else col
        else:
            cols[field.name] = pa.nulls(len(tbl), type=field.type)
    return pa.table(cols, schema=schema)


class _JsonlStreamReader:
    """Streams a JSONL file as RecordBatches, _JSONL_BATCH_LINES rows at a time."""

    def __init__(self, path: str):
        self._path = path
        self.schema = self._peek_schema()

    def _peek_schema(self):
        buf = []
        with open(self._path, "rb") as fh:
            for line in fh:
                line = line.rstrip()
                if line:
                    buf.append(line)
                    if len(buf) >= _JSONL_BATCH_LINES:
                        break
        return pa_json.read_json(pa.BufferReader(b"\n".join(buf))).schema

    def __iter__(self):
        buf = []
        with open(self._path, "rb") as fh:
            for line in fh:
                line = line.rstrip()
                if not line:
                    continue
                buf.append(line)
                if len(buf) >= _JSONL_BATCH_LINES:
                    tbl = pa_json.read_json(pa.BufferReader(b"\n".join(buf)))
                    buf.clear()
                    if tbl.schema != self.schema:
                        tbl = _align_batch(tbl, self.schema)
                    yield from tbl.to_batches()
        if buf:
            tbl = pa_json.read_json(pa.BufferReader(b"\n".join(buf)))
            if tbl.schema != self.schema:
                tbl = _align_batch(tbl, self.schema)
            yield from tbl.to_batches()


_STREAM_READERS = {
    "csv":   pa_csv.open_csv,
    "jsonl": _JsonlStreamReader,
}


def _reader_with_timestamp(path: str, fmt: str, ts) -> pa.RecordBatchReader:
    stream = _STREAM_READERS[fmt](path)
    schema = stream.schema.append(_TS_FIELD)

    def _batches():
        for batch in stream:
            ts_col = pa.array([ts] * len(batch), type=pa.timestamp("us"))
            yield pa.record_batch(list(batch.columns) + [ts_col], schema=schema)

    return pa.RecordBatchReader.from_batches(schema, _batches())


def run_ingestion():
    config = load_config()
    bronze = config["output"]["bronze_path"]
    ts = datetime.now(timezone.utc).replace(tzinfo=None)

    for source in config["sources"]:
        with profile_stage(f"ingest.{source['name']}"):
            rbr = _reader_with_timestamp(source["path"], source["format"], ts)
            write_delta(rbr, f"{bronze}/{source['name']}")
