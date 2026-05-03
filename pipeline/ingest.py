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

import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.csv as pa_csv

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
    """
    Streams a JSONL file as RecordBatches using json.loads() to handle
    columns with mixed JSON types across rows (e.g. amount as numeric in
    some rows and as a quoted string in others).

    Stores `amount` and `transaction_date` as large_string to preserve
    source values exactly when the field type varies across records.
    Adds `_amount_was_string` (bool) to flag rows where amount was
    delivered as a JSON string rather than a numeric literal.
    """

    # Columns that must be stored as large_string to avoid type conflicts
    _FORCE_STRING = {"amount", "transaction_date"}

    def __init__(self, path: str):
        self._path = path
        self.schema = self._peek_schema()

    def _read_batch(self, raw_lines: list) -> pa.Table:
        records = [json.loads(line) for line in raw_lines]
        if not records:
            return None

        # Union of all keys across every record in this batch
        all_keys: dict = {}
        for rec in records:
            for k in rec:
                if k not in all_keys:
                    all_keys[k] = True

        cols = {}
        amount_flags = []

        for key in all_keys:
            vals = [rec.get(key) for rec in records]

            if key == "amount":
                amount_flags = [isinstance(v, str) for v in vals]
                # Store as large_string to preserve both numeric and string amounts
                cols[key] = pa.array(
                    [str(v) if v is not None else None for v in vals],
                    type=pa.large_string(),
                )
            elif key == "transaction_date":
                # Store as large_string so epoch integers and date strings coexist
                cols[key] = pa.array(
                    [str(v) if v is not None else None for v in vals],
                    type=pa.large_string(),
                )
            else:
                try:
                    cols[key] = pa.array(vals)
                except Exception:
                    cols[key] = pa.array(
                        [str(v) if v is not None else None for v in vals],
                        type=pa.large_string(),
                    )

        if not amount_flags:
            amount_flags = [False] * len(records)
        cols["_amount_was_string"] = pa.array(amount_flags, type=pa.bool_())

        return pa.table(cols)

    def _peek_schema(self) -> pa.Schema:
        buf = []
        with open(self._path, "rb") as fh:
            for line in fh:
                line = line.rstrip()
                if line:
                    buf.append(line)
                    if len(buf) >= _JSONL_BATCH_LINES:
                        break
        return self._read_batch(buf).schema

    def __iter__(self):
        buf = []
        with open(self._path, "rb") as fh:
            for line in fh:
                line = line.rstrip()
                if not line:
                    continue
                buf.append(line)
                if len(buf) >= _JSONL_BATCH_LINES:
                    tbl = self._read_batch(buf)
                    buf.clear()
                    if tbl.schema != self.schema:
                        tbl = _align_batch(tbl, self.schema)
                    yield from tbl.to_batches()
        if buf:
            tbl = self._read_batch(buf)
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
