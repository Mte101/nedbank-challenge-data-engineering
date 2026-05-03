"""
Stage 3 streaming pipeline.

Polls /data/stream/ for new JSONL micro-batch files (lexicographic order = chronological),
parses events applying the same DQ normalisation as the batch pipeline, and writes to two
stream_gold Delta tables:
  - current_balances:      one-row-per-account upsert, running balance from stream events
  - recent_transactions:   last-50-per-account merge, keyed on (account_id, transaction_id)

Run concurrently with the batch pipeline so the first poll cycle (t≈60s) processes all
pre-staged files within the 5-minute SLA window.
"""

import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.utils import profile_stage

_log = logging.getLogger(__name__)

_CB_SCHEMA = pa.schema([
    pa.field("account_id",                pa.string()),
    pa.field("current_balance",           pa.decimal128(18, 2)),
    pa.field("last_transaction_timestamp", pa.timestamp("us")),
    pa.field("updated_at",                pa.timestamp("us")),
])

_RT_SCHEMA = pa.schema([
    pa.field("account_id",            pa.string()),
    pa.field("transaction_id",        pa.string()),
    pa.field("transaction_timestamp", pa.timestamp("us")),
    pa.field("amount",                pa.decimal128(18, 2)),
    pa.field("transaction_type",      pa.string()),
    pa.field("channel",               pa.string()),
    pa.field("updated_at",            pa.timestamp("us")),
])


def _cast_to_schema(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast each column in tbl to match schema exactly (e.g. large_utf8 → utf8)."""
    cols = {}
    for field in schema:
        col = tbl.column(field.name)
        if col.type != field.type:
            col = col.cast(field.type)
        cols[field.name] = col
    return pa.table(cols, schema=schema)


def _delta_parquet_expr(path: str) -> str | None:
    """Return a DuckDB read_parquet([...]) expression for an existing Delta table.

    Returns None if the table does not yet exist or has no files.
    Does not scan data — reads Delta log metadata only.
    """
    try:
        files = DeltaTable(path).files()
        if not files:
            return None
        paths = ", ".join(f"'{path}/{f}'" for f in files)
        return f"read_parquet([{paths}])"
    except Exception:
        return None


def _parse_events(path: str, now: datetime) -> list:
    """Parse a JSONL stream file into a list of normalised event dicts.

    Applies the same amount and timestamp normalisation as the batch ingest layer:
    - Amount: strip leading non-numeric characters (handles "R359.50", "rands120")
    - Timestamp: combine transaction_date + transaction_time; fallback to `now`
    """
    events = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            raw_amount = rec.get("amount")
            try:
                amount = float(re.sub(r"^[^0-9.-]+", "", str(raw_amount)))
            except Exception:
                amount = None

            tx_date = rec.get("transaction_date", "")
            tx_time = rec.get("transaction_time", "")
            try:
                ts = datetime.strptime(f"{tx_date} {tx_time}", "%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = now

            events.append({
                "account_id":            rec.get("account_id"),
                "transaction_id":        rec.get("transaction_id"),
                "transaction_timestamp": ts,
                "amount":                amount,
                "transaction_type":      rec.get("transaction_type"),
                "channel":               rec.get("channel"),
                "updated_at":            now,
            })
    return events


def _update_current_balances(new_events: pa.Table, cb_path: str) -> None:
    """Upsert current_balances with running balance deltas from this batch.

    Balance semantics: CREDIT and REVERSAL add to balance; DEBIT and FEE subtract.
    For accounts with no existing row, the starting balance is 0 (stream-delta-only).
    Accounts not in this batch are preserved unchanged via UNION ALL.
    """
    con = duckdb.connect()
    con.register("new_events", new_events)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE batch_deltas AS
        SELECT
            account_id,
            SUM(CASE
                WHEN transaction_type IN ('CREDIT', 'REVERSAL') THEN CAST(amount AS DOUBLE)
                ELSE -CAST(amount AS DOUBLE)
            END)                       AS balance_delta,
            MAX(transaction_timestamp) AS last_transaction_timestamp,
            MAX(updated_at)            AS updated_at
        FROM new_events
        WHERE account_id IS NOT NULL AND amount IS NOT NULL
        GROUP BY account_id
    """)

    existing_expr = _delta_parquet_expr(cb_path)

    if existing_expr:
        result = con.execute(f"""
            WITH existing AS (
                SELECT
                    account_id,
                    CAST(current_balance AS DOUBLE) AS current_balance,
                    last_transaction_timestamp,
                    updated_at
                FROM {existing_expr}
            )
            SELECT
                b.account_id,
                CAST(COALESCE(e.current_balance, 0.0) + b.balance_delta AS DECIMAL(18,2)) AS current_balance,
                b.last_transaction_timestamp,
                b.updated_at
            FROM batch_deltas b
            LEFT JOIN existing e ON b.account_id = e.account_id
            UNION ALL
            SELECT
                account_id,
                CAST(current_balance AS DECIMAL(18,2)) AS current_balance,
                last_transaction_timestamp,
                updated_at
            FROM existing
            WHERE account_id NOT IN (SELECT account_id FROM batch_deltas)
        """).fetch_arrow_table()
    else:
        result = con.execute("""
            SELECT
                account_id,
                CAST(balance_delta AS DECIMAL(18,2)) AS current_balance,
                last_transaction_timestamp,
                updated_at
            FROM batch_deltas
        """).fetch_arrow_table()

    con.close()

    if len(result) == 0:
        return

    write_deltalake(cb_path, _cast_to_schema(result, _CB_SCHEMA), mode="overwrite")


def _update_recent_transactions(new_events: pa.Table, rt_path: str) -> None:
    """Upsert recent_transactions, retaining only the last 50 rows per account.

    Merge key: (account_id, transaction_id). New events overwrite existing rows
    with the same key. Rows beyond position 50 per account (by transaction_timestamp
    descending) are evicted on each cycle.
    """
    con = duckdb.connect()
    con.register("new_events", new_events)

    existing_expr = _delta_parquet_expr(rt_path)

    if existing_expr:
        result = con.execute(f"""
            SELECT account_id, transaction_id, transaction_timestamp,
                   CAST(amount AS DECIMAL(18,2)) AS amount,
                   transaction_type, channel, updated_at
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY account_id
                           ORDER BY transaction_timestamp DESC
                       ) AS rn
                FROM (
                    SELECT
                        account_id, transaction_id, transaction_timestamp,
                        CAST(amount AS DOUBLE) AS amount,
                        transaction_type, channel, updated_at
                    FROM new_events
                    UNION ALL
                    SELECT
                        account_id, transaction_id, transaction_timestamp,
                        CAST(amount AS DOUBLE) AS amount,
                        transaction_type, channel, updated_at
                    FROM {existing_expr}
                    WHERE (account_id, transaction_id) NOT IN (
                        SELECT account_id, transaction_id FROM new_events
                    )
                ) combined
            ) ranked
            WHERE rn <= 50
        """).fetch_arrow_table()
    else:
        result = con.execute("""
            SELECT account_id, transaction_id, transaction_timestamp,
                   CAST(amount AS DECIMAL(18,2)) AS amount,
                   transaction_type, channel, updated_at
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY account_id
                           ORDER BY transaction_timestamp DESC
                       ) AS rn
                FROM new_events
            ) ranked
            WHERE rn <= 50
        """).fetch_arrow_table()

    con.close()

    if len(result) == 0:
        return

    write_deltalake(rt_path, _cast_to_schema(result, _RT_SCHEMA), mode="overwrite")


def _process_file(filepath: str, stream_gold: str) -> None:
    """Parse one stream file and update both stream_gold tables."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    events = _parse_events(filepath, now)

    valid = [
        e for e in events
        if e["account_id"] and e["transaction_id"] and e["amount"] is not None
    ]
    if not valid:
        _log.warning("No valid events in %s", filepath)
        return

    new_events = pa.table({
        "account_id":            pa.array([e["account_id"]            for e in valid], type=pa.string()),
        "transaction_id":        pa.array([e["transaction_id"]        for e in valid], type=pa.string()),
        "transaction_timestamp": pa.array([e["transaction_timestamp"] for e in valid], type=pa.timestamp("us")),
        "amount":                pa.array([e["amount"]                for e in valid], type=pa.float64()).cast(pa.decimal128(18, 2)),
        "transaction_type":      pa.array([e["transaction_type"]      for e in valid], type=pa.string()),
        "channel":               pa.array([e["channel"]               for e in valid], type=pa.string()),
        "updated_at":            pa.array([e["updated_at"]            for e in valid], type=pa.timestamp("us")),
    }, schema=_RT_SCHEMA)

    cb_path = f"{stream_gold}/current_balances"
    rt_path = f"{stream_gold}/recent_transactions"
    os.makedirs(cb_path, exist_ok=True)
    os.makedirs(rt_path, exist_ok=True)

    _update_current_balances(new_events, cb_path)
    _update_recent_transactions(new_events, rt_path)


def run_stream_loop(config: dict, stop_event: threading.Event) -> None:
    """Poll the stream directory and process new files until stop_event is set.

    Discovers all pre-staged files on the first cycle (lexicographic = chronological order).
    Subsequent cycles are no-ops if no new files appear — the loop keeps running to satisfy
    the 'must not exit after processing the first batch' requirement.
    """
    stream_dir    = config["streaming"]["stream_input_path"]
    stream_gold   = config["streaming"]["stream_gold_path"]
    poll_interval = int(config["streaming"].get("poll_interval_seconds", 60))
    processed: set = set()

    _log.info("Stream loop started — polling %s every %ds", stream_dir, poll_interval)

    while not stop_event.is_set():
        try:
            files = sorted(Path(stream_dir).glob("stream_*.jsonl"))
            new_files = [f for f in files if f.name not in processed]

            for f in new_files:
                with profile_stage(f"stream.{f.name}"):
                    _process_file(str(f), stream_gold)
                processed.add(f.name)
                _log.info("Stream processed: %s (%d total)", f.name, len(processed))

        except Exception as exc:
            _log.error("Stream poll error: %s", exc, exc_info=True)

        stop_event.wait(poll_interval)

    _log.info("Stream loop stopped. %d files processed.", len(processed))
