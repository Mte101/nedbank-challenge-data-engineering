# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Ellac Thendo Motloung
**Date:** 2026-05-03
**Status:** Final

---

## Context

Stage 3 introduced a real-time requirement: the mobile product team needs current balance and recent transaction data within seconds of a transaction occurring. The daily batch pipeline is insufficient for this SLA. The fintech provides a directory of micro-batch JSONL files at `/data/stream/`, each containing 50–500 transaction events. All 12 stream files are pre-staged at container start and named `stream_YYYYMMDD_HHMMSS_NNNN.jsonl` — lexicographic order gives chronological order. Two new Gold tables are required in `/data/output/stream_gold/`: `current_balances` (one row per account, upsert semantics, 4 fields) and `recent_transactions` (last 50 transactions per account, merge semantics, 7 fields). The SLA mandates that `updated_at` in the output tables is within 300 seconds of the source event's `transaction_timestamp`.

Coming into Stage 3, the pipeline comprised approximately 650 lines across 7 Python modules, all using a DuckDB + deltalake (Python) stack without PySpark. The batch pipeline had been through two significant iterations: Stage 1 established the medallion architecture and Stage 2 added DQ flagging, multi-format date parsing, and memory-safe bucketed processing of 3M transactions.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**What made Stage 3 easier:**

The most directly reusable component was `pipeline/utils.py`, specifically `parquet_expr()` and `write_delta()`. In `stream.py`, `_delta_parquet_expr()` follows the same pattern as `parquet_expr()` in `utils.py` — call `DeltaTable(path).files()` to get active file paths from the Delta log (metadata-only, no data scan), then construct a `read_parquet([...])` expression for DuckDB. This pattern transferred directly with minimal modification. The `profile_stage()` context manager in `utils.py` was also reused as-is for per-file timing in the stream loop.

The JSON parsing pattern from `pipeline/ingest.py` (`_JsonlStreamReader`) established the correct approach for handling mixed-type `amount` fields and variable schemas. Stage 3's `_parse_events()` in `stream.py` reapplied the same `re.sub(r'^[^0-9.-]+', '', str(v))` amount normalisation. Having already solved this problem in Stage 2 meant it was a copy-adapt rather than a re-solve.

The config-driven path setup (`config/pipeline_config.yaml`) made adding the streaming paths trivial — uncomment four lines, reference `config["streaming"]["stream_gold_path"]` in the new module, and the whole output routing works. No code needed to change to support a new output directory.

**What made Stage 3 harder:**

`pipeline/run_all.py` was designed as a flat linear script with no concept of concurrent execution. Adding the streaming loop required retrofitting `threading.Thread` and `threading.Event` into what had been a sequential `ingest → transform → provision` chain. The SLA constraint means streaming must start before batch (so the first poll cycle processes all 12 files within ~60 seconds of container start), but the original `run_all.py` had no mechanism for parallel execution paths. This was the single largest structural change required.

There was also no shared JSONL event parsing utility between `ingest.py` and `stream.py`. The batch ingest layer uses `_JsonlStreamReader` (a class with schema-peek and batched iteration). The streaming layer needs a simpler per-event parser. These ended up as two separate implementations of overlapping logic — `_parse_events()` in `stream.py` and `_JsonlStreamReader` in `ingest.py`.

**Code survival rate:**

Approximately 85% of Stage 1/2 code survived unchanged. `ingest.py`, `transform.py`, `provision.py`, `dq_report.py`, `utils.py`, `dq_rules.yaml`, and `pipeline_config.yaml` (minus the streaming section) were all untouched. Only `run_all.py` required structural modification; `pipeline/stream.py` was entirely new (approximately 230 lines).

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**`run_all.py` as a flat linear script:** The most impactful change would have been designing `run_all.py` to support a `--mode` argument (`batch`, `stream`, or `all`) from Day 1, rather than a single sequential execution path. When Stage 3 required concurrent execution, threading logic had to be bolted onto a script not designed for it. With mode support, the streaming path would have been a first-class execution mode: `if mode in ("stream", "all"): start_stream_thread()`. This would have made the Stage 3 addition a config-level decision rather than a structural refactor.

**No shared JSONL event parsing utility:** In `pipeline/ingest.py`, the `_JsonlStreamReader` class handles all the complexity of mixed-type JSON fields, schema-peeking, and batched iteration. In `pipeline/stream.py`, `_parse_events()` reimplements the amount normalisation and timestamp parsing independently. Both functions apply `re.sub(r'^[^0-9.-]+', '', str(v))` to strip non-numeric amount prefixes. Had I extracted a `parse_jsonl_event(record: dict) -> dict` utility function into `pipeline/utils.py` at Stage 1, both the batch ingest layer and the streaming layer could have consumed it without duplication.

**Inline schema definitions:** The field lists for `current_balances` and `recent_transactions` are defined inline in `pipeline/stream.py` as `_CB_SCHEMA` and `_RT_SCHEMA`. When I added `merchant_subcategory` in Stage 2, I had to update `transform.py`, `provision.py`, and the schema detection logic independently. With a centralised `config/schemas.py` or `pipeline/schemas.py` defining all Gold-layer PyArrow schemas, schema changes would propagate from a single location rather than requiring grep-and-edit across multiple files.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

**Entry point design:** I would have built `run_all.py` as a dispatcher from Day 1: `python -m pipeline.run_all --mode batch` and `python -m pipeline.run_all --mode stream` sharing a common library of stage functions. The batch and streaming paths share utility code (`parquet_expr`, `write_delta`, `load_config`, `profile_stage`) but have completely different execution models — sequential vs. event-driven polling. Keeping them as separate modes behind a shared entry point would have made each path independently testable and easier to reason about.

**Shared ingestion abstraction:** With full Stage 3 visibility from Day 1, I would have designed a single `parse_jsonl_record(rec: dict, now: datetime) -> dict` function in `utils.py` handling amount normalisation, date parsing, and `_amount_was_string` flagging. `_JsonlStreamReader` in `ingest.py` and `_parse_events` in `stream.py` would both call it. This avoids the current situation where the same `re.sub(r'^[^0-9.-]+', ...)` expression exists in two places.

**State management:** For `current_balances`, I chose a read-modify-write-overwrite pattern: read the existing Delta table, apply deltas in DuckDB, write the full updated table back with `mode="overwrite"`. This is correct and memory-safe at the small scale of stream events (50–500 per file, O(hundreds) distinct accounts). If I had known this from Day 1, I would have invested in `DeltaTable.merge()` with an explicit predicate instead — true upsert semantics that only touch affected rows without rewriting unchanged accounts. The `deltalake` Python library supports this from v0.15. The read-modify-write approach works, but at larger scale it becomes a full table rewrite on every poll cycle.

**Output path design:** I would have included `stream_gold_path` in `pipeline_config.yaml` from Day 1 (commented or not), and structured `provision.py` to accept a `gold_path` argument rather than reading it directly from config. This would have made the batch Gold path and the streaming Gold path interchangeable — both just Delta tables at configurable paths — rather than `stream_gold/` feeling like a retrofitted addition with its own separate module.

---

## Appendix

**Stream pipeline data flow:**

```
/data/stream/stream_*.jsonl (pre-staged, 12 files)
    ↓  _parse_events() — json.loads per line, normalise amount + timestamp
    ↓  filter: account_id, transaction_id, amount must be non-null
    ↓  pa.Table (RT_SCHEMA — 7 fields)
    ├─→ _update_current_balances()
    │     DuckDB: GROUP BY account_id → balance_delta
    │     UNION with existing current_balances (COALESCE baseline = 0)
    │     write_deltalake(mode="overwrite")
    └─→ _update_recent_transactions()
          DuckDB: UNION new + existing (exclude dup keys), ROW_NUMBER <= 50
          write_deltalake(mode="overwrite")
```

**Concurrency model:**

```
t=0s    container starts → streaming thread starts
t=60s   first poll cycle → all 12 files discovered and processed
        updated_at = datetime.now()  →  SLA latency ≈ 60–120s  (<300s)
t=0–20m batch pipeline runs (ingest → transform → provision → dq_report)
t=20m+  time.sleep(70) → stop_stream.set() → stream_thread.join()
```
