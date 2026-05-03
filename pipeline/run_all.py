"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

Stage 3: a streaming polling loop runs concurrently with the batch pipeline
in a background thread. Starting it before the batch ensures stream events
are processed within the 5-minute SLA window (all 12 files are pre-staged
at container start and processed in the first poll cycle at ~t=60s).

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import logging
import threading
import time
from datetime import datetime, timezone

from pipeline.dq_report import run_dq_report
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.stream import run_stream_loop
from pipeline.transform import run_transformation
from pipeline.utils import load_config, profile_stage

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    _log = logging.getLogger(__name__)
    config = load_config()
    run_start = datetime.now(timezone.utc)

    # ── Streaming loop (background thread) ───────────────────────────────────
    # Started before the batch pipeline so the first poll cycle (~60s) processes
    # all pre-staged stream files within the SLA window.
    stop_stream = threading.Event()
    stream_thread = None

    if "streaming" in config:
        stream_thread = threading.Thread(
            target=run_stream_loop,
            args=(config, stop_stream),
            daemon=True,
            name="stream-loop",
        )
        stream_thread.start()
        _log.info("Streaming loop started in background.")
    else:
        _log.warning("No 'streaming' key in config — skipping stream loop.")

    # ── Batch pipeline ────────────────────────────────────────────────────────
    with profile_stage("pipeline.ingest"):
        run_ingestion()
    with profile_stage("pipeline.transform"):
        dq_counts = run_transformation()
    with profile_stage("pipeline.provision"):
        gold_counts = run_provisioning()

    run_end = datetime.now(timezone.utc)
    run_dq_report(dq_counts, gold_counts, config, run_start, run_end)

    # ── Shut down streaming loop ──────────────────────────────────────────────
    if stream_thread is not None:
        poll_interval = int(config["streaming"].get("poll_interval_seconds", 60))
        _log.info("Batch complete. Waiting %ds for final stream poll...", poll_interval + 10)
        time.sleep(poll_interval + 10)
        stop_stream.set()
        stream_thread.join(timeout=120)
        _log.info("Streaming loop shut down.")

    _log.info("Pipeline complete.")
