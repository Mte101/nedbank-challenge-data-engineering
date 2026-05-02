"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import logging
from datetime import datetime, timezone

from pipeline.dq_report import run_dq_report
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.transform import run_transformation
from pipeline.utils import load_config, profile_stage

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    run_start = datetime.now(timezone.utc)

    with profile_stage("pipeline.ingest"):
        run_ingestion()
    with profile_stage("pipeline.transform"):
        dq_counts = run_transformation()
    with profile_stage("pipeline.provision"):
        gold_counts = run_provisioning()

    run_end = datetime.now(timezone.utc)
    run_dq_report(dq_counts, gold_counts, load_config(), run_start, run_end)
