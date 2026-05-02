import logging
import os
import time
from contextlib import contextmanager

import duckdb
import psutil
import yaml
from deltalake import DeltaTable, write_deltalake

_log = logging.getLogger(__name__)
_proc = psutil.Process()


@contextmanager
def profile_stage(name: str):
    rss_before = _proc.memory_info().rss
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - t0
        rss_after = _proc.memory_info().rss
        delta_mb = (rss_after - rss_before) / 1024 / 1024
        rss_mb = rss_after / 1024 / 1024
        _log.info("[%s] time=%.2fs  mem_delta=%+.1fMB  rss=%.1fMB", name, elapsed, delta_mb, rss_mb)


def load_config() -> dict:
    path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def get_connection() -> duckdb.DuckDBPyConnection:
    os.makedirs("/data/tmp", exist_ok=True)
    con = duckdb.connect()
    con.execute("SET temp_directory='/data/tmp'")
    con.execute("SET memory_limit='1GB'")
    return con


def read_delta(path: str, batch_size: int = 300000):
    # Return the Dataset (not a Scanner) so DuckDB registers it lazily instead
    # of materialising all rows into its buffer pool at con.register() time.
    return DeltaTable(path).to_pyarrow_dataset()


def parquet_expr(path: str) -> str:
    """Return a DuckDB read_parquet([...]) expression for all active Delta files.

    Prefer this over read_delta + con.register for large tables (>500k rows).
    DuckDB's native Parquet reader streams data without buffering the full table.
    """
    files = DeltaTable(path).files()
    paths = ", ".join(f"'{path}/{f}'" for f in files)
    return f"read_parquet([{paths}])"


def write_delta(table, path: str, mode: str = "overwrite") -> None:
    write_deltalake(path, table, mode=mode)
