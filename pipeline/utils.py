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
    os.makedirs("/tmp", exist_ok=True)
    con = duckdb.connect()
    con.execute("SET temp_directory='/data/tmp'")
    con.execute("SET memory_limit='1GB'")
    return con


def read_delta(path: str, batch_size: int = 300000):
    return DeltaTable(path).to_pyarrow_dataset().scanner(batch_size=batch_size)


def write_delta(table, path: str, mode: str = "overwrite") -> None:
    write_deltalake(path, table, mode=mode)
