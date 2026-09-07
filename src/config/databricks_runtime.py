"""
Runtime detection helpers for Databricks vs local Spark.
"""

from __future__ import annotations

import os


def is_databricks_runtime() -> bool:
    """Return True when executing inside a Databricks cluster or serverless environment."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def is_driver_local_path(path: str) -> bool:
    """
    Return True for driver-only local paths that Spark cannot read on Databricks serverless.

    Examples: file:/tmp/..., /tmp/...
    """
    normalized = path.strip()
    lowered = normalized.lower()
    if lowered.startswith("file:"):
        return True
    if lowered.startswith("/tmp/") or lowered == "/tmp":
        return True
    return False


def assert_spark_readable_source_path(path: str) -> None:
    """
    Fail fast when a Databricks runtime is configured with a non-DBFS source path.

    Spark CSV reads require DBFS, cloud object stores, or /Workspace paths — not file:/tmp.
    """
    if is_databricks_runtime() and is_driver_local_path(path):
        raise ValueError(
            f"Source path '{path}' is not readable by Spark on Databricks. "
            "Upload CSVs to DBFS (for example dbfs:/FileStore/ecommerce/data) and set "
            "source_base_path / PIPELINE_SOURCE_BASE_PATH to that DBFS location."
        )


def upload_local_csvs_to_dbfs(local_dir: str, dbfs_base: str, spark) -> None:
    """
    Copy generated sample CSV files from driver-local disk to DBFS.

    Requires Databricks DBUtils (no-op import failure on local dev).
    """
    from pathlib import Path

    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    base = dbfs_base.rstrip("/")
    dbutils.fs.mkdirs(base)
    local = Path(local_dir)
    for name in ("customers.csv", "products.csv", "orders.csv"):
        src = f"file:{(local / name).resolve()}"
        dest = f"{base}/{name}"
        dbutils.fs.cp(src, dest, True)
