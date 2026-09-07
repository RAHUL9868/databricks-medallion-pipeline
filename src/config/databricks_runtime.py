"""
Runtime detection helpers for Databricks vs local Spark.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DEFAULT_DBFS_SAMPLE_DATA_PATH = "dbfs:/FileStore/ecommerce/data"

_REMOTE_PATH_PREFIXES = ("dbfs:", "s3:", "abfss:", "gs:", "wasbs:", "hdfs:")


def local_filesystem_path(path: str) -> Optional[Path]:
    """Return a driver-local Path for file:/ or /tmp paths; None for remote URIs."""
    normalized = path.strip()
    if normalized.lower().startswith("file:"):
        return Path(urlparse(normalized).path)
    if normalized.startswith("/dbfs/"):
        return None
    if any(normalized.lower().startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES):
        return None
    if normalized.startswith("/tmp/") or normalized == "/tmp":
        return Path(normalized)
    return Path(normalized)


def is_driver_local_path(path: str) -> bool:
    """
    Return True for driver-only local paths that Spark cannot read on Databricks serverless.

    Examples: file:/tmp/..., /tmp/...
    """
    normalized = path.strip().lower()
    if normalized.startswith("file:"):
        return True
    if normalized.startswith("/tmp/") or normalized == "/tmp":
        return True
    return False


def is_remote_path(path: str) -> bool:
    """Return True when the path uses DBFS or a cloud object-store scheme."""
    normalized = path.strip().lower()
    if normalized.startswith("/dbfs/"):
        return True
    return any(normalized.startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES)


def is_databricks_runtime(spark: Optional[SparkSession] = None) -> bool:
    """
    Return True when executing on Databricks (including serverless).

    Uses environment variables first, then Spark conf / DBUtils because serverless
    notebooks do not always expose DATABRICKS_RUNTIME_VERSION to Python.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True

    if spark is None:
        try:
            from pyspark.sql import SparkSession as _SparkSession

            spark = _SparkSession.getActiveSession()
        except Exception:
            spark = None

    if spark is not None:
        try:
            if spark.conf.get("spark.databricks.clusterUsageTags.clusterName", None):
                return True
        except Exception:
            pass

        try:
            from pyspark.dbutils import DBUtils

            DBUtils(spark)
            return True
        except Exception:
            pass

    return False


def assert_spark_readable_source_path(path: str, spark: Optional[SparkSession] = None) -> None:
    """
    Fail fast when a Databricks runtime is configured with a non-DBFS source path.

    Spark CSV reads require DBFS, cloud object stores, or /Workspace paths — not file:/tmp.
    """
    if is_databricks_runtime(spark) and is_driver_local_path(path):
        raise ValueError(
            f"Source path '{path}' is not readable by Spark on Databricks. "
            "Upload CSVs to DBFS (for example dbfs:/FileStore/ecommerce/data) and set "
            "source_base_path / PIPELINE_SOURCE_BASE_PATH to that DBFS location."
        )


def upload_local_csvs_to_dbfs(local_dir: str, dbfs_base: str, spark: SparkSession) -> None:
    """
    Copy generated sample CSV files from driver-local disk to DBFS.

    Requires Databricks DBUtils.
    """
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    base = dbfs_base.rstrip("/")
    dbutils.fs.mkdirs(base)
    local = Path(local_dir)
    for name in ("customers.csv", "products.csv", "orders.csv"):
        src = f"file:{(local / name).resolve()}"
        dest = f"{base}/{name}"
        dbutils.fs.cp(src, dest, True)


def prepare_config_source_for_spark(
    spark: SparkSession,
    config,
    *,
    local_csv_dir: Optional[str] = None,
):
    """
    On Databricks, rewrite driver-local source paths to DBFS and upload CSVs when present.

    Returns an updated PipelineConfig when upload/rewrite occurs.
    """
    from config.pipeline_config import load_config

    if not is_databricks_runtime(spark):
        return config

    dbfs_base = (
        config.source_base_path
        if is_remote_path(config.source_base_path)
        else DEFAULT_DBFS_SAMPLE_DATA_PATH
    )

    candidate_dirs: list[Path] = []
    if local_csv_dir:
        candidate_dirs.append(Path(local_csv_dir))
    if is_driver_local_path(config.source_base_path):
        local_path = local_filesystem_path(config.source_base_path)
        if local_path is not None:
            candidate_dirs.append(local_path)

    uploaded = False
    for local_dir in candidate_dirs:
        if local_dir.is_dir() and any(local_dir.glob("*.csv")):
            upload_local_csvs_to_dbfs(str(local_dir), dbfs_base, spark)
            logger.info(
                "Uploaded sample CSVs from %s to %s for Databricks Spark ingest",
                local_dir,
                dbfs_base,
            )
            uploaded = True
            break

    if uploaded or is_driver_local_path(config.source_base_path):
        if config.source_base_path != dbfs_base:
            logger.warning(
                "Rewriting source_base_path from %s to %s for Databricks",
                config.source_base_path,
                dbfs_base,
            )
        return load_config(
            source_base_path=dbfs_base,
            catalog=config.catalog,
            schema_name=config.schema_name,
            bronze_write_mode=config.bronze_write_mode,
            silver_write_mode=config.silver_write_mode,
            gold_write_mode=config.gold_write_mode,
            batch_id=config.batch_id,
            run_id=config.run_id,
        )

    return config
