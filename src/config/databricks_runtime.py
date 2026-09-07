"""
Runtime detection helpers for Databricks vs local Spark.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Legacy default for workspaces with public DBFS enabled.
DEFAULT_DBFS_SAMPLE_DATA_PATH = "dbfs:/FileStore/ecommerce/data"

_REMOTE_PATH_PREFIXES = ("dbfs:", "s3:", "abfss:", "gs:", "wasbs:", "hdfs:")
_CSV_NAMES = ("customers.csv", "products.csv", "orders.csv")


def local_filesystem_path(path: str) -> Optional[Path]:
    """Return a driver-local Path for file:/ or absolute paths; None for remote URIs."""
    normalized = path.strip()
    if normalized.lower().startswith("file:"):
        return Path(urlparse(normalized).path)
    if normalized.startswith("/dbfs/"):
        return None
    if any(normalized.lower().startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES):
        return None
    if normalized.startswith("/"):
        return Path(normalized)
    return Path(normalized)


def is_workspace_path(path: str) -> bool:
    """Return True when the path lives under /Workspace (allowed on serverless)."""
    local = local_filesystem_path(path)
    if local is None:
        return False
    return str(local).replace("\\", "/").startswith("/Workspace")


def is_unreadable_local_path_on_databricks(path: str) -> bool:
    """
    Return True for driver-local paths Spark cannot read on Databricks serverless.

    ``file:/tmp/...`` is blocked; ``file:/Workspace/Repos/...`` is allowed.
    """
    if is_workspace_path(path):
        return False
    normalized = path.strip().lower()
    if normalized.startswith("file:"):
        return True
    if normalized.startswith("/tmp/") or normalized == "/tmp":
        return True
    return False


def is_driver_local_path(path: str) -> bool:
    """Backward-compatible alias for unreadable local paths on Databricks."""
    return is_unreadable_local_path_on_databricks(path)


def is_legacy_filestore_path(path: str) -> bool:
    """Return True for deprecated public DBFS FileStore paths."""
    return "/FileStore/" in path.replace("\\", "/")


def is_remote_path(path: str) -> bool:
    """Return True when the path uses DBFS or a cloud object-store scheme."""
    normalized = path.strip().lower()
    if normalized.startswith("/dbfs/"):
        return True
    return any(normalized.startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES)


def workspace_data_path(repo_root: str) -> str:
    """Create ``{repo_root}/data`` and return a file URI Spark can read on serverless."""
    data_dir = Path(repo_root) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir.resolve().as_uri()


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
    Fail fast when a Databricks runtime is configured with a blocked local path.

    Use a workspace repo path (``file:/Workspace/Repos/.../data``), a UC volume,
    or a non-FileStore DBFS path — not ``file:/tmp`` or disabled ``/FileStore``.
    """
    if not is_databricks_runtime(spark):
        return
    if is_unreadable_local_path_on_databricks(path):
        raise ValueError(
            f"Source path '{path}' is not readable by Spark on Databricks serverless. "
            "Use the repo data folder (file:/Workspace/Repos/.../data), a Unity Catalog "
            "volume, or set PIPELINE_SOURCE_BASE_PATH to an allowed location."
        )
    if is_legacy_filestore_path(path):
        raise ValueError(
            f"Source path '{path}' uses public DBFS FileStore, which may be disabled. "
            "Use the repo data folder or set PIPELINE_SOURCE_BASE_PATH to a UC volume path."
        )


def upload_local_csvs_to_dbfs(local_dir: str, dbfs_base: str, spark: SparkSession) -> None:
    """Copy generated sample CSV files from driver-local disk to DBFS via DBUtils."""
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    base = dbfs_base.rstrip("/")
    dbutils.fs.mkdirs(base)
    local = Path(local_dir)
    for name in _CSV_NAMES:
        src = f"file:{(local / name).resolve()}"
        dest = f"{base}/{name}"
        dbutils.fs.cp(src, dest, True)


def stage_local_csvs_for_ingest(
    local_dir: str,
    target_base: str,
    spark: SparkSession,
    *,
    repo_root: Optional[str] = None,
) -> str:
    """
    Copy CSVs from a driver-local staging directory to the ingest location.

    Returns the ``source_base_path`` to use for Bronze reads.
    """
    target_base = target_base.rstrip("/")

    if is_legacy_filestore_path(target_base) and repo_root:
        try:
            upload_local_csvs_to_dbfs(local_dir, target_base, spark)
            return target_base
        except Exception as exc:
            message = str(exc)
            if "DBFS_DISABLED" in message or "DbfsDisabled" in message or "FileStore" in message:
                logger.warning(
                    "Public DBFS FileStore is disabled; staging CSVs under repo data/: %s",
                    repo_root,
                )
                target_base = workspace_data_path(repo_root)
            else:
                raise

    local_target = local_filesystem_path(target_base)
    if local_target is not None:
        local_target.mkdir(parents=True, exist_ok=True)
        src = Path(local_dir)
        for name in _CSV_NAMES:
            shutil.copy2(src / name, local_target / name)
        logger.info("Staged sample CSVs from %s to %s", local_dir, local_target)
        return target_base if target_base.lower().startswith("file:") else local_target.resolve().as_uri()

    upload_local_csvs_to_dbfs(local_dir, target_base, spark)
    return target_base


def resolve_databricks_source_base_path(
    configured_path: str,
    repo_root: Optional[str],
) -> str:
    """Pick a serverless-safe source path when the configured default is not usable."""
    if repo_root and (
        is_legacy_filestore_path(configured_path)
        or is_unreadable_local_path_on_databricks(configured_path)
        or configured_path == DEFAULT_DBFS_SAMPLE_DATA_PATH
    ):
        return workspace_data_path(repo_root)
    return configured_path


def prepare_config_source_for_spark(
    spark: SparkSession,
    config,
    *,
    local_csv_dir: Optional[str] = None,
    repo_root: Optional[str] = None,
):
    """
    On Databricks, stage CSVs to a Spark-readable path and rewrite ``source_base_path``.

    Prefers ``{repo_root}/data`` when public DBFS FileStore is disabled.
    """
    from config.pipeline_config import load_config

    if not is_databricks_runtime(spark):
        return config

    target_base = resolve_databricks_source_base_path(config.source_base_path, repo_root)

    candidate_dirs: list[Path] = []
    if local_csv_dir:
        candidate_dirs.append(Path(local_csv_dir))
    if is_unreadable_local_path_on_databricks(config.source_base_path):
        local_path = local_filesystem_path(config.source_base_path)
        if local_path is not None:
            candidate_dirs.append(local_path)

    staged_base: Optional[str] = None
    for local_dir in candidate_dirs:
        if local_dir.is_dir() and any(local_dir.glob("*.csv")):
            staged_base = stage_local_csvs_for_ingest(
                str(local_dir),
                target_base,
                spark,
                repo_root=repo_root,
            )
            break

    final_base = staged_base or target_base
    if final_base != config.source_base_path:
        logger.warning(
            "Using source_base_path=%s for Databricks ingest (was %s)",
            final_base,
            config.source_base_path,
        )
        return load_config(
            source_base_path=final_base,
            catalog=config.catalog,
            schema_name=config.schema_name,
            bronze_write_mode=config.bronze_write_mode,
            silver_write_mode=config.silver_write_mode,
            gold_write_mode=config.gold_write_mode,
            batch_id=config.batch_id,
            run_id=config.run_id,
        )

    return config
