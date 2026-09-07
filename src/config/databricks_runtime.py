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

# Legacy path for workspaces with public DBFS enabled (do not use on serverless).
DEFAULT_DBFS_SAMPLE_DATA_PATH = "dbfs:/FileStore/ecommerce/data"
DEFAULT_LOCAL_SAMPLE_DATA_PATH = "./data"

_REMOTE_PATH_PREFIXES = ("dbfs:", "s3:", "abfss:", "gs:", "wasbs:", "hdfs:")
_CSV_NAMES = ("customers.csv", "products.csv", "orders.csv")
_LEGACY_SAMPLE_DIR_NAMES = ("ecommerce_medallion_sample_data", ".ecommerce_sample_data")


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


def is_unity_catalog_volume_path(path: str) -> bool:
    """Return True for Unity Catalog volume paths."""
    normalized = path.strip().replace("\\", "/")
    return normalized.startswith("/Volumes/") or normalized.lower().startswith("file:/volumes/")


def is_path_under_repo(path: str, repo_root: str) -> bool:
    """Return True when ``path`` is inside the detected Git repository root."""
    local = local_filesystem_path(path)
    if local is None:
        return False
    try:
        return str(local.resolve()).startswith(str(Path(repo_root).resolve()))
    except OSError:
        return False


def should_use_repo_workspace_data(
    configured_path: str,
    repo_root: Optional[str] = None,
) -> bool:
    """
    Return True when Databricks should ingest from ``{repo_root}/data`` instead.

    Relative paths like ``./data`` or ``.ecommerce_sample_data`` resolve against the
    driver working directory (often the user home folder), not the Git repo.
    """
    normalized = configured_path.strip()
    if repo_root and is_path_under_repo(normalized, repo_root):
        return False
    if is_legacy_filestore_path(normalized) or normalized == DEFAULT_DBFS_SAMPLE_DATA_PATH:
        return True
    if is_unreadable_local_path_on_databricks(normalized):
        return True
    if normalized in ("./data", "data", DEFAULT_LOCAL_SAMPLE_DATA_PATH):
        return True
    if is_unity_catalog_volume_path(normalized):
        return False
    if is_remote_path(normalized):
        return is_legacy_filestore_path(normalized)
    return True


def is_remote_path(path: str) -> bool:
    """Return True when the path uses DBFS or a cloud object-store scheme."""
    normalized = path.strip().lower()
    if normalized.startswith("/dbfs/"):
        return True
    return any(normalized.startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES)


def workspace_data_path(repo_root: str, spark: Optional[SparkSession] = None) -> str:
    """Create ``{repo_root}/data`` and return a Spark-readable path on serverless."""
    data_dir = Path(repo_root) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    resolved = data_dir.resolve()
    path_str = str(resolved).replace("\\", "/")
    if path_str.startswith("/Workspace"):
        return path_str
    return resolved.as_uri()


def _directory_has_sample_csvs(path: Path) -> bool:
    return path.is_dir() and all((path / name).is_file() for name in _CSV_NAMES)


def discover_sample_data_dirs(
    configured_path: str,
    repo_root: Optional[str],
    local_csv_dir: Optional[str],
) -> list[Path]:
    """
    Return driver-local directories that may contain generated sample CSVs.

    Includes legacy notebook locations such as ``/tmp/ecommerce_medallion_sample_data``
    and ``~/.ecommerce_sample_data`` so data is not lost when ingest paths are rewritten.
    """
    candidates: list[Path] = []
    seen: set[str] = set()

    def add(path: Path) -> None:
        key = str(path)
        if key not in seen:
            seen.add(key)
            candidates.append(path)

    if local_csv_dir:
        add(Path(local_csv_dir))

    configured_local = local_filesystem_path(configured_path)
    if configured_local is not None:
        add(configured_local if configured_local.is_dir() else configured_local.parent)

    for name in _LEGACY_SAMPLE_DIR_NAMES:
        add(Path("/tmp") / name)

    if repo_root:
        repo_parent = Path(repo_root).parent
        for name in _LEGACY_SAMPLE_DIR_NAMES:
            add(repo_parent / name)

    return candidates


def _looks_like_repo_root(path: Path) -> bool:
    """Return True when ``path`` appears to be this project's repository root."""
    return (path / "src").is_dir()


def detect_databricks_repo_root(spark: Optional[SparkSession] = None) -> Optional[str]:
    """
    Best-effort detection of the Git repo root on Databricks.

    Checks ``PIPELINE_REPO_ROOT``, the active notebook path, then walks upward
    from the current working directory.
    """
    env_root = os.environ.get("PIPELINE_REPO_ROOT", "").strip()
    if env_root:
        root = Path(env_root)
        if _looks_like_repo_root(root):
            return str(root.resolve())

    if spark is None:
        try:
            from pyspark.sql import SparkSession as _SparkSession

            spark = _SparkSession.getActiveSession()
        except Exception:
            spark = None

    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(spark)
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            if notebook_path:
                repo_rel = os.path.dirname(os.path.dirname(notebook_path))
                repo_root = (
                    f"/Workspace{repo_rel}"
                    if not repo_rel.startswith("/Workspace")
                    else repo_rel
                )
                if _looks_like_repo_root(Path(repo_root)):
                    return repo_root
        except Exception:
            pass

    for parent in (Path.cwd(), *Path.cwd().parents):
        if _looks_like_repo_root(parent):
            return str(parent.resolve())

    return None


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


def _filestore_unavailable_message(configured_path: str) -> str:
    return (
        f"Source path '{configured_path}' uses public DBFS FileStore, which is disabled "
        "on this workspace. Pull the latest repo, run notebooks/run_full_pipeline.ipynb "
        "(cells 1–4), or set PIPELINE_SOURCE_BASE_PATH to "
        "file:/Workspace/Repos/<user>/databricks-medallion-pipeline/data"
    )


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
        raise ValueError(_filestore_unavailable_message(path))


def upload_local_csvs_to_dbfs(local_dir: str, dbfs_base: str, spark: SparkSession) -> None:
    """Copy generated sample CSV files from driver-local disk to DBFS via DBUtils."""
    if is_legacy_filestore_path(dbfs_base) and is_databricks_runtime(spark):
        raise ValueError(_filestore_unavailable_message(dbfs_base))

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
    effective_repo_root = repo_root or detect_databricks_repo_root(spark)

    if is_legacy_filestore_path(target_base):
        if is_databricks_runtime(spark):
            if not effective_repo_root:
                raise ValueError(_filestore_unavailable_message(target_base))
            target_base = workspace_data_path(effective_repo_root, spark=spark)
        else:
            upload_local_csvs_to_dbfs(local_dir, target_base, spark)
            return target_base

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
    spark: Optional[SparkSession] = None,
) -> str:
    """Pick a serverless-safe source path when the configured default is not usable."""
    if not is_databricks_runtime(spark):
        return configured_path

    effective_repo_root = repo_root or detect_databricks_repo_root(spark)
    needs_workspace = should_use_repo_workspace_data(configured_path, effective_repo_root)

    if needs_workspace:
        if effective_repo_root:
            return workspace_data_path(effective_repo_root, spark=spark)
        if is_legacy_filestore_path(configured_path) or configured_path == DEFAULT_DBFS_SAMPLE_DATA_PATH:
            raise ValueError(_filestore_unavailable_message(configured_path))

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

    effective_repo_root = repo_root or detect_databricks_repo_root(spark)
    target_base = resolve_databricks_source_base_path(
        config.source_base_path,
        effective_repo_root,
        spark=spark,
    )

    candidate_dirs = discover_sample_data_dirs(
        config.source_base_path,
        effective_repo_root,
        local_csv_dir,
    )
    target_local = local_filesystem_path(target_base)
    target_has_csvs = target_local is not None and _directory_has_sample_csvs(target_local)

    staged_base: Optional[str] = None
    if not target_has_csvs:
        for local_dir in candidate_dirs:
            if _directory_has_sample_csvs(local_dir):
                if target_local is not None and local_dir.resolve() == target_local.resolve():
                    staged_base = target_base
                    break
                staged_base = stage_local_csvs_for_ingest(
                    str(local_dir),
                    target_base,
                    spark,
                    repo_root=effective_repo_root,
                )
                logger.info(
                    "Copied sample CSVs from %s to ingest path %s",
                    local_dir,
                    staged_base,
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
