"""
Shared Bronze ingestion logic for CSV -> Delta tables.

Preserves raw source values (all business columns as STRING), adds ingest metadata,
and records run-level audit rows.
"""

import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

# Allow imports when executed from Databricks notebooks or repo scripts.
_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from bronze.bronze_schemas import ENTITY_SCHEMAS, SOURCE_COLUMN_NAMES
from config.databricks_runtime import spark_path_candidates, to_spark_readable_path
from config.pipeline_config import PipelineConfig, load_config

sys.modules.setdefault(__name__, sys.modules[__name__])

logger = logging.getLogger(__name__)

CORRUPT_RECORD_COLUMN = "_corrupt_record"

BRONZE_INGEST_AUDIT_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), nullable=False),
        StructField("entity", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("source_path", StringType(), nullable=False),
        StructField("target_table", StringType(), nullable=False),
        StructField("write_mode", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("row_count", LongType(), nullable=True),
        StructField("source_column_count", LongType(), nullable=True),
        StructField("bronze_column_count", LongType(), nullable=True),
        StructField("corrupt_record_count", LongType(), nullable=True),
        StructField("ingest_started_at", TimestampType(), nullable=False),
        StructField("ingest_completed_at", TimestampType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
    ]
)


class IngestResult:
    """Outcome of a single Bronze entity ingest."""

    def __init__(
        self,
        entity: str,
        source_path: str,
        target_table: str,
        row_count: int,
        source_column_count: int,
        bronze_column_count: int,
        corrupt_record_count: int,
        batch_id: str,
        write_mode: str,
    ) -> None:
        self.entity = entity
        self.source_path = source_path
        self.target_table = target_table
        self.row_count = row_count
        self.source_column_count = source_column_count
        self.bronze_column_count = bronze_column_count
        self.corrupt_record_count = corrupt_record_count
        self.batch_id = batch_id
        self.write_mode = write_mode


class BronzeIngestError(Exception):
    """Raised when Bronze ingestion cannot complete."""


def get_spark(existing: Optional[SparkSession] = None) -> SparkSession:
    """Return an existing Spark session or create a local one for development."""
    if existing is not None:
        return existing
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName("ecommerce-bronze-ingest").getOrCreate()


def ensure_schema_exists(spark: SparkSession, config: PipelineConfig) -> None:
    """Create the target database/schema if it does not exist."""
    if config.catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema_name}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema_name}")


def normalize_source_path(path: str) -> str:
    """Normalize local paths for Spark reads on Databricks and local dev."""
    if path.startswith("dbfs:/") or path.startswith("s3://") or path.startswith("abfss://"):
        return path
    if path.startswith("/dbfs/"):
        return "dbfs:" + path[5:]
    return to_spark_readable_path(path)


_REMOTE_PATH_PREFIXES = ("dbfs:", "s3:", "abfss:", "gs:", "wasbs:", "hdfs:")


def _local_filesystem_path(path: str) -> Optional[Path]:
    """
    Return a driver-local Path when ``path`` refers to the local filesystem.

    Supports ``file://`` / ``file:/`` URIs and plain local paths. Returns None for
    remote/object-store schemes (dbfs, s3, etc.).
    """
    if path.startswith("file:"):
        return Path(urlparse(path).path)
    if path.startswith("/dbfs/"):
        return None
    if any(path.startswith(prefix) for prefix in _REMOTE_PATH_PREFIXES):
        return None
    return Path(path)


def _path_exists_dbutils(spark: SparkSession, path: str) -> Optional[bool]:
    """
    Use Databricks DBUtils when available (serverless-safe).

    Returns None when DBUtils is not on the classpath.
    """
    try:
        from pyspark.dbutils import DBUtils
    except ImportError:
        return None

    dbutils = DBUtils(spark)
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        message = str(exc).lower()
        if any(
            token in message
            for token in ("does not exist", "pathnotfound", "file not found", "not found")
        ):
            return False
        raise


def _path_exists_spark_read(spark: SparkSession, path: str) -> bool:
    """Probe path readability without ``spark._jvm`` (Databricks serverless-safe)."""
    try:
        spark.read.format("binaryFile").load(path).limit(1).count()
        return True
    except AnalysisException as exc:
        message = str(exc).lower()
        if any(
            token in message
            for token in (
                "path does not exist",
                "does not exist",
                "path_not_found",
                "unable to infer",
            )
        ):
            return False
        raise
    except Exception as exc:
        message = str(exc).lower()
        if "does not exist" in message or "path_not_found" in message:
            return False
        raise


def path_exists(spark: SparkSession, path: str) -> bool:
    """
    Check whether a path exists using the active Spark filesystem.

    Does not use ``spark._jvm`` so this works on Databricks serverless compute.
    On serverless, ``Path.exists()`` may be False for ``/Workspace`` files that
    Spark can still read via ``file:/Workspace/...`` — so workspace paths are
    verified with Spark/dbutils, not driver pathlib alone.
    """
    for candidate in spark_path_candidates(path):
        local = _local_filesystem_path(candidate)
        path_str = str(local).replace("\\", "/") if local is not None else candidate

        if path_str.startswith("/Workspace") or candidate.startswith("file:/Workspace"):
            if _path_exists_spark_read(spark, candidate):
                return True
            dbutils_result = _path_exists_dbutils(spark, candidate)
            if dbutils_result is True:
                return True
            continue

        if local is not None and local.is_file():
            return True

        dbutils_result = _path_exists_dbutils(spark, candidate)
        if dbutils_result is True:
            return True

        if _path_exists_spark_read(spark, candidate):
            return True

    return False


def validate_source_file(spark: SparkSession, source_path: str, entity: str) -> str:
    """Validate source file presence and return normalized path."""
    normalized = normalize_source_path(source_path)
    if not path_exists(spark, normalized):
        raise BronzeIngestError(
            f"Missing source file for {entity}: '{source_path}'. "
            f"Upload CSV to the configured source path or set PIPELINE_SOURCE_BASE_PATH.",
        )
    return normalized


def read_source_csv(spark: SparkSession, source_path: str, entity: str) -> DataFrame:
    """
    Read CSV using an explicit all-string schema for source columns.

    PERMISSIVE mode captures malformed lines in `_corrupt_record` instead of
    silently dropping rows.
    """
    source_columns = SOURCE_COLUMN_NAMES[entity]
    source_schema = StructType(
        [StructField(name, StringType(), nullable=True) for name in source_columns],
    )

    try:
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .schema(source_schema)
            .load(source_path)
        )
    except AnalysisException as exc:
        raise BronzeIngestError(
            f"Failed to parse CSV for {entity} at '{source_path}'. "
            f"Check file format and header row. Details: {exc}",
        ) from exc

    return raw_df


def validate_csv_columns(raw_df: DataFrame, entity: str) -> None:
    """Fail fast when required source columns are missing from the CSV header."""
    expected = set(SOURCE_COLUMN_NAMES[entity])
    actual = set(raw_df.columns) - {CORRUPT_RECORD_COLUMN}
    missing = sorted(expected - actual)
    if missing:
        raise BronzeIngestError(
            f"CSV for {entity} is missing required columns: {missing}. "
            f"Found columns: {sorted(actual)}",
        )


def add_bronze_metadata(
    raw_df: DataFrame,
    source_path: str,
    source_file: str,
    batch_id: str,
) -> DataFrame:
    """Append Bronze ingest metadata without modifying business column values."""
    ordered = [F.col(name) for name in raw_df.columns if name != CORRUPT_RECORD_COLUMN]
    if CORRUPT_RECORD_COLUMN not in raw_df.columns:
        raw_df = raw_df.withColumn(CORRUPT_RECORD_COLUMN, F.lit(None).cast("string"))

    ordered.append(F.col(CORRUPT_RECORD_COLUMN))

    with_row_num = raw_df.withColumn("_tmp_row_order", F.monotonically_increasing_id())
    window = Window.orderBy(F.col("_tmp_row_order"))

    return (
        with_row_num.select(
            *ordered,
            F.current_timestamp().alias("_ingest_ts"),
            F.lit(source_file).alias("_source_file"),
            F.lit(source_path).alias("_source_path"),
            F.lit(batch_id).alias("_batch_id"),
            F.row_number().over(window).alias("_source_row_num"),
        )
        .drop("_tmp_row_order")
    )


def align_to_bronze_schema(df: DataFrame, entity: str) -> DataFrame:
    """Select columns in the explicit Bronze schema order."""
    schema = ENTITY_SCHEMAS[entity]
    return df.select([F.col(field.name) for field in schema.fields])


def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    write_mode: str,
) -> None:
    """Write DataFrame to a Delta table."""
    if write_mode not in {"overwrite", "append"}:
        raise BronzeIngestError(
            f"Unsupported bronze write mode '{write_mode}'. Use 'overwrite' or 'append'.",
        )

    (
        df.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def write_audit_record(
    spark: SparkSession,
    config: PipelineConfig,
    record: Dict[str, object],
) -> None:
    """Persist a run-level ingest audit row (append-only history)."""
    audit_table = config.qualified_table_name(config.bronze_ingest_audit_table)
    audit_df = spark.createDataFrame([record], schema=BRONZE_INGEST_AUDIT_SCHEMA)
    audit_df.write.format("delta").mode("append").option(
        "mergeSchema", "true",
    ).saveAsTable(audit_table)


def ingest_entity(
    entity: str,
    source_filename: str,
    target_table_name: str,
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> IngestResult:
    """
    Ingest one CSV entity into a Bronze Delta table.

    Parameters
    ----------
    entity:
        One of 'customers', 'orders', 'products'.
    source_filename:
        CSV file name under the configured source base path.
    target_table_name:
        Bronze Delta table name (without catalog/schema prefix).
    """
    if entity not in ENTITY_SCHEMAS:
        raise BronzeIngestError(f"Unsupported entity '{entity}'.")

    spark = get_spark(spark)
    config = config or load_config()
    batch_id = config.resolved_batch_id()
    write_mode = config.bronze_write_mode
    source_path = config.source_path(source_filename)
    target_table = config.qualified_table_name(target_table_name)
    started_at = datetime.now(timezone.utc)

    logger.info(
        "Starting Bronze ingest entity=%s source=%s target=%s mode=%s batch_id=%s",
        entity,
        source_path,
        target_table,
        write_mode,
        batch_id,
    )

    audit_base = {
        "batch_id": batch_id,
        "entity": entity,
        "source_file": source_filename,
        "source_path": source_path,
        "target_table": target_table,
        "write_mode": write_mode,
        "ingest_started_at": started_at,
    }

    try:
        ensure_schema_exists(spark, config)
        normalized_path = validate_source_file(spark, source_path, entity)
        raw_df = read_source_csv(spark, normalized_path, entity)
        validate_csv_columns(raw_df, entity)

        source_column_count = len(SOURCE_COLUMN_NAMES[entity])
        bronze_df = add_bronze_metadata(
            raw_df,
            source_path=normalized_path,
            source_file=source_filename,
            batch_id=batch_id,
        )
        bronze_df = align_to_bronze_schema(bronze_df, entity)

        row_count = bronze_df.count()
        corrupt_record_count = bronze_df.filter(
            F.col(CORRUPT_RECORD_COLUMN).isNotNull(),
        ).count()
        bronze_column_count = len(bronze_df.columns)

        if corrupt_record_count > 0:
            logger.warning(
                "Entity=%s has %s malformed CSV line(s) captured in _corrupt_record.",
                entity,
                corrupt_record_count,
            )

        write_bronze_table(spark, bronze_df, target_table, write_mode)

        completed_at = datetime.now(timezone.utc)
        result = IngestResult(
            entity=entity,
            source_path=normalized_path,
            target_table=target_table,
            row_count=row_count,
            source_column_count=source_column_count,
            bronze_column_count=bronze_column_count,
            corrupt_record_count=corrupt_record_count,
            batch_id=batch_id,
            write_mode=write_mode,
        )

        write_audit_record(
            spark,
            config,
            {
                **audit_base,
                "status": "SUCCESS",
                "row_count": row_count,
                "source_column_count": source_column_count,
                "bronze_column_count": bronze_column_count,
                "corrupt_record_count": corrupt_record_count,
                "ingest_completed_at": completed_at,
                "error_message": None,
            },
        )

        logger.info(
            "Bronze ingest complete entity=%s rows=%s source_columns=%s bronze_columns=%s corrupt_rows=%s table=%s",
            entity,
            row_count,
            source_column_count,
            bronze_column_count,
            corrupt_record_count,
            target_table,
        )
        return result

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        logger.error("Bronze ingest failed entity=%s error=%s", entity, error_message)
        logger.debug(traceback.format_exc())

        try:
            write_audit_record(
                spark,
                config,
                {
                    **audit_base,
                    "status": "FAILED",
                    "row_count": None,
                    "source_column_count": len(SOURCE_COLUMN_NAMES[entity]),
                    "bronze_column_count": None,
                    "corrupt_record_count": None,
                    "ingest_completed_at": datetime.now(timezone.utc),
                    "error_message": error_message[:2000],
                },
            )
        except Exception as audit_exc:
            logger.error("Failed to write ingest audit record: %s", audit_exc)

        if isinstance(exc, BronzeIngestError):
            raise
        raise BronzeIngestError(error_message) from exc


def ingest_all_entities(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> List[IngestResult]:
    """Ingest customers, products, and orders into Bronze tables."""
    base_config = config or load_config()
    batch_id = base_config.resolved_batch_id()
    config = load_config(
        source_base_path=base_config.source_base_path,
        catalog=base_config.catalog,
        schema_name=base_config.schema_name,
        bronze_write_mode=base_config.bronze_write_mode,
        batch_id=batch_id,
        bronze_customers_table=base_config.bronze_customers_table,
        bronze_orders_table=base_config.bronze_orders_table,
        bronze_products_table=base_config.bronze_products_table,
        bronze_ingest_audit_table=base_config.bronze_ingest_audit_table,
        customers_csv=base_config.customers_csv,
        orders_csv=base_config.orders_csv,
        products_csv=base_config.products_csv,
    )
    spark = get_spark(spark)

    plan = [
        ("customers", config.customers_csv, config.bronze_customers_table),
        ("products", config.products_csv, config.bronze_products_table),
        ("orders", config.orders_csv, config.bronze_orders_table),
    ]

    results: List[IngestResult] = []
    for entity, filename, table_name in plan:
        results.append(
            ingest_entity(
                entity=entity,
                source_filename=filename,
                target_table_name=table_name,
                spark=spark,
                config=config,
            ),
        )
    return results
