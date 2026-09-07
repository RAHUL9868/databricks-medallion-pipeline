"""Shared Spark utilities for Silver layer processing."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from config.pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)

CHECK_CATEGORY_COMPLETENESS = "completeness"
CHECK_CATEGORY_UNIQUENESS = "uniqueness"
CHECK_CATEGORY_REFERENTIAL_INTEGRITY = "referential_integrity"
CHECK_CATEGORY_TYPE_BUSINESS = "type_business"

DQ_METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("entity", StringType(), nullable=False),
        StructField("check_category", StringType(), nullable=False),
        StructField("rule_id", StringType(), nullable=False),
        StructField("rule_description", StringType(), nullable=False),
        StructField("total_rows", LongType(), nullable=False),
        StructField("failed_rows", LongType(), nullable=False),
        StructField("passed_rows", LongType(), nullable=False),
        StructField("pass_pct", DecimalType(5, 2), nullable=False),
        StructField("fail_pct", DecimalType(5, 2), nullable=False),
        StructField("evaluated_at", TimestampType(), nullable=False),
    ]
)


def get_spark(existing: Optional[SparkSession] = None) -> SparkSession:
    if existing is not None:
        return existing
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName("ecommerce-silver").getOrCreate()


def ensure_schema_exists(spark: SparkSession, config: PipelineConfig) -> None:
    if config.catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema_name}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema_name}")


def is_null_or_blank(column_name: str) -> Column:
    """True when a Bronze STRING column is NULL, empty, or whitespace-only."""
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == F.lit(""))


def read_bronze_table(spark: SparkSession, config: PipelineConfig, table_name: str) -> DataFrame:
    qualified = config.qualified_table_name(table_name)
    logger.info("Reading Bronze table %s", qualified)
    if not spark.catalog.tableExists(qualified):
        raise ValueError(
            f"Bronze table '{qualified}' does not exist. Run Bronze ingestion first.",
        )
    return spark.table(qualified)


def read_silver_table(spark: SparkSession, config: PipelineConfig, table_name: str) -> DataFrame:
    qualified = config.qualified_table_name(table_name)
    logger.info("Reading Silver table %s", qualified)
    if not spark.catalog.tableExists(qualified):
        raise ValueError(
            f"Silver table '{qualified}' does not exist. "
            f"Run upstream Silver validation (e.g. completeness) first.",
        )
    return spark.table(qualified)


def read_parent_table(
    spark: SparkSession,
    config: PipelineConfig,
    silver_table_name: str,
    bronze_table_name: str,
) -> DataFrame:
    """
    Read a parent entity table for referential integrity checks.

    Prefers the Silver table when present; otherwise falls back to Bronze.
    """
    silver_qualified = config.qualified_table_name(silver_table_name)
    if spark.catalog.tableExists(silver_qualified):
        logger.info("Using Silver parent table %s for referential integrity", silver_qualified)
        return spark.table(silver_qualified)
    return read_bronze_table(spark, config, bronze_table_name)


def load_reference_keys(
    parent_df: DataFrame,
    key_column: str,
    alias: str,
) -> DataFrame:
    """
    Build a distinct set of non-null parent keys for join-based RI checks.

    Duplicate primary keys in the parent table collapse to one reference key via
    ``distinct()``. Referential integrity only requires that a key exists at least
    once in the parent dataset.
    """
    return (
        parent_df.filter(~is_null_or_blank(key_column))
        .select(F.col(key_column).alias(alias))
        .distinct()
    )


def write_delta_table(
    df: DataFrame,
    qualified_name: str,
    write_mode: str,
) -> None:
    if write_mode not in {"overwrite", "append"}:
        raise ValueError(f"Unsupported write mode '{write_mode}'. Use overwrite or append.")
    (
        df.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(qualified_name)
    )
    logger.info("Wrote Delta table %s (mode=%s)", qualified_name, write_mode)


def write_metrics_table(
    spark: SparkSession,
    metrics_df: DataFrame,
    config: PipelineConfig,
    write_mode: str = "append",
) -> None:
    qualified = config.qualified_table_name(config.silver_dq_metrics_table)
    metrics_df.write.format("delta").mode(write_mode).option(
        "mergeSchema", "true",
    ).saveAsTable(qualified)
    logger.info("Wrote %s DQ metric rows to %s", metrics_df.count(), qualified)
