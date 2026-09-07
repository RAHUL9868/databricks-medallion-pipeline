#!/usr/bin/env python3
"""
End-to-end orchestration for the e-commerce Medallion pipeline.

Runs Bronze ingestion, Silver quality validation, Gold analytics builds, and
post-run checks in a single, logged, fail-fast entry point.

Usage
-----
From the repository root (local Spark or Databricks cluster with repo attached):

    python src/run_pipeline.py --help

Typical local run (generate CSVs, ingest, transform):

    python src/run_pipeline.py \\
        --generate-sample-data \\
        --source-base-path file:///path/to/data \\
        --schema ecommerce

Databricks run (CSVs already on DBFS):

    python src/run_pipeline.py --schema ecommerce

Environment variables (see src/config/pipeline_config.py):
    PIPELINE_SOURCE_BASE_PATH, PIPELINE_SCHEMA, PIPELINE_CATALOG,
    PIPELINE_BATCH_ID, PIPELINE_RUN_ID, PIPELINE_*_WRITE_MODE

Exit codes:
    0 — success
    1 — pipeline execution failure
    2 — configuration or usage error
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession

_SRC_ROOT = Path(__file__).resolve().parent
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from bronze.bronze_ingest import (
    BronzeIngestError,
    get_spark,
    ingest_entity,
    validate_source_file,
)
from config.pipeline_config import PipelineConfig, load_config
from config.databricks_runtime import (
    assert_spark_readable_source_path,
    prepare_config_source_for_spark,
)
from data_generation.generate_sample_data import (
    CUSTOMER_TOTAL_COUNT,
    ORDER_TOTAL_COUNT,
    PRODUCT_COUNT,
    DEFAULT_SEED,
    write_sample_datasets,
)
from gold.create_gold_tables import (
    GoldPipelineError,
    build_gold_tables,
    log_gold_row_counts,
    run_reconciliation_checks,
)
from silver.create_silver_tables import apply_quality_pipeline, write_silver_layer
from silver.silver_common import ensure_schema_exists, read_bronze_table

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_PIPELINE_FAILURE = 1
EXIT_CONFIG_ERROR = 2

REMOTE_PATH_PREFIXES = ("dbfs:", "s3:", "abfss:", "gs:", "wasbs:", "hdfs:")

EXPECTED_SAMPLE_ROW_COUNTS = {
    "customers": CUSTOMER_TOTAL_COUNT,
    "products": PRODUCT_COUNT,
    "orders": ORDER_TOTAL_COUNT,
}

BRONZE_INGEST_PLAN: Tuple[Tuple[str, str, str], ...] = (
    ("customers", "customers_csv", "bronze_customers_table"),
    ("products", "products_csv", "bronze_products_table"),
    ("orders", "orders_csv", "bronze_orders_table"),
)


class PipelineError(Exception):
    """Raised when a pipeline stage cannot complete."""


class PipelineConfigurationError(PipelineError):
    """Raised when configuration is invalid before execution starts."""


@dataclass
class PipelineRunSummary:
    """High-level counters collected during the run."""

    run_id: str
    batch_id: str
    schema_name: str
    catalog: Optional[str]
    source_base_path: str
    steps_completed: List[str] = field(default_factory=list)
    bronze_row_counts: Dict[str, int] = field(default_factory=dict)
    silver_row_counts: Dict[str, int] = field(default_factory=dict)
    gold_row_counts: Dict[str, int] = field(default_factory=dict)
    elapsed_seconds: float = 0.0


def is_remote_path(path: str) -> bool:
    """Return True when the path uses a remote/object-store scheme."""
    normalized = path.strip().lower()
    if normalized.startswith("/dbfs/"):
        return True
    return any(normalized.startswith(prefix) for prefix in REMOTE_PATH_PREFIXES)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def validate_configuration(config: PipelineConfig, spark: Optional[SparkSession] = None) -> None:
    """
    Fail fast on invalid or incomplete pipeline configuration.

    Does not touch datasets — validation only.
    """
    if not config.schema_name or not config.schema_name.strip():
        raise PipelineConfigurationError("PIPELINE_SCHEMA / schema_name must be a non-empty string.")

    for mode_name, mode_value in (
        ("bronze_write_mode", config.bronze_write_mode),
        ("silver_write_mode", config.silver_write_mode),
        ("gold_write_mode", config.gold_write_mode),
    ):
        if mode_value not in ("overwrite", "append"):
            raise PipelineConfigurationError(
                f"Invalid {mode_name}='{mode_value}'. Allowed: overwrite, append.",
            )

    if not config.source_base_path or not config.source_base_path.strip():
        raise PipelineConfigurationError(
            "PIPELINE_SOURCE_BASE_PATH / source_base_path must be configured.",
        )

    try:
        assert_spark_readable_source_path(config.source_base_path, spark=spark)
    except ValueError as exc:
        raise PipelineConfigurationError(str(exc)) from exc

    required_tables = (
        config.bronze_customers_table,
        config.bronze_orders_table,
        config.bronze_products_table,
        config.silver_customers_table,
        config.silver_orders_table,
        config.silver_products_table,
        config.gold_sales_by_product_table,
        config.gold_revenue_by_customer_table,
        config.gold_daily_weekly_trends_table,
        config.gold_customer_segmentation_table,
    )
    for table_name in required_tables:
        if not table_name or not table_name.strip():
            raise PipelineConfigurationError("Pipeline table names must be non-empty strings.")

    run_id = config.resolved_run_id()
    batch_id = config.resolved_batch_id()
    if not run_id or not batch_id:
        raise PipelineConfigurationError("run_id and batch_id must resolve to non-empty values.")

    logger.info(
        "Configuration validated schema=%s catalog=%s source_base_path=%s "
        "batch_id=%s run_id=%s bronze_mode=%s silver_mode=%s gold_mode=%s",
        config.schema_name,
        config.catalog or "(default)",
        config.source_base_path,
        batch_id,
        run_id,
        config.bronze_write_mode,
        config.silver_write_mode,
        config.gold_write_mode,
    )


def resolve_sample_data_output_dir(
    config: PipelineConfig,
    cli_output_dir: Optional[str],
) -> Path:
    """
    Resolve where sample CSV files should be written.

    Uses ``cli_output_dir`` when provided. Otherwise uses ``source_base_path``
    when it is a local path. Remote source paths require an explicit output dir.
    """
    if cli_output_dir:
        return Path(cli_output_dir).expanduser()

    if is_remote_path(config.source_base_path):
        raise PipelineConfigurationError(
            "Cannot write generated sample data directly to a remote source path "
            f"('{config.source_base_path}'). Provide --sample-data-output-dir with a "
            "local directory, upload the CSVs to DBFS, then rerun without "
            "--generate-sample-data.",
        )

    base = config.source_base_path
    if base.startswith("file:"):
        return Path(base[5:])
    return Path(base).expanduser()


def generate_sample_data(
    config: PipelineConfig,
    seed: int,
    output_dir: Optional[str],
) -> Path:
    """Generate deterministic sample CSV files (driver-side; small reference datasets)."""
    target_dir = resolve_sample_data_output_dir(config, output_dir)
    logger.info(
        "Generating sample data seed=%s output_dir=%s",
        seed,
        target_dir,
    )
    return write_sample_datasets(target_dir, seed=seed)


def validate_sample_data_sources(
    spark: SparkSession,
    config: PipelineConfig,
    strict_row_counts: bool,
) -> Dict[str, int]:
    """
    Verify source CSV files exist and optionally match expected row counts.

    Row counts use Spark (distributed) — no driver-side collection of records.
    """
    counts: Dict[str, int] = {}
    for entity, csv_attr, _ in BRONZE_INGEST_PLAN:
        filename = getattr(config, csv_attr)
        source_path = config.source_path(filename)
        normalized = validate_source_file(spark, source_path, entity)

        if strict_row_counts:
            row_count = (
                spark.read.format("csv")
                .option("header", "true")
                .load(normalized)
                .count()
            )
            expected = EXPECTED_SAMPLE_ROW_COUNTS[entity]
            if row_count != expected:
                raise PipelineError(
                    f"Sample data row count mismatch for {entity}: "
                    f"path={source_path} actual={row_count} expected={expected}.",
                )
            counts[entity] = row_count
            logger.info(
                "Sample data validated entity=%s path=%s rows=%s",
                entity,
                source_path,
                row_count,
            )
        else:
            counts[entity] = -1
            logger.info("Sample data present entity=%s path=%s", entity, source_path)

    return counts


def ingest_bronze_entity(
    spark: SparkSession,
    config: PipelineConfig,
    entity: str,
    csv_attr: str,
    table_attr: str,
) -> int:
    """Ingest one Bronze entity and return the written row count."""
    filename = getattr(config, csv_attr)
    table_name = getattr(config, table_attr)
    result = ingest_entity(
        entity=entity,
        source_filename=filename,
        target_table_name=table_name,
        spark=spark,
        config=config,
    )
    logger.info(
        "Bronze ingest complete entity=%s rows=%s target=%s batch_id=%s",
        entity,
        result.row_count,
        result.target_table,
        result.batch_id,
    )
    return result.row_count


def run_silver_quality_and_persist(
    spark: SparkSession,
    config: PipelineConfig,
    step_tracker: List[str],
) -> Dict[str, int]:
    """Run Silver quality validations (steps 6–10) and persist outputs (11–12)."""
    run_id = config.resolved_run_id()
    ensure_schema_exists(spark, config)

    bronze_customers = read_bronze_table(spark, config, config.bronze_customers_table)
    bronze_orders = read_bronze_table(spark, config, config.bronze_orders_table)
    bronze_products = read_bronze_table(spark, config, config.bronze_products_table)

    bronze_counts = {
        "customers": bronze_customers.count(),
        "orders": bronze_orders.count(),
        "products": bronze_products.count(),
    }
    logger.info(
        "Silver input Bronze rows customers=%s orders=%s products=%s",
        bronze_counts["customers"],
        bronze_counts["orders"],
        bronze_counts["products"],
    )

    quality_step_map = {
        "completeness_checks": "run_completeness_checks",
        "uniqueness_checks": "run_uniqueness_checks",
        "type_validation": "run_type_validation",
        "referential_integrity_checks": "run_referential_integrity_checks",
        "business_validation": "run_business_validation",
    }

    def on_quality_step(step_name: str) -> None:
        pipeline_step = quality_step_map.get(step_name)
        if pipeline_step:
            step_tracker.append(pipeline_step)

    pipeline_result = apply_quality_pipeline(
        spark,
        bronze_customers,
        bronze_orders,
        bronze_products,
        run_id,
        config=config,
        on_step_complete=on_quality_step,
    )

    step_tracker.append("build_silver_tables")
    step_tracker.append("generate_quality_metrics")
    write_silver_layer(
        spark,
        config,
        pipeline_result,
        bronze_counts,
        run_id,
    )

    silver_counts = {
        "customers": spark.table(
            config.qualified_table_name(config.silver_customers_table),
        ).count(),
        "orders": spark.table(
            config.qualified_table_name(config.silver_orders_table),
        ).count(),
        "products": spark.table(
            config.qualified_table_name(config.silver_products_table),
        ).count(),
    }
    return silver_counts


def run_final_validation(
    spark: SparkSession,
    config: PipelineConfig,
    summary: PipelineRunSummary,
) -> None:
    """
    Post-pipeline sanity checks — table presence and minimum row expectations.

    Distributed counts only; no driver-side dataset materialization.
    """
    required_tables = (
        config.bronze_customers_table,
        config.bronze_orders_table,
        config.bronze_products_table,
        config.silver_customers_table,
        config.silver_orders_table,
        config.silver_products_table,
        config.silver_dq_report_table,
        config.gold_sales_by_product_table,
        config.gold_revenue_by_customer_table,
        config.gold_daily_weekly_trends_table,
        config.gold_customer_segmentation_table,
    )

    for table_name in required_tables:
        qualified = config.qualified_table_name(table_name)
        if not spark.catalog.tableExists(qualified):
            raise PipelineError(
                f"Final validation failed: required table '{qualified}' does not exist.",
            )

    for entity, bronze_count in summary.bronze_row_counts.items():
        silver_count = summary.silver_row_counts.get(entity)
        if silver_count is not None and silver_count != bronze_count:
            raise PipelineError(
                f"Final validation failed: Silver row count for {entity} ({silver_count}) "
                f"does not match Bronze ({bronze_count}).",
            )

    segmentation_table = config.qualified_table_name(config.gold_customer_segmentation_table)
    segment_rows = spark.table(segmentation_table).count()
    if segment_rows != 4:
        raise PipelineError(
            f"Final validation failed: expected 4 segmentation rows, found {segment_rows}.",
        )

    for logical_name, row_count in summary.gold_row_counts.items():
        if row_count <= 0 and logical_name != "daily_weekly_trends":
            raise PipelineError(
                f"Final validation failed: Gold table '{logical_name}' has no rows.",
            )

    logger.info(
        "Final validation passed schema=%s bronze=%s silver=%s gold=%s steps=%s",
        config.schema_name,
        summary.bronze_row_counts,
        summary.silver_row_counts,
        summary.gold_row_counts,
        len(summary.steps_completed),
    )


def run_pipeline(
    config: PipelineConfig,
    *,
    generate_sample_data_flag: bool = False,
    validate_sample_data: bool = True,
    strict_sample_row_counts: bool = False,
    sample_data_seed: int = DEFAULT_SEED,
    sample_data_output_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
    repo_root: Optional[str] = None,
) -> PipelineRunSummary:
    """
    Execute the full Medallion pipeline in order.

    Parameters
    ----------
    generate_sample_data_flag:
        When True, generate reference CSVs before ingestion (small datasets only).
    validate_sample_data:
        When True, verify source CSV files exist before Bronze ingest.
    strict_sample_row_counts:
        When True, assert generated sample files match expected row counts via Spark.
    """
    started = time.monotonic()
    spark = get_spark(spark)

    generated_dir: Optional[Path] = None
    if generate_sample_data_flag:
        generated_dir = generate_sample_data(
            config,
            seed=sample_data_seed,
            output_dir=sample_data_output_dir,
        )

    config = prepare_config_source_for_spark(
        spark,
        config,
        local_csv_dir=str(generated_dir) if generated_dir is not None else None,
        repo_root=repo_root,
    )
    validate_configuration(config, spark=spark)

    summary = PipelineRunSummary(
        run_id=config.resolved_run_id(),
        batch_id=config.resolved_batch_id(),
        schema_name=config.schema_name,
        catalog=config.catalog,
        source_base_path=config.source_base_path,
    )

    try:
        if generate_sample_data_flag:
            summary.steps_completed.append("generate_sample_data")
            if generated_dir is not None and config.source_base_path.startswith("dbfs:"):
                summary.steps_completed.append("upload_sample_data_to_dbfs")

        if validate_sample_data:
            summary.steps_completed.append("validate_sample_data")
            validate_sample_data_sources(
                spark,
                config,
                strict_row_counts=strict_sample_row_counts,
            )

        ensure_schema_exists(spark, config)

        for entity, csv_attr, table_attr in BRONZE_INGEST_PLAN:
            step_name = f"ingest_{entity}"
            summary.steps_completed.append(step_name)
            row_count = ingest_bronze_entity(spark, config, entity, csv_attr, table_attr)
            summary.bronze_row_counts[entity] = row_count

        summary.silver_row_counts = run_silver_quality_and_persist(
            spark,
            config,
            summary.steps_completed,
        )

        summary.steps_completed.append("build_gold_tables")
        gold_tables = build_gold_tables(spark, config)
        summary.gold_row_counts = log_gold_row_counts(spark, gold_tables)

        summary.steps_completed.append("run_reconciliation_checks")
        run_reconciliation_checks(spark, config, gold_tables)

        summary.steps_completed.append("run_final_validation")
        run_final_validation(spark, config, summary)

    except (BronzeIngestError, GoldPipelineError, PipelineError, RuntimeError, ValueError) as exc:
        logger.error(
            "Pipeline failed after steps=%s error=%s",
            summary.steps_completed,
            exc,
        )
        raise PipelineError(str(exc)) from exc

    summary.elapsed_seconds = time.monotonic() - started
    logger.info(
        "Pipeline completed successfully run_id=%s elapsed_seconds=%.2f steps=%s",
        summary.run_id,
        summary.elapsed_seconds,
        summary.steps_completed,
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the e-commerce Medallion pipeline end-to-end (Bronze → Silver → Gold).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipeline stages (in order):
  1. Validate configuration
  2. Generate / validate sample data (optional)
  3–5. Bronze ingest: customers, products, orders
  6–10. Silver quality: completeness, uniqueness, type, referential, business
  11–12. Build Silver tables and DQ metrics
  13. Build Gold tables
  14. Gold reconciliation checks
  15. Final validation

Environment variables: PIPELINE_SCHEMA, PIPELINE_CATALOG, PIPELINE_SOURCE_BASE_PATH,
PIPELINE_BATCH_ID, PIPELINE_RUN_ID, PIPELINE_BRONZE_WRITE_MODE,
PIPELINE_SILVER_WRITE_MODE, PIPELINE_GOLD_WRITE_MODE.

Examples:
  python src/run_pipeline.py --generate-sample-data --source-base-path ./data
  python src/run_pipeline.py --schema ecommerce --run-id demo-001
  python src/run_pipeline.py --generate-sample-data --sample-data-output-dir ./data \\
      --source-base-path dbfs:/FileStore/ecommerce/data
        """,
    )

    parser.add_argument(
        "--source-base-path",
        help="Directory/URI containing source CSV files (PIPELINE_SOURCE_BASE_PATH).",
    )
    parser.add_argument("--catalog", help="Unity Catalog name (PIPELINE_CATALOG).")
    parser.add_argument("--schema", help="Database/schema name (default: ecommerce).")
    parser.add_argument(
        "--bronze-write-mode",
        choices=["overwrite", "append"],
        help="Bronze Delta write mode (default: overwrite).",
    )
    parser.add_argument(
        "--silver-write-mode",
        choices=["overwrite", "append"],
        help="Silver Delta write mode (default: overwrite).",
    )
    parser.add_argument(
        "--gold-write-mode",
        choices=["overwrite", "append"],
        help="Gold write mode label (SQL uses CREATE OR REPLACE).",
    )
    parser.add_argument("--batch-id", help="Shared Bronze ingest batch id.")
    parser.add_argument("--run-id", help="Silver/Gold pipeline run id.")

    parser.add_argument(
        "--generate-sample-data",
        action="store_true",
        help="Generate reference CSV files before ingestion (seed=42 by default).",
    )
    parser.add_argument(
        "--sample-data-output-dir",
        help="Local directory for generated CSVs when source path is remote (e.g. DBFS).",
    )
    parser.add_argument(
        "--sample-data-seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for sample data generation (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--skip-sample-data-validation",
        action="store_true",
        help="Skip pre-ingest source file existence checks.",
    )
    parser.add_argument(
        "--strict-sample-row-counts",
        action="store_true",
        help="Assert source CSV row counts match the reference generator volumes.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = load_config(
            source_base_path=args.source_base_path,
            catalog=args.catalog,
            schema_name=args.schema,
            bronze_write_mode=args.bronze_write_mode,
            silver_write_mode=args.silver_write_mode,
            gold_write_mode=args.gold_write_mode,
            batch_id=args.batch_id,
            run_id=args.run_id,
        )
        run_pipeline(
            config,
            generate_sample_data_flag=args.generate_sample_data,
            validate_sample_data=not args.skip_sample_data_validation,
            strict_sample_row_counts=args.strict_sample_row_counts,
            sample_data_seed=args.sample_data_seed,
            sample_data_output_dir=args.sample_data_output_dir,
        )
        return EXIT_SUCCESS
    except PipelineConfigurationError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR
    except PipelineError as exc:
        logger.error("Pipeline error: %s", exc)
        return EXIT_PIPELINE_FAILURE


if __name__ == "__main__":
    raise SystemExit(main())
