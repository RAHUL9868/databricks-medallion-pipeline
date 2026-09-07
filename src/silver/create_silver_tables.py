#!/usr/bin/env python3
"""
Create consolidated Silver datasets from Bronze with a unified quality model.

Design decision: single primary Silver tables (``silver_customers``, ``silver_orders``,
``silver_products``) retain **all** rows including invalid records. Invalid data is
**not** moved to a separate quarantine table because:

- The assignment requires bad rows to remain traceable and never deleted.
- A single table keeps lineage simple: Bronze metadata + ``_source_*`` raw values +
  typed columns + quality flags on the same row.
- Consumers filter with ``dq_is_valid`` or use convenience views
  (``silver_*_valid``) for analytics-ready subsets.

``dq_record_status`` distinguishes ``valid``, ``invalid`` (one failure), and
``invalid_multiple`` (two or more failed rules).

Pipeline orchestration
----------------------
1. Read Bronze tables.
2. Apply completeness → uniqueness → type → referential integrity → business logic.
3. Consolidate category pass flags from ``dq_failed_rules`` (authoritative).
4. Cast business columns to typed Silver schema; preserve Bronze strings as ``_source_*``.
5. Write entity tables, metrics, report, and valid-record views.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
from datetime import datetime, timezone
from functools import reduce
from operator import or_
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from bronze.bronze_schemas import SOURCE_COLUMN_NAMES
from config.pipeline_config import PipelineConfig, load_config
from silver.silver_common import (
    ensure_schema_exists,
    get_spark,
    is_null_or_blank,
    read_bronze_table,
    write_delta_table,
    write_metrics_table,
)
from silver.silver_dq_rules import (
    ENTITY_BUSINESS_RULE_IDS,
    ENTITY_COMPLETENESS_RULE_IDS,
    ENTITY_REFERENTIAL_RULE_IDS,
    ENTITY_TYPE_RULE_IDS,
    ENTITY_UNIQUENESS_RULE_IDS,
)

logger = logging.getLogger(__name__)

DATE_FORMAT = "yyyy-MM-dd"
EMPTY_STRING_ARRAY = F.array().cast(ArrayType(StringType()))

SILVER_METADATA_COLUMNS: Tuple[str, ...] = (
    "_ingest_ts",
    "_source_file",
    "_source_path",
    "_batch_id",
    "_source_row_num",
    "_corrupt_record",
)

DQ_COLUMNS: Tuple[str, ...] = (
    "dq_is_valid",
    "dq_record_status",
    "dq_failed_rules",
    "dq_failure_count",
    "dq_completeness_pass",
    "dq_uniqueness_pass",
    "dq_referential_pass",
    "dq_type_business_pass",
    "dq_business_logic_pass",
    "dq_completeness_errors",
    "dq_uniqueness_errors",
    "dq_referential_errors",
    "dq_type_business_errors",
    "dq_business_logic_errors",
    "dq_error_messages",
    "dq_checked_at",
    "dq_run_id",
)


def _load_step_module(file_name: str, module_name: str):
    """Load numbered Silver step modules by file path."""
    module_path = Path(__file__).parent / file_name
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _category_failure(failed_rules: Column, rule_ids: frozenset) -> Column:
    """True when any rule id from the category appears in failed_rules."""
    if not rule_ids:
        return F.lit(False)
    checks = [F.array_contains(failed_rules, F.lit(rule_id)) for rule_id in sorted(rule_ids)]
    return reduce(or_, checks)


def _ensure_error_arrays(df: DataFrame) -> DataFrame:
    """Initialize optional per-category error arrays when upstream steps were skipped."""
    result = df
    for column_name in (
        "dq_completeness_errors",
        "dq_uniqueness_errors",
        "dq_referential_errors",
        "dq_type_business_errors",
        "dq_business_logic_errors",
    ):
        if column_name not in result.columns:
            result = result.withColumn(column_name, EMPTY_STRING_ARRAY)
    if "dq_failed_rules" not in result.columns:
        result = result.withColumn("dq_failed_rules", EMPTY_STRING_ARRAY)
    return result


def consolidate_quality_model(df: DataFrame, entity: str, run_id: str) -> DataFrame:
    """
    Recompute authoritative quality columns from ``dq_failed_rules``.

    Prevents contradictory category flags when incremental modules disagree.
    """
    checked_at = datetime.now(timezone.utc)
    working = _ensure_error_arrays(df)
    failed_rules = F.coalesce(F.col("dq_failed_rules"), EMPTY_STRING_ARRAY)

    completeness_fail = _category_failure(
        failed_rules,
        ENTITY_COMPLETENESS_RULE_IDS.get(entity, frozenset()),
    )
    uniqueness_fail = _category_failure(
        failed_rules,
        ENTITY_UNIQUENESS_RULE_IDS.get(entity, frozenset()),
    )
    referential_fail = _category_failure(
        failed_rules,
        ENTITY_REFERENTIAL_RULE_IDS.get(entity, frozenset()),
    )
    type_fail = _category_failure(
        failed_rules,
        ENTITY_TYPE_RULE_IDS.get(entity, frozenset()),
    )
    business_fail = _category_failure(
        failed_rules,
        ENTITY_BUSINESS_RULE_IDS.get(entity, frozenset()),
    )
    type_business_fail = type_fail | business_fail

    error_messages = F.expr(
        """
        filter(
            array_distinct(
                flatten(
                    array(
                        coalesce(dq_completeness_errors, array()),
                        coalesce(dq_uniqueness_errors, array()),
                        coalesce(dq_referential_errors, array()),
                        coalesce(dq_type_business_errors, array()),
                        coalesce(dq_business_logic_errors, array())
                    )
                )
            ),
            x -> x is not null
        )
        """,
    )

    failure_count = F.size(failed_rules)

    return (
        working.withColumn("dq_failed_rules", failed_rules)
        .withColumn("dq_failure_count", failure_count)
        .withColumn("dq_completeness_pass", ~completeness_fail)
        .withColumn("dq_uniqueness_pass", ~uniqueness_fail)
        .withColumn("dq_referential_pass", ~referential_fail)
        .withColumn("dq_business_logic_pass", ~business_fail)
        .withColumn("dq_type_business_pass", ~type_business_fail)
        .withColumn("dq_is_valid", failure_count == F.lit(0))
        .withColumn(
            "dq_record_status",
            F.when(failure_count == F.lit(0), F.lit("valid"))
            .when(failure_count == F.lit(1), F.lit("invalid"))
            .otherwise(F.lit("invalid_multiple")),
        )
        .withColumn("dq_error_messages", error_messages)
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_checked_at", F.lit(checked_at))
    )


def _cast_int(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(is_null_or_blank(column_name), F.lit(None).cast("int")).otherwise(
        trimmed.cast("int"),
    )


def _cast_decimal(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(is_null_or_blank(column_name), F.lit(None).cast("decimal(18,2)")).otherwise(
        trimmed.cast("decimal(18,2)"),
    )


def _cast_date(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(is_null_or_blank(column_name), F.lit(None).cast("date")).otherwise(
        F.to_date(trimmed, DATE_FORMAT),
    )


def _cast_string(column_name: str) -> Column:
    return F.when(is_null_or_blank(column_name), F.lit(None).cast("string")).otherwise(
        F.trim(F.col(column_name)),
    )


CUSTOMER_CAST_MAP = {
    "customer_id": _cast_int,
    "customer_name": _cast_string,
    "email": _cast_string,
    "country": _cast_string,
    "signup_date": _cast_date,
    "customer_segment": _cast_string,
    "lifetime_value": _cast_decimal,
}

ORDER_CAST_MAP = {
    "order_id": _cast_int,
    "customer_id": _cast_int,
    "order_date": _cast_date,
    "product_id": _cast_int,
    "quantity": _cast_int,
    "unit_price": _cast_decimal,
    "total_amount": _cast_decimal,
    "order_status": _cast_string,
    "payment_date": _cast_date,
}

PRODUCT_CAST_MAP = {
    "product_id": _cast_int,
    "product_name": _cast_string,
    "category": _cast_string,
    "price": _cast_decimal,
    "cost": _cast_decimal,
    "stock_quantity": _cast_int,
    "reorder_level": _cast_int,
}

ENTITY_CAST_MAP = {
    "customers": CUSTOMER_CAST_MAP,
    "orders": ORDER_CAST_MAP,
    "products": PRODUCT_CAST_MAP,
}


def add_source_traceability_columns(df: DataFrame, entity: str) -> DataFrame:
    """Preserve Bronze string values as ``_source_<column>`` before casting."""
    result = df
    for column_name in SOURCE_COLUMN_NAMES[entity]:
        source_column = f"_source_{column_name}"
        if source_column not in result.columns:
            result = result.withColumn(source_column, F.col(column_name))
    return result


def cast_business_columns(df: DataFrame, entity: str) -> DataFrame:
    """Cast Bronze STRING business columns to typed Silver columns."""
    cast_map = ENTITY_CAST_MAP[entity]
    result = df
    for column_name, cast_fn in cast_map.items():
        if column_name in result.columns:
            result = result.withColumn(column_name, cast_fn(column_name))
    return result


def finalize_silver_schema(df: DataFrame, entity: str) -> DataFrame:
    """Add traceability columns and cast business fields to Silver types."""
    with_sources = add_source_traceability_columns(df, entity)
    return cast_business_columns(with_sources, entity)


def build_quality_report(metrics_df: DataFrame) -> DataFrame:
    """Transform internal metrics into the reporting schema."""
    return metrics_df.select(
        F.col("entity").alias("dataset"),
        F.col("rule_id").alias("check_name"),
        F.col("total_rows").alias("total_records"),
        F.col("passed_rows").alias("passed_records"),
        F.col("failed_rows").alias("failed_records"),
        F.col("pass_pct").alias("pass_percentage"),
        F.col("evaluated_at").alias("execution_timestamp"),
    )


def _union_metrics(frames: Sequence[DataFrame]) -> DataFrame:
    metrics_df = frames[0]
    for frame in frames[1:]:
        metrics_df = metrics_df.unionByName(frame)
    return metrics_df


def collect_pipeline_metrics(
    spark: SparkSession,
    config: PipelineConfig,
    bronze_customers: DataFrame,
    bronze_orders: DataFrame,
    bronze_products: DataFrame,
    customers_pre_ri: DataFrame,
    orders_pre_ri: DataFrame,
    orders_with_ri: DataFrame,
    products_pre_business: DataFrame,
    run_id: str,
    qc,
    qu,
    qtv,
    ri,
    qbl,
) -> DataFrame:
    """Aggregate dataset-level metrics from all validation steps."""
    reference_keys = ri.build_reference_key_map(spark, config, ri.ORDER_REFERENTIAL_RULES)

    metric_frames: List[DataFrame] = [
        qc.compute_completeness_metrics(
            bronze_customers,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id=run_id,
        ),
        qc.compute_completeness_metrics(
            bronze_orders,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id=run_id,
        ),
        qu.compute_uniqueness_metrics(
            customers_pre_ri,
            entity="customers",
            rules=qu.CUSTOMER_UNIQUENESS_RULES,
            run_id=run_id,
        ),
        qu.compute_uniqueness_metrics(
            orders_pre_ri,
            entity="orders",
            rules=qu.ORDER_UNIQUENESS_RULES,
            run_id=run_id,
        ),
        qtv.compute_type_validation_metrics(
            customers_pre_ri,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id=run_id,
        ),
        qtv.compute_type_validation_metrics(
            orders_pre_ri,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id=run_id,
        ),
        qtv.compute_type_validation_metrics(
            products_pre_business,
            entity="products",
            rules=qtv.PRODUCT_TYPE_RULES,
            run_id=run_id,
        ),
        ri.compute_referential_integrity_metrics(
            orders_pre_ri,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id=run_id,
        ),
        qbl.compute_business_logic_metrics(
            orders_with_ri,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id=run_id,
        ),
        qbl.compute_business_logic_metrics(
            products_pre_business,
            entity="products",
            rules=qbl.PRODUCT_BUSINESS_RULES,
            run_id=run_id,
        ),
    ]
    return _union_metrics(metric_frames)


def apply_quality_pipeline(
    spark: SparkSession,
    bronze_customers: DataFrame,
    bronze_orders: DataFrame,
    bronze_products: DataFrame,
    run_id: str,
    config: Optional[PipelineConfig] = None,
    on_step_complete: Optional[Callable[[str], None]] = None,
) -> Dict[str, DataFrame]:
    """
    Run all Silver quality steps in memory and return consolidated entity DataFrames.

    Returns dict with keys: customers, orders, products, and intermediate snapshots
    used for metrics collection.

    Parameters
    ----------
    on_step_complete:
        Optional callback invoked after each validation stage completes
        (completeness, uniqueness, type_validation, referential_integrity,
        business_validation, consolidate_quality_model).
    """
    def _step(step_name: str) -> None:
        logger.info("Silver quality step completed: %s", step_name)
        if on_step_complete is not None:
            on_step_complete(step_name)

    qc = _load_step_module("01_quality_completeness.py", "quality_completeness")
    qu = _load_step_module("02_quality_uniqueness.py", "quality_uniqueness")
    qtv = _load_step_module("03_quality_type_validation.py", "quality_type_validation")
    ri = _load_step_module("04_quality_referential_integrity.py", "quality_referential_integrity")
    qbl = _load_step_module("05_quality_business_logic.py", "quality_business_logic")

    customers = qc.apply_completeness_checks(
        bronze_customers,
        entity="customers",
        rules=qc.CUSTOMER_COMPLETENESS_RULES,
        run_id=run_id,
    )
    orders = qc.apply_completeness_checks(
        bronze_orders,
        entity="orders",
        rules=qc.ORDER_COMPLETENESS_RULES,
        run_id=run_id,
    )
    _step("completeness_checks")

    customers = qu.apply_uniqueness_checks(
        customers,
        entity="customers",
        rules=qu.CUSTOMER_UNIQUENESS_RULES,
        run_id=run_id,
    )
    orders = qu.apply_uniqueness_checks(
        orders,
        entity="orders",
        rules=qu.ORDER_UNIQUENESS_RULES,
        run_id=run_id,
    )
    _step("uniqueness_checks")

    customers_pre_ri = qtv.apply_type_validation_checks(
        customers,
        entity="customers",
        rules=qtv.CUSTOMER_TYPE_RULES,
        run_id=run_id,
    )
    orders_pre_ri = qtv.apply_type_validation_checks(
        orders,
        entity="orders",
        rules=qtv.ORDER_TYPE_RULES,
        run_id=run_id,
    )
    products_pre_business = qtv.apply_type_validation_checks(
        bronze_products,
        entity="products",
        rules=qtv.PRODUCT_TYPE_RULES,
        run_id=run_id,
    )
    _step("type_validation")

    config = config or load_config(run_id=run_id)
    reference_keys = ri.build_reference_key_map(spark, config, ri.ORDER_REFERENTIAL_RULES)
    orders_with_ri = ri.apply_referential_integrity_checks(
        orders_pre_ri,
        reference_keys=reference_keys,
        rules=ri.ORDER_REFERENTIAL_RULES,
        run_id=run_id,
    )
    _step("referential_integrity_checks")

    orders_final = qbl.apply_business_logic_checks(
        orders_with_ri,
        entity="orders",
        rules=qbl.ORDER_BUSINESS_RULES,
        run_id=run_id,
    )
    products_final = qbl.apply_business_logic_checks(
        products_pre_business,
        entity="products",
        rules=qbl.PRODUCT_BUSINESS_RULES,
        run_id=run_id,
    )
    _step("business_validation")

    customers_final = consolidate_quality_model(customers_pre_ri, "customers", run_id)
    orders_final = consolidate_quality_model(orders_final, "orders", run_id)
    products_final = consolidate_quality_model(products_final, "products", run_id)
    _step("consolidate_quality_model")

    return {
        "customers": finalize_silver_schema(customers_final, "customers"),
        "orders": finalize_silver_schema(orders_final, "orders"),
        "products": finalize_silver_schema(products_final, "products"),
        "_bronze_customers": bronze_customers,
        "_bronze_orders": bronze_orders,
        "_bronze_products": bronze_products,
        "_customers_pre_ri": customers_pre_ri,
        "_orders_pre_ri": orders_pre_ri,
        "_orders_with_ri": orders_with_ri,
        "_products_pre_business": products_pre_business,
        "_qc": qc,
        "_qu": qu,
        "_qtv": qtv,
        "_ri": ri,
        "_qbl": qbl,
    }


def _assert_row_count_unchanged(
    entity: str,
    input_count: int,
    output_df: DataFrame,
) -> None:
    output_count = output_df.count()
    if output_count != input_count:
        raise RuntimeError(
            f"Row count changed for {entity}: bronze={input_count} silver={output_count}. "
            "Silver creation must not delete rows.",
        )


def create_valid_record_views(
    spark: SparkSession,
    config: PipelineConfig,
) -> None:
    """Create convenience views exposing only valid Silver rows."""
    view_map = {
        f"{config.silver_customers_table}_valid": config.silver_customers_table,
        f"{config.silver_orders_table}_valid": config.silver_orders_table,
        f"{config.silver_products_table}_valid": config.silver_products_table,
    }
    for view_name, table_name in view_map.items():
        qualified_table = config.qualified_table_name(table_name)
        qualified_view = config.qualified_table_name(view_name)
        spark.sql(
            f"CREATE OR REPLACE VIEW {qualified_view} AS "
            f"SELECT * FROM {qualified_table} WHERE dq_is_valid = true",
        )
        logger.info("Created valid-record view %s", qualified_view)


def write_silver_layer(
    spark: SparkSession,
    config: PipelineConfig,
    pipeline_result: Dict[str, DataFrame],
    bronze_counts: Dict[str, int],
    run_id: str,
) -> Dict[str, DataFrame]:
    """
    Persist Silver entity tables, DQ metrics/report, and valid-record views.

    Assumes ``pipeline_result`` was produced by ``apply_quality_pipeline``.
    """
    write_mode = config.silver_write_mode

    silver_customers = pipeline_result["customers"]
    silver_orders = pipeline_result["orders"]
    silver_products = pipeline_result["products"]

    _assert_row_count_unchanged("customers", bronze_counts["customers"], silver_customers)
    _assert_row_count_unchanged("orders", bronze_counts["orders"], silver_orders)
    _assert_row_count_unchanged("products", bronze_counts["products"], silver_products)

    metrics_df = collect_pipeline_metrics(
        spark,
        config,
        pipeline_result["_bronze_customers"],
        pipeline_result["_bronze_orders"],
        pipeline_result["_bronze_products"],
        pipeline_result["_customers_pre_ri"],
        pipeline_result["_orders_pre_ri"],
        pipeline_result["_orders_with_ri"],
        pipeline_result["_products_pre_business"],
        run_id,
        pipeline_result["_qc"],
        pipeline_result["_qu"],
        pipeline_result["_qtv"],
        pipeline_result["_ri"],
        pipeline_result["_qbl"],
    )
    report_df = build_quality_report(metrics_df)

    write_delta_table(
        silver_customers,
        config.qualified_table_name(config.silver_customers_table),
        write_mode,
    )
    write_delta_table(
        silver_orders,
        config.qualified_table_name(config.silver_orders_table),
        write_mode,
    )
    write_delta_table(
        silver_products,
        config.qualified_table_name(config.silver_products_table),
        write_mode,
    )
    write_metrics_table(spark, metrics_df, config, write_mode="append")
    write_delta_table(
        report_df,
        config.qualified_table_name(config.silver_dq_report_table),
        write_mode,
    )

    create_valid_record_views(spark, config)

    valid_customers = silver_customers.filter(F.col("dq_is_valid")).count()
    valid_orders = silver_orders.filter(F.col("dq_is_valid")).count()
    valid_products = silver_products.filter(F.col("dq_is_valid")).count()
    logger.info(
        "Silver row status customers valid=%s invalid=%s orders valid=%s invalid=%s "
        "products valid=%s invalid=%s",
        valid_customers,
        bronze_counts["customers"] - valid_customers,
        valid_orders,
        bronze_counts["orders"] - valid_orders,
        valid_products,
        bronze_counts["products"] - valid_products,
    )

    return {
        "customers": silver_customers,
        "orders": silver_orders,
        "products": silver_products,
        "metrics": metrics_df,
        "report": report_df,
    }


def run_create_silver_tables(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
    on_quality_step_complete: Optional[Callable[[str], None]] = None,
) -> Dict[str, DataFrame]:
    """Build consolidated Silver tables, metrics, and reporting outputs."""
    spark = get_spark(spark)
    config = config or load_config()
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
        "Bronze input rows customers=%s orders=%s products=%s run_id=%s",
        bronze_counts["customers"],
        bronze_counts["orders"],
        bronze_counts["products"],
        run_id,
    )

    pipeline_result = apply_quality_pipeline(
        spark,
        bronze_customers,
        bronze_orders,
        bronze_products,
        run_id,
        config=config,
        on_step_complete=on_quality_step_complete,
    )

    return write_silver_layer(
        spark,
        config,
        pipeline_result,
        bronze_counts,
        run_id,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create consolidated Silver tables.")
    parser.add_argument("--catalog", help="Unity Catalog name (optional).")
    parser.add_argument("--schema", help="Database/schema name (default: ecommerce).")
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        help="Silver table write mode (default: overwrite).",
    )
    parser.add_argument("--run-id", help="Pipeline run identifier.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    config = load_config(
        catalog=args.catalog,
        schema_name=args.schema,
        silver_write_mode=args.write_mode,
        run_id=args.run_id,
    )

    try:
        run_create_silver_tables(config=config)
        logger.info("Silver table creation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver table creation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
