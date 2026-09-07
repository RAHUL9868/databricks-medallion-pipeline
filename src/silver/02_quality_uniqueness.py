#!/usr/bin/env python3
"""
Silver uniqueness validation: customers.customer_id, orders.order_id.

Duplicate flagging decision
---------------------------
When a business key appears more than once, **every row in that duplicate group is
flagged** (not only subsequent occurrences).

Rationale:
- Avoids picking an arbitrary "winner" row that appears valid while still being part
  of a duplicate-key violation.
- Aligns with `data-quality-strategy.md` uniqueness semantics and assignment metrics
  (e.g. 10 duplicated customer_id values -> 20 rows fail uniqueness).
- Makes duplicate counts auditable: failed_rows equals the number of rows participating
  in duplicate groups.

NULL or blank keys are **excluded** from uniqueness evaluation (they are handled by
completeness rules). Those rows receive `dq_uniqueness_pass = true`.

Determinism:
- Duplicate detection uses a Spark window `count(*) OVER (PARTITION BY key)` with an
  explicit branch for null/blank keys, so results do not depend on row order or
  driver-side collection.

Columns added/updated
---------------------
| Column                 | Description |
|------------------------|-------------|
| dq_uniqueness_pass     | False when row is in a duplicate key group |
| dq_uniqueness_errors   | Human-readable uniqueness failure messages |
| dq_failed_rules        | Existing failures plus uniqueness rule id when applicable |
| dq_failure_count       | Size of `dq_failed_rules` after merge |
| dq_is_valid            | `dq_completeness_pass AND dq_uniqueness_pass` (when completeness exists) |
| dq_run_id              | Updated run id |
| dq_checked_at          | Updated timestamp |
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from config.pipeline_config import PipelineConfig, load_config
from silver.silver_common import (
    CHECK_CATEGORY_UNIQUENESS,
    ensure_schema_exists,
    get_spark,
    is_null_or_blank,
    read_silver_table,
    write_delta_table,
    write_metrics_table,
)

logger = logging.getLogger(__name__)


class UniquenessRule:
    def __init__(self, rule_id: str, column: str, description: str) -> None:
        self.rule_id = rule_id
        self.column = column
        self.description = description


CUSTOMER_UNIQUENESS_RULES: Tuple[UniquenessRule, ...] = (
    UniquenessRule(
        rule_id="customers_customer_id_unique",
        column="customer_id",
        description="Customer customer_id must be unique across all customer rows",
    ),
)

ORDER_UNIQUENESS_RULES: Tuple[UniquenessRule, ...] = (
    UniquenessRule(
        rule_id="orders_order_id_unique",
        column="order_id",
        description="Order order_id must be unique across all order rows",
    ),
)

ENTITY_UNIQUENESS_RULES: Dict[str, Tuple[UniquenessRule, ...]] = {
    "customers": CUSTOMER_UNIQUENESS_RULES,
    "orders": ORDER_UNIQUENESS_RULES,
}


def duplicate_membership_condition(key_column: str) -> Column:
    """
    True when the row participates in a duplicate key group.

    Null/blank keys are not evaluated and never flagged as duplicates.
    """
    window = Window.partitionBy(key_column)
    key_count = F.when(
        is_null_or_blank(key_column),
        F.lit(1),
    ).otherwise(F.count(F.lit(1)).over(window))
    return (~is_null_or_blank(key_column)) & (key_count > F.lit(1))


def _ensure_completeness_columns(df: DataFrame) -> DataFrame:
    """Initialize completeness columns when uniqueness runs without a prior completeness step."""
    result = df
    if "dq_failed_rules" not in result.columns:
        result = result.withColumn("dq_failed_rules", F.array().cast("array<string>"))
    if "dq_completeness_errors" not in result.columns:
        result = result.withColumn("dq_completeness_errors", F.array().cast("array<string>"))
    if "dq_completeness_pass" not in result.columns:
        result = result.withColumn("dq_completeness_pass", F.lit(True))
    if "dq_uniqueness_errors" not in result.columns:
        result = result.withColumn("dq_uniqueness_errors", F.array().cast("array<string>"))
    return result


def apply_uniqueness_checks(
    df: DataFrame,
    entity: str,
    rules: Sequence[UniquenessRule],
    run_id: str,
) -> DataFrame:
    """
    Apply uniqueness rules without deleting rows.

    Merges uniqueness results into existing completeness quality columns when present.
    """
    if not rules:
        raise ValueError(f"No uniqueness rules configured for entity '{entity}'.")

    checked_at = datetime.now(timezone.utc)
    working = _ensure_completeness_columns(df)

    duplicate_conditions = [
        duplicate_membership_condition(rule.column) for rule in rules
    ]
    is_duplicate = duplicate_conditions[0]
    for condition in duplicate_conditions[1:]:
        is_duplicate = is_duplicate | condition

    failed_rule_candidates = F.array(
        *[
            F.when(duplicate_membership_condition(rule.column), F.lit(rule.rule_id))
            for rule in rules
        ],
    )
    error_candidates = F.array(
        *[
            F.when(
                duplicate_membership_condition(rule.column),
                F.lit(
                    f"{rule.rule_id}: duplicate value for column '{rule.column}'",
                ),
            )
            for rule in rules
        ],
    )

    new_uniqueness_errors = F.expr("filter(_dq_uniqueness_error_candidates, x -> x is not null)")
    new_uniqueness_failed_rules = F.expr(
        "filter(_dq_uniqueness_failed_candidates, x -> x is not null)",
    )

    merged_failed_rules = F.array_union(
        F.coalesce(F.col("dq_failed_rules"), F.array().cast("array<string>")),
        new_uniqueness_failed_rules,
    )

    result = (
        working.withColumn("_dq_uniqueness_failed_candidates", failed_rule_candidates)
        .withColumn("_dq_uniqueness_error_candidates", error_candidates)
        .withColumn("dq_uniqueness_errors", new_uniqueness_errors)
        .withColumn("dq_uniqueness_pass", ~is_duplicate)
        .withColumn("dq_failed_rules", merged_failed_rules)
        .withColumn("dq_failure_count", F.size(F.col("dq_failed_rules")))
        .withColumn(
            "dq_is_valid",
            F.col("dq_completeness_pass") & F.col("dq_uniqueness_pass"),
        )
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_checked_at", F.lit(checked_at))
        .drop("_dq_uniqueness_failed_candidates", "_dq_uniqueness_error_candidates")
    )

    logger.info(
        "Applied %s uniqueness rule(s) to entity=%s run_id=%s",
        len(rules),
        entity,
        run_id,
    )
    return result


def _with_duplicate_metric_columns(
    df: DataFrame,
    rules: Sequence[UniquenessRule],
) -> DataFrame:
    """Materialize per-rule duplicate flags before aggregation (Spark Connect safe)."""
    result = df
    for rule in rules:
        result = result.withColumn(
            f"_metric_dup_{rule.rule_id}",
            F.when(duplicate_membership_condition(rule.column), F.lit(1)).otherwise(F.lit(0)),
        )
    return result


def compute_uniqueness_metrics(
    df: DataFrame,
    entity: str,
    rules: Sequence[UniquenessRule],
    run_id: str,
) -> DataFrame:
    """Compute dataset-level uniqueness metrics (counts, duplicate rows, pass/fail %)."""
    if not rules:
        raise ValueError(f"No uniqueness rules configured for entity '{entity}'.")

    evaluated_at = datetime.now(timezone.utc)
    flagged = _with_duplicate_metric_columns(df, rules)
    agg_exprs = [F.count(F.lit(1)).alias("total_rows")]
    for rule in rules:
        agg_exprs.append(
            F.sum(F.col(f"_metric_dup_{rule.rule_id}")).alias(rule.rule_id),
        )

    totals = flagged.agg(*agg_exprs)
    total_rows = F.col("total_rows")

    metric_frames: List[DataFrame] = []
    for rule in rules:
        failed_rows = F.col(rule.rule_id)
        passed_rows = total_rows - failed_rows
        metric_frames.append(
            totals.select(
                F.lit(run_id).alias("run_id"),
                F.lit(entity).alias("entity"),
                F.lit(CHECK_CATEGORY_UNIQUENESS).alias("check_category"),
                F.lit(rule.rule_id).alias("rule_id"),
                F.lit(rule.description).alias("rule_description"),
                total_rows.alias("total_rows"),
                failed_rows.alias("failed_rows"),
                passed_rows.alias("passed_rows"),
                F.round((passed_rows / total_rows) * F.lit(100), 2).alias("pass_pct"),
                F.round((failed_rows / total_rows) * F.lit(100), 2).alias("fail_pct"),
                F.lit(evaluated_at).alias("evaluated_at"),
            ),
        )

    metrics_df = metric_frames[0]
    for frame in metric_frames[1:]:
        metrics_df = metrics_df.unionByName(frame)

    return metrics_df


def count_duplicate_rows(df: DataFrame, rule_id: str) -> int:
    """Count rows flagged for a uniqueness rule (for tests)."""
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def count_distinct_duplicate_keys(df: DataFrame, key_column: str) -> int:
    """Count distinct key values that appear more than once among non-null keys."""
    return (
        df.filter(~is_null_or_blank(key_column))
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def run_uniqueness_validation(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, DataFrame]:
    """Read Silver tables, apply uniqueness checks, persist updates and metrics."""
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()
    write_mode = config.silver_write_mode

    ensure_schema_exists(spark, config)

    silver_customers = read_silver_table(spark, config, config.silver_customers_table)
    silver_orders = read_silver_table(spark, config, config.silver_orders_table)

    input_customer_rows = silver_customers.count()
    input_order_rows = silver_orders.count()
    logger.info(
        "Silver input rows customers=%s orders=%s run_id=%s",
        input_customer_rows,
        input_order_rows,
        run_id,
    )

    updated_customers = apply_uniqueness_checks(
        silver_customers,
        entity="customers",
        rules=CUSTOMER_UNIQUENESS_RULES,
        run_id=run_id,
    )
    updated_orders = apply_uniqueness_checks(
        silver_orders,
        entity="orders",
        rules=ORDER_UNIQUENESS_RULES,
        run_id=run_id,
    )

    output_customer_rows = updated_customers.count()
    output_order_rows = updated_orders.count()
    if output_customer_rows != input_customer_rows:
        raise RuntimeError(
            f"Row count changed for customers: input={input_customer_rows} "
            f"output={output_customer_rows}. Uniqueness validation must not delete rows.",
        )
    if output_order_rows != input_order_rows:
        raise RuntimeError(
            f"Row count changed for orders: input={input_order_rows} "
            f"output={output_order_rows}. Uniqueness validation must not delete rows.",
        )

    customer_metrics = compute_uniqueness_metrics(
        silver_customers,
        entity="customers",
        rules=CUSTOMER_UNIQUENESS_RULES,
        run_id=run_id,
    )
    order_metrics = compute_uniqueness_metrics(
        silver_orders,
        entity="orders",
        rules=ORDER_UNIQUENESS_RULES,
        run_id=run_id,
    )
    metrics_df = customer_metrics.unionByName(order_metrics)

    customers_table = config.qualified_table_name(config.silver_customers_table)
    orders_table = config.qualified_table_name(config.silver_orders_table)

    write_delta_table(updated_customers, customers_table, write_mode)
    write_delta_table(updated_orders, orders_table, write_mode)
    write_metrics_table(spark, metrics_df, config, write_mode="append")

    for row in metrics_df.collect():
        logger.info(
            "Uniqueness metric entity=%s rule=%s duplicate_rows=%s pass_pct=%s fail_pct=%s",
            row["entity"],
            row["rule_id"],
            row["failed_rows"],
            row["pass_pct"],
            row["fail_pct"],
        )

    return {
        "customers": updated_customers,
        "orders": updated_orders,
        "metrics": metrics_df,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver uniqueness validation.")
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
        run_uniqueness_validation(config=config)
        logger.info("Silver uniqueness validation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver uniqueness validation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
