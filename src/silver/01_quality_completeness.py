#!/usr/bin/env python3
"""
Silver completeness validation: customers.email, orders.customer_id, orders.product_id.

Quality column design (completeness phase)
------------------------------------------
This module adds the following columns to entity DataFrames. All rows are retained.

| Column                  | Type            | Description |
|-------------------------|-----------------|-------------|
| dq_completeness_pass    | boolean         | False if any completeness rule failed for the row |
| dq_completeness_errors  | array<string>   | Human-readable failure messages per failed rule |
| dq_failed_rules         | array<string>   | Stable rule IDs that failed (completeness only in this module) |
| dq_failure_count        | int             | Number of failed completeness rules on the row |
| dq_is_valid             | boolean         | Overall validity (completeness-only until later modules run) |
| dq_run_id               | string          | Pipeline run identifier |
| dq_checked_at           | timestamp       | UTC timestamp when completeness checks were applied |

Dataset-level metrics are written to `silver_dq_metrics` with:
run_id, entity, check_category='completeness', rule_id, counts, pass_pct, fail_pct.

NULL handling: NULL, empty string, and whitespace-only Bronze STRING values are treated
as missing for required fields.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from config.pipeline_config import PipelineConfig, load_config
from silver.silver_common import (
    CHECK_CATEGORY_COMPLETENESS,
    ensure_schema_exists,
    get_spark,
    is_null_or_blank,
    read_bronze_table,
    write_delta_table,
    write_metrics_table,
)

logger = logging.getLogger(__name__)


class CompletenessRule:
    def __init__(self, rule_id: str, column: str, description: str) -> None:
        self.rule_id = rule_id
        self.column = column
        self.description = description


CUSTOMER_COMPLETENESS_RULES: Tuple[CompletenessRule, ...] = (
    CompletenessRule(
        rule_id="customers_email_not_null",
        column="email",
        description="Customer email must be present and non-blank",
    ),
)

ORDER_COMPLETENESS_RULES: Tuple[CompletenessRule, ...] = (
    CompletenessRule(
        rule_id="orders_customer_id_not_null",
        column="customer_id",
        description="Order customer_id must be present and non-blank",
    ),
    CompletenessRule(
        rule_id="orders_product_id_not_null",
        column="product_id",
        description="Order product_id must be present and non-blank",
    ),
)

ENTITY_COMPLETENESS_RULES: Dict[str, Tuple[CompletenessRule, ...]] = {
    "customers": CUSTOMER_COMPLETENESS_RULES,
    "orders": ORDER_COMPLETENESS_RULES,
}


def rule_failure_condition(rule: CompletenessRule) -> Column:
    return is_null_or_blank(rule.column)


def apply_completeness_checks(
    df: DataFrame,
    entity: str,
    rules: Sequence[CompletenessRule],
    run_id: str,
) -> DataFrame:
    """
    Apply completeness rules to a DataFrame without removing rows.

    Returns the input columns plus quality status columns.
    """
    if not rules:
        raise ValueError(f"No completeness rules configured for entity '{entity}'.")

    checked_at = datetime.now(timezone.utc)

    failed_candidates = F.array(
        *[
            F.when(rule_failure_condition(rule), F.lit(rule.rule_id))
            for rule in rules
        ],
    )
    error_candidates = F.array(
        *[
            F.when(
                rule_failure_condition(rule),
                F.lit(
                    f"{rule.rule_id}: column '{rule.column}' is null or blank",
                ),
            )
            for rule in rules
        ],
    )

    result = (
        df.withColumn("_dq_failed_candidates", failed_candidates)
        .withColumn("_dq_error_candidates", error_candidates)
        .withColumn(
            "dq_failed_rules",
            F.expr("filter(_dq_failed_candidates, x -> x is not null)"),
        )
        .withColumn(
            "dq_completeness_errors",
            F.expr("filter(_dq_error_candidates, x -> x is not null)"),
        )
        .withColumn("dq_failure_count", F.size(F.col("dq_failed_rules")))
        .withColumn("dq_completeness_pass", F.col("dq_failure_count") == F.lit(0))
        .withColumn("dq_is_valid", F.col("dq_completeness_pass"))
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_checked_at", F.lit(checked_at))
        .drop("_dq_failed_candidates", "_dq_error_candidates")
    )

    logger.info(
        "Applied %s completeness rule(s) to entity=%s run_id=%s",
        len(rules),
        entity,
        run_id,
    )
    return result


def compute_completeness_metrics(
    df: DataFrame,
    entity: str,
    rules: Sequence[CompletenessRule],
    run_id: str,
) -> DataFrame:
    """
    Compute dataset-level pass/fail counts and percentages per rule.

    Uses a single aggregate per entity (one small result row set; no large collects).
    """
    if not rules:
        raise ValueError(f"No completeness rules configured for entity '{entity}'.")

    evaluated_at = datetime.now(timezone.utc)
    agg_exprs = [F.count(F.lit(1)).alias("total_rows")]
    for rule in rules:
        agg_exprs.append(
            F.sum(
                F.when(rule_failure_condition(rule), F.lit(1)).otherwise(F.lit(0)),
            ).alias(rule.rule_id),
        )

    totals = df.agg(*agg_exprs)
    total_rows = F.col("total_rows")

    metric_frames: List[DataFrame] = []
    for rule in rules:
        failed_rows = F.col(rule.rule_id)
        passed_rows = total_rows - failed_rows
        metric_frames.append(
            totals.select(
                F.lit(run_id).alias("run_id"),
                F.lit(entity).alias("entity"),
                F.lit(CHECK_CATEGORY_COMPLETENESS).alias("check_category"),
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


def count_rule_failures(df: DataFrame, rule_id: str) -> int:
    """Count rows where a specific completeness rule failed (for tests)."""
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def run_completeness_validation(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, DataFrame]:
    """
    Read Bronze customers/orders, apply completeness checks, persist Silver tables and metrics.

    Returns a dict with keys: customers, orders, metrics.
    """
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()
    write_mode = config.silver_write_mode

    ensure_schema_exists(spark, config)

    bronze_customers = read_bronze_table(spark, config, config.bronze_customers_table)
    bronze_orders = read_bronze_table(spark, config, config.bronze_orders_table)

    input_customer_rows = bronze_customers.count()
    input_order_rows = bronze_orders.count()
    logger.info(
        "Bronze input rows customers=%s orders=%s run_id=%s",
        input_customer_rows,
        input_order_rows,
        run_id,
    )

    silver_customers = apply_completeness_checks(
        bronze_customers,
        entity="customers",
        rules=CUSTOMER_COMPLETENESS_RULES,
        run_id=run_id,
    )
    silver_orders = apply_completeness_checks(
        bronze_orders,
        entity="orders",
        rules=ORDER_COMPLETENESS_RULES,
        run_id=run_id,
    )

    output_customer_rows = silver_customers.count()
    output_order_rows = silver_orders.count()
    if output_customer_rows != input_customer_rows:
        raise RuntimeError(
            f"Row count changed for customers: input={input_customer_rows} "
            f"output={output_customer_rows}. Completeness validation must not delete rows.",
        )
    if output_order_rows != input_order_rows:
        raise RuntimeError(
            f"Row count changed for orders: input={input_order_rows} "
            f"output={output_order_rows}. Completeness validation must not delete rows.",
        )

    customer_metrics = compute_completeness_metrics(
        bronze_customers,
        entity="customers",
        rules=CUSTOMER_COMPLETENESS_RULES,
        run_id=run_id,
    )
    order_metrics = compute_completeness_metrics(
        bronze_orders,
        entity="orders",
        rules=ORDER_COMPLETENESS_RULES,
        run_id=run_id,
    )
    metrics_df = customer_metrics.unionByName(order_metrics)

    customers_table = config.qualified_table_name(config.silver_customers_table)
    orders_table = config.qualified_table_name(config.silver_orders_table)

    write_delta_table(silver_customers, customers_table, write_mode)
    write_delta_table(silver_orders, orders_table, write_mode)
    write_metrics_table(spark, metrics_df, config, write_mode="append")

    for row in metrics_df.collect():
        logger.info(
            "Completeness metric entity=%s rule=%s failed=%s pass_pct=%s fail_pct=%s",
            row["entity"],
            row["rule_id"],
            row["failed_rows"],
            row["pass_pct"],
            row["fail_pct"],
        )

    return {
        "customers": silver_customers,
        "orders": silver_orders,
        "metrics": metrics_df,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver completeness validation.")
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
        run_completeness_validation(config=config)
        logger.info("Silver completeness validation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver completeness validation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
