#!/usr/bin/env python3
"""
Silver business-logic validation for cross-column and domain-relationship rules.

Rows are never modified or deleted; failures are appended to ``dq_failed_rules``.

Rule ownership (assignment vs engineering)
------------------------------------------
**Enforced in ``03_quality_type_validation.py`` (assignment, single-column type/domain):**

| User requirement | Rule ID | Module |
|------------------|---------|--------|
| quantity must be positive | ``orders_quantity_positive`` | 03 |
| unit_price must be non-negative | ``orders_unit_price_non_negative`` | 03 |
| order_status valid domain | ``orders_status_valid`` | 03 |
| signup_date not in the future | ``customers_signup_date_valid`` | 03 |

This module does **not** re-apply those rules to avoid duplicate flags.

**Enforced here (assignment, ``data-quality-strategy.md`` CHECK-B01–B03):**

| Rule ID | Rationale | Failure condition |
|---------|-----------|-------------------|
| ``orders_total_amount_matches`` | Revenue reporting requires line totals to match quantity × unit price | All three fields parse as valid numbers AND ``ABS(total_amount - quantity * unit_price) >= 0.01`` |
| ``orders_completed_has_payment_date`` | Completed orders represent fulfilled sales; payment should be recorded | ``order_status = 'Completed'`` AND ``payment_date`` is null/blank |
| ``orders_payment_date_after_order_date`` | Payment cannot occur before the order was placed | Both dates parse AND ``payment_date < order_date`` |

**Enforced here (engineering assumption, ``data-model.md`` optional rule):**

| Rule ID | Rationale | Failure condition |
|---------|-----------|-------------------|
| ``products_cost_not_exceeds_price`` | Negative margin (cost > price) is unexpected for standard catalog pricing | Both fields parse as non-negative decimals AND ``cost > price`` |

Columns added/updated
---------------------
| Column                   | Description |
|--------------------------|-------------|
| dq_business_logic_pass   | False when any business-logic rule failed on the row |
| dq_business_logic_errors | Human-readable business-logic failure messages |
| dq_type_business_pass    | Updated: false when prior type failures OR new business failures |
| dq_failed_rules          | Existing failures plus business rule ids when applicable |
| dq_failure_count         | Size of ``dq_failed_rules`` |
| dq_is_valid              | All category pass flags including business logic |
| dq_run_id                | Updated run id |
| dq_checked_at            | Updated timestamp |
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from config.pipeline_config import PipelineConfig, load_config
from silver.silver_common import (
    CHECK_CATEGORY_TYPE_BUSINESS,
    ensure_schema_exists,
    get_spark,
    is_null_or_blank,
    read_silver_table,
    write_delta_table,
    write_metrics_table,
)

logger = logging.getLogger(__name__)

DATE_FORMAT = "yyyy-MM-dd"
AMOUNT_MATCH_TOLERANCE = 0.01
COMPLETED_STATUS = "Completed"


class RuleScope(str, Enum):
    """Distinguishes assignment-required rules from engineering assumptions."""

    ASSIGNMENT_REQUIRED = "assignment_required"
    ENGINEERING_ASSUMPTION = "engineering_assumption"


class BusinessLogicRule:
    def __init__(
        self,
        rule_id: str,
        entity: str,
        description: str,
        rationale: str,
        failure_condition: str,
        scope: RuleScope,
        failure_predicate: Callable[[], Column],
    ) -> None:
        self.rule_id = rule_id
        self.entity = entity
        self.description = description
        self.rationale = rationale
        self.failure_condition = failure_condition
        self.scope = scope
        self.failure_predicate = failure_predicate


def _trimmed(column_name: str) -> Column:
    return F.trim(F.col(column_name))


def _parsed_date(column_name: str) -> Column:
    return F.to_date(_trimmed(column_name), DATE_FORMAT)


def _parses_positive_integer(column_name: str) -> Column:
    trimmed = _trimmed(column_name)
    parsed = trimmed.cast("int")
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^[0-9]+$")
        & parsed.isNotNull()
        & (parsed > F.lit(0))
    )


def _parses_non_negative_decimal(column_name: str) -> Column:
    trimmed = _trimmed(column_name)
    parsed = trimmed.cast("decimal(18,2)")
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^\d+(\.\d+)?$")
        & parsed.isNotNull()
        & (parsed >= F.lit(0))
    )


def _total_amount_mismatch_condition() -> Column:
    """
    Fail when arithmetic total diverges from quantity × unit_price beyond tolerance.

    Skipped when operands do not parse (type module owns parse failures).
    """
    quantity = _trimmed("quantity").cast("int")
    unit_price = _trimmed("unit_price").cast("decimal(18,2)")
    total_amount = _trimmed("total_amount").cast("decimal(18,2)")
    operands_valid = (
        _parses_positive_integer("quantity")
        & _parses_non_negative_decimal("unit_price")
        & _parses_non_negative_decimal("total_amount")
    )
    expected_total = (quantity * unit_price).cast("decimal(18,2)")
    return operands_valid & (
        F.abs(total_amount - expected_total) >= F.lit(AMOUNT_MATCH_TOLERANCE)
    )


def _completed_missing_payment_date_condition() -> Column:
    """Fail when a completed order has no payment date."""
    return (_trimmed("order_status") == F.lit(COMPLETED_STATUS)) & is_null_or_blank(
        "payment_date",
    )


def _payment_before_order_date_condition() -> Column:
    """Fail when payment date precedes order date."""
    order_date = _parsed_date("order_date")
    payment_date = _parsed_date("payment_date")
    both_valid = order_date.isNotNull() & payment_date.isNotNull()
    return both_valid & (payment_date < order_date)


def _cost_exceeds_price_condition() -> Column:
    """Fail when product cost exceeds list price (negative margin)."""
    price = _trimmed("price").cast("decimal(18,2)")
    cost = _trimmed("cost").cast("decimal(18,2)")
    both_valid = _parses_non_negative_decimal("price") & _parses_non_negative_decimal("cost")
    return both_valid & (cost > price)


ORDER_BUSINESS_RULES: Tuple[BusinessLogicRule, ...] = (
    BusinessLogicRule(
        rule_id="orders_total_amount_matches",
        entity="orders",
        description="Order total_amount must equal quantity × unit_price within $0.01",
        rationale=(
            "Line totals must be arithmetically consistent for accurate revenue "
            "and margin reporting (CHECK-B01)."
        ),
        failure_condition=(
            "quantity, unit_price, and total_amount all parse as valid numbers AND "
            f"ABS(total_amount - quantity * unit_price) >= {AMOUNT_MATCH_TOLERANCE}"
        ),
        scope=RuleScope.ASSIGNMENT_REQUIRED,
        failure_predicate=_total_amount_mismatch_condition,
    ),
    BusinessLogicRule(
        rule_id="orders_completed_has_payment_date",
        entity="orders",
        description="Completed orders must have a payment_date",
        rationale=(
            "Completed orders represent fulfilled sales; cash-flow reporting expects "
            "a recorded payment date (CHECK-B02)."
        ),
        failure_condition="order_status = 'Completed' AND payment_date is null or blank",
        scope=RuleScope.ASSIGNMENT_REQUIRED,
        failure_predicate=_completed_missing_payment_date_condition,
    ),
    BusinessLogicRule(
        rule_id="orders_payment_date_after_order_date",
        entity="orders",
        description="payment_date must be on or after order_date when both are present",
        rationale=(
            "Payment cannot logically occur before the order was placed (CHECK-B03)."
        ),
        failure_condition=(
            "order_date and payment_date both parse as dates AND payment_date < order_date"
        ),
        scope=RuleScope.ASSIGNMENT_REQUIRED,
        failure_predicate=_payment_before_order_date_condition,
    ),
)

PRODUCT_BUSINESS_RULES: Tuple[BusinessLogicRule, ...] = (
    BusinessLogicRule(
        rule_id="products_cost_not_exceeds_price",
        entity="products",
        description="Product cost should not exceed list price",
        rationale=(
            "Standard catalog items are expected to have cost <= price for positive "
            "margin. Documented as an optional rule in data-model.md; flagged here "
            "as an engineering assumption, not an assignment acceptance criterion."
        ),
        failure_condition=(
            "price and cost both parse as non-negative decimals AND cost > price"
        ),
        scope=RuleScope.ENGINEERING_ASSUMPTION,
        failure_predicate=_cost_exceeds_price_condition,
    ),
)

ENTITY_BUSINESS_RULES: Dict[str, Tuple[BusinessLogicRule, ...]] = {
    "orders": ORDER_BUSINESS_RULES,
    "products": PRODUCT_BUSINESS_RULES,
}

TYPE_RULES_OWNED_BY_MODULE_03: Tuple[str, ...] = (
    "orders_quantity_positive",
    "orders_unit_price_non_negative",
    "orders_status_valid",
    "customers_signup_date_valid",
)


def rule_failure_condition(rule: BusinessLogicRule) -> Column:
    return rule.failure_predicate()


def rule_failure_message(rule: BusinessLogicRule) -> str:
    scope_label = rule.scope.value
    return (
        f"{rule.rule_id} [{scope_label}]: {rule.failure_condition}"
    )


def _ensure_prior_quality_columns(df: DataFrame) -> DataFrame:
    """Initialize upstream quality columns when earlier modules were not run."""
    result = df
    defaults = {
        "dq_failed_rules": F.array().cast("array<string>"),
        "dq_completeness_errors": F.array().cast("array<string>"),
        "dq_uniqueness_errors": F.array().cast("array<string>"),
        "dq_type_business_errors": F.array().cast("array<string>"),
        "dq_referential_errors": F.array().cast("array<string>"),
        "dq_business_logic_errors": F.array().cast("array<string>"),
        "dq_completeness_pass": F.lit(True),
        "dq_uniqueness_pass": F.lit(True),
        "dq_type_business_pass": F.lit(True),
        "dq_referential_pass": F.lit(True),
    }
    for column_name, default_value in defaults.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, default_value)
    return result


def apply_business_logic_checks(
    df: DataFrame,
    entity: str,
    rules: Sequence[BusinessLogicRule],
    run_id: str,
) -> DataFrame:
    """Apply business-logic rules without modifying source values or deleting rows."""
    if not rules:
        return _ensure_prior_quality_columns(df)

    checked_at = datetime.now(timezone.utc)
    working = _ensure_prior_quality_columns(df)

    failure_conditions = [rule_failure_condition(rule) for rule in rules]
    any_business_failure = failure_conditions[0]
    for condition in failure_conditions[1:]:
        any_business_failure = any_business_failure | condition

    failed_rule_candidates = F.array(
        *[
            F.when(rule_failure_condition(rule), F.lit(rule.rule_id))
            for rule in rules
        ],
    )
    error_candidates = F.array(
        *[
            F.when(rule_failure_condition(rule), F.lit(rule_failure_message(rule)))
            for rule in rules
        ],
    )

    new_business_failed_rules = F.expr(
        "filter(_dq_business_failed_candidates, x -> x is not null)",
    )
    new_business_errors = F.expr(
        "filter(_dq_business_error_candidates, x -> x is not null)",
    )

    merged_failed_rules = F.array_union(
        F.coalesce(F.col("dq_failed_rules"), F.array().cast("array<string>")),
        new_business_failed_rules,
    )

    result = (
        working.withColumn("_dq_business_failed_candidates", failed_rule_candidates)
        .withColumn("_dq_business_error_candidates", error_candidates)
        .withColumn("dq_business_logic_errors", new_business_errors)
        .withColumn("dq_business_logic_pass", ~any_business_failure)
        .withColumn(
            "dq_type_business_pass",
            F.col("dq_type_business_pass") & (~any_business_failure),
        )
        .withColumn("dq_failed_rules", merged_failed_rules)
        .withColumn("dq_failure_count", F.size(F.col("dq_failed_rules")))
        .withColumn(
            "dq_is_valid",
            F.col("dq_completeness_pass")
            & F.col("dq_uniqueness_pass")
            & F.col("dq_type_business_pass")
            & F.col("dq_referential_pass"),
        )
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_checked_at", F.lit(checked_at))
        .drop("_dq_business_failed_candidates", "_dq_business_error_candidates")
    )

    logger.info(
        "Applied %s business-logic rule(s) to entity=%s run_id=%s",
        len(rules),
        entity,
        run_id,
    )
    return result


def compute_business_logic_metrics(
    df: DataFrame,
    entity: str,
    rules: Sequence[BusinessLogicRule],
    run_id: str,
) -> DataFrame:
    """Compute dataset-level business-logic metrics per rule."""
    if not rules:
        raise ValueError(f"No business-logic rules configured for entity '{entity}'.")

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
        scoped_description = f"[{rule.scope.value}] {rule.description}"
        metric_frames.append(
            totals.select(
                F.lit(run_id).alias("run_id"),
                F.lit(entity).alias("entity"),
                F.lit(CHECK_CATEGORY_TYPE_BUSINESS).alias("check_category"),
                F.lit(rule.rule_id).alias("rule_id"),
                F.lit(scoped_description).alias("rule_description"),
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
    """Count rows where a specific business-logic rule failed (for tests)."""
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def run_business_logic_validation(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, DataFrame]:
    """Read Silver tables, apply business-logic checks, persist results."""
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()
    write_mode = config.silver_write_mode

    ensure_schema_exists(spark, config)

    silver_orders = read_silver_table(spark, config, config.silver_orders_table)
    silver_products = read_silver_table(spark, config, config.silver_products_table)

    input_order_rows = silver_orders.count()
    input_product_rows = silver_products.count()
    logger.info(
        "Business-logic input rows orders=%s products=%s run_id=%s",
        input_order_rows,
        input_product_rows,
        run_id,
    )

    updated_orders = apply_business_logic_checks(
        silver_orders,
        entity="orders",
        rules=ORDER_BUSINESS_RULES,
        run_id=run_id,
    )
    updated_products = apply_business_logic_checks(
        silver_products,
        entity="products",
        rules=PRODUCT_BUSINESS_RULES,
        run_id=run_id,
    )

    output_order_rows = updated_orders.count()
    output_product_rows = updated_products.count()
    if output_order_rows != input_order_rows:
        raise RuntimeError(
            f"Row count changed for orders: input={input_order_rows} "
            f"output={output_order_rows}. Business-logic validation must not delete rows.",
        )
    if output_product_rows != input_product_rows:
        raise RuntimeError(
            f"Row count changed for products: input={input_product_rows} "
            f"output={output_product_rows}. Business-logic validation must not delete rows.",
        )

    order_metrics = compute_business_logic_metrics(
        silver_orders,
        entity="orders",
        rules=ORDER_BUSINESS_RULES,
        run_id=run_id,
    )
    product_metrics = compute_business_logic_metrics(
        silver_products,
        entity="products",
        rules=PRODUCT_BUSINESS_RULES,
        run_id=run_id,
    )
    metrics_df = order_metrics.unionByName(product_metrics)

    write_delta_table(
        updated_orders,
        config.qualified_table_name(config.silver_orders_table),
        write_mode,
    )
    write_delta_table(
        updated_products,
        config.qualified_table_name(config.silver_products_table),
        write_mode,
    )
    write_metrics_table(spark, metrics_df, config, write_mode="append")

    for row in metrics_df.collect():
        logger.info(
            "Business-logic metric entity=%s rule=%s failed=%s pass_pct=%s fail_pct=%s",
            row["entity"],
            row["rule_id"],
            row["failed_rows"],
            row["pass_pct"],
            row["fail_pct"],
        )

    return {
        "orders": updated_orders,
        "products": updated_products,
        "metrics": metrics_df,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver business-logic validation.")
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
        run_business_logic_validation(config=config)
        logger.info("Silver business-logic validation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver business-logic validation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
