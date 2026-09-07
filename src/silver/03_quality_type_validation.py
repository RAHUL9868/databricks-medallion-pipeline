#!/usr/bin/env python3
"""
Silver type and domain validation for customers, orders, and products.

Validates parseable types, allowed enum values, and documented numeric constraints.
Rows are never deleted; failures are appended to ``dq_failed_rules``.

Assumptions (documented)
------------------------
- Bronze business columns are STRING; validation evaluates the raw string values.
- Dates use ISO format ``yyyy-MM-dd`` (matches sample data generation).
- ``signup_date`` and ``order_date`` must parse and must not be after ``current_date()``
  (per ``data-quality-strategy.md`` CHECK-T02 / CHECK-T08).
- ``payment_date`` is optional; when present it must parse as ``yyyy-MM-dd``. Future
  payment dates, payment-before-order, and Completed-without-payment are deferred to
  ``05_quality_business_logic.py``.
- Identifier columns (``customer_id``, ``order_id``, ``product_id``) are positive
  integers with no sign or decimal component.
- Enum matching is case-sensitive: ``Premium`` / ``Standard`` / ``Basic`` and
  ``Pending`` / ``Completed`` / ``Cancelled``.
- NULL/blank foreign keys on orders (``customer_id``, ``product_id``) are skipped for
  type validation (completeness module owns those failures).
- Monetary/numeric columns use ``decimal(18,2)`` or ``int`` casts after trim.

Columns added/updated
---------------------
| Column                  | Description |
|-------------------------|-------------|
| dq_type_business_pass   | False when any type/domain rule failed on the row |
| dq_type_business_errors | Human-readable type/domain failure messages |
| dq_failed_rules         | Existing failures plus type rule ids when applicable |
| dq_failure_count        | Size of ``dq_failed_rules`` |
| dq_is_valid             | completeness AND uniqueness AND type_business pass flags |
| dq_run_id               | Updated run id |
| dq_checked_at           | Updated timestamp |
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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
    read_parent_table,
    read_silver_table,
    write_delta_table,
    write_metrics_table,
)

logger = logging.getLogger(__name__)

DATE_FORMAT = "yyyy-MM-dd"
CUSTOMER_SEGMENTS: Tuple[str, ...] = ("Premium", "Standard", "Basic")
ORDER_STATUSES: Tuple[str, ...] = ("Pending", "Completed", "Cancelled")


class ValidationKind(str, Enum):
    REQUIRED_POSITIVE_INTEGER = "required_positive_integer"
    OPTIONAL_POSITIVE_INTEGER = "optional_positive_integer"
    REQUIRED_DATE_NOT_FUTURE = "required_date_not_future"
    OPTIONAL_DATE = "optional_date"
    REQUIRED_ENUM = "required_enum"
    REQUIRED_NON_NEGATIVE_DECIMAL = "required_non_negative_decimal"
    REQUIRED_POSITIVE_INTEGER_STRICT = "required_positive_integer_strict"
    REQUIRED_NON_NEGATIVE_INTEGER = "required_non_negative_integer"


@dataclass(frozen=True)
class TypeValidationRule:
    rule_id: str
    column: str
    description: str
    kind: ValidationKind
    allowed_values: Tuple[str, ...] = field(default_factory=tuple)


def _trimmed(column_name: str) -> Column:
    return F.trim(F.col(column_name))


def _parsed_date(column_name: str) -> Column:
    return F.to_date(_trimmed(column_name), DATE_FORMAT)


def passes_positive_integer(column_name: str) -> Column:
    """True when value is a positive integer string."""
    trimmed = _trimmed(column_name)
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^[0-9]+$")
        & (trimmed.cast("long") > F.lit(0))
    )


def passes_non_negative_decimal(column_name: str) -> Column:
    """True when value parses to a non-negative decimal."""
    trimmed = _trimmed(column_name)
    parsed = trimmed.cast("decimal(18,2)")
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^\d+(\.\d+)?$")
        & parsed.isNotNull()
        & (parsed >= F.lit(0))
    )


def passes_positive_integer_strict(column_name: str) -> Column:
    """True when value parses to an integer strictly greater than zero."""
    trimmed = _trimmed(column_name)
    parsed = trimmed.cast("int")
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^[0-9]+$")
        & parsed.isNotNull()
        & (parsed > F.lit(0))
    )


def passes_non_negative_integer(column_name: str) -> Column:
    """True when value parses to a non-negative integer."""
    trimmed = _trimmed(column_name)
    parsed = trimmed.cast("int")
    return (
        (~is_null_or_blank(column_name))
        & trimmed.rlike(r"^[0-9]+$")
        & parsed.isNotNull()
        & (parsed >= F.lit(0))
    )


def passes_required_date_not_future(column_name: str) -> Column:
    """True when value parses as a date on or before today."""
    parsed = _parsed_date(column_name)
    return (
        (~is_null_or_blank(column_name))
        & parsed.isNotNull()
        & (parsed <= F.current_date())
    )


def passes_optional_date(column_name: str) -> Column:
    """True when blank/null or parseable as a date."""
    parsed = _parsed_date(column_name)
    return is_null_or_blank(column_name) | parsed.isNotNull()


def passes_required_enum(column_name: str, allowed_values: Sequence[str]) -> Column:
    """True when value is present and in the allowed set."""
    return (~is_null_or_blank(column_name)) & _trimmed(column_name).isin(list(allowed_values))


def rule_failure_condition(rule: TypeValidationRule) -> Column:
    """Return a Column that is True when the rule fails."""
    if rule.kind == ValidationKind.REQUIRED_POSITIVE_INTEGER:
        return ~passes_positive_integer(rule.column)

    if rule.kind == ValidationKind.OPTIONAL_POSITIVE_INTEGER:
        return (~is_null_or_blank(rule.column)) & (~passes_positive_integer(rule.column))

    if rule.kind == ValidationKind.REQUIRED_DATE_NOT_FUTURE:
        return ~passes_required_date_not_future(rule.column)

    if rule.kind == ValidationKind.OPTIONAL_DATE:
        return ~passes_optional_date(rule.column)

    if rule.kind == ValidationKind.REQUIRED_ENUM:
        if not rule.allowed_values:
            raise ValueError(f"Rule {rule.rule_id} requires allowed_values for REQUIRED_ENUM.")
        return ~passes_required_enum(rule.column, rule.allowed_values)

    if rule.kind == ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL:
        return ~passes_non_negative_decimal(rule.column)

    if rule.kind == ValidationKind.REQUIRED_POSITIVE_INTEGER_STRICT:
        return ~passes_positive_integer_strict(rule.column)

    if rule.kind == ValidationKind.REQUIRED_NON_NEGATIVE_INTEGER:
        return ~passes_non_negative_integer(rule.column)

    raise ValueError(f"Unsupported validation kind '{rule.kind}' for rule {rule.rule_id}.")


def rule_failure_message(rule: TypeValidationRule) -> str:
    return f"{rule.rule_id}: type/domain validation failed for column '{rule.column}'"


CUSTOMER_TYPE_RULES: Tuple[TypeValidationRule, ...] = (
    TypeValidationRule(
        rule_id="customers_customer_id_valid",
        column="customer_id",
        description="Customer customer_id must be a positive integer",
        kind=ValidationKind.REQUIRED_POSITIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="customers_signup_date_valid",
        column="signup_date",
        description="Customer signup_date must be a valid date not in the future",
        kind=ValidationKind.REQUIRED_DATE_NOT_FUTURE,
    ),
    TypeValidationRule(
        rule_id="customers_segment_valid",
        column="customer_segment",
        description="Customer segment must be Premium, Standard, or Basic",
        kind=ValidationKind.REQUIRED_ENUM,
        allowed_values=CUSTOMER_SEGMENTS,
    ),
    TypeValidationRule(
        rule_id="customers_lifetime_value_valid",
        column="lifetime_value",
        description="Customer lifetime_value must be a non-negative decimal",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL,
    ),
)

ORDER_TYPE_RULES: Tuple[TypeValidationRule, ...] = (
    TypeValidationRule(
        rule_id="orders_order_id_valid",
        column="order_id",
        description="Order order_id must be a positive integer",
        kind=ValidationKind.REQUIRED_POSITIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="orders_customer_id_valid",
        column="customer_id",
        description="Order customer_id must be a positive integer when present",
        kind=ValidationKind.OPTIONAL_POSITIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="orders_order_date_valid",
        column="order_date",
        description="Order order_date must be a valid date not in the future",
        kind=ValidationKind.REQUIRED_DATE_NOT_FUTURE,
    ),
    TypeValidationRule(
        rule_id="orders_product_id_valid",
        column="product_id",
        description="Order product_id must be a positive integer when present",
        kind=ValidationKind.OPTIONAL_POSITIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="orders_quantity_positive",
        column="quantity",
        description="Order quantity must be a positive integer",
        kind=ValidationKind.REQUIRED_POSITIVE_INTEGER_STRICT,
    ),
    TypeValidationRule(
        rule_id="orders_unit_price_non_negative",
        column="unit_price",
        description="Order unit_price must be a non-negative decimal",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL,
    ),
    TypeValidationRule(
        rule_id="orders_total_amount_non_negative",
        column="total_amount",
        description="Order total_amount must be a non-negative decimal",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL,
    ),
    TypeValidationRule(
        rule_id="orders_status_valid",
        column="order_status",
        description="Order status must be Pending, Completed, or Cancelled",
        kind=ValidationKind.REQUIRED_ENUM,
        allowed_values=ORDER_STATUSES,
    ),
    TypeValidationRule(
        rule_id="orders_payment_date_valid",
        column="payment_date",
        description="Order payment_date must be a valid date when present",
        kind=ValidationKind.OPTIONAL_DATE,
    ),
)

PRODUCT_TYPE_RULES: Tuple[TypeValidationRule, ...] = (
    TypeValidationRule(
        rule_id="products_product_id_valid",
        column="product_id",
        description="Product product_id must be a positive integer",
        kind=ValidationKind.REQUIRED_POSITIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="products_price_non_negative",
        column="price",
        description="Product price must be a non-negative decimal",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL,
    ),
    TypeValidationRule(
        rule_id="products_cost_non_negative",
        column="cost",
        description="Product cost must be a non-negative decimal",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_DECIMAL,
    ),
    TypeValidationRule(
        rule_id="products_stock_quantity_non_negative",
        column="stock_quantity",
        description="Product stock_quantity must be a non-negative integer",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_INTEGER,
    ),
    TypeValidationRule(
        rule_id="products_reorder_level_non_negative",
        column="reorder_level",
        description="Product reorder_level must be a non-negative integer",
        kind=ValidationKind.REQUIRED_NON_NEGATIVE_INTEGER,
    ),
)

ENTITY_TYPE_RULES: Dict[str, Tuple[TypeValidationRule, ...]] = {
    "customers": CUSTOMER_TYPE_RULES,
    "orders": ORDER_TYPE_RULES,
    "products": PRODUCT_TYPE_RULES,
}


def _ensure_prior_quality_columns(df: DataFrame) -> DataFrame:
    """Initialize upstream quality columns when earlier modules were not run."""
    result = df
    defaults = {
        "dq_failed_rules": F.array().cast("array<string>"),
        "dq_completeness_errors": F.array().cast("array<string>"),
        "dq_uniqueness_errors": F.array().cast("array<string>"),
        "dq_type_business_errors": F.array().cast("array<string>"),
        "dq_completeness_pass": F.lit(True),
        "dq_uniqueness_pass": F.lit(True),
    }
    for column_name, default_value in defaults.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, default_value)
    return result


def apply_type_validation_checks(
    df: DataFrame,
    entity: str,
    rules: Sequence[TypeValidationRule],
    run_id: str,
) -> DataFrame:
    """Apply type/domain rules without removing rows."""
    if not rules:
        raise ValueError(f"No type validation rules configured for entity '{entity}'.")

    checked_at = datetime.now(timezone.utc)
    working = _ensure_prior_quality_columns(df)

    failure_conditions = [rule_failure_condition(rule) for rule in rules]
    any_failure = failure_conditions[0]
    for condition in failure_conditions[1:]:
        any_failure = any_failure | condition

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

    new_type_failed_rules = F.expr("filter(_dq_type_failed_candidates, x -> x is not null)")
    new_type_errors = F.expr("filter(_dq_type_error_candidates, x -> x is not null)")

    merged_failed_rules = F.array_union(
        F.coalesce(F.col("dq_failed_rules"), F.array().cast("array<string>")),
        new_type_failed_rules,
    )

    result = (
        working.withColumn("_dq_type_failed_candidates", failed_rule_candidates)
        .withColumn("_dq_type_error_candidates", error_candidates)
        .withColumn("dq_type_business_errors", new_type_errors)
        .withColumn("dq_type_business_pass", ~any_failure)
        .withColumn("dq_failed_rules", merged_failed_rules)
        .withColumn("dq_failure_count", F.size(F.col("dq_failed_rules")))
        .withColumn(
            "dq_is_valid",
            F.col("dq_completeness_pass")
            & F.col("dq_uniqueness_pass")
            & F.col("dq_type_business_pass"),
        )
        .withColumn("dq_run_id", F.lit(run_id))
        .withColumn("dq_checked_at", F.lit(checked_at))
        .drop("_dq_type_failed_candidates", "_dq_type_error_candidates")
    )

    logger.info(
        "Applied %s type validation rule(s) to entity=%s run_id=%s",
        len(rules),
        entity,
        run_id,
    )
    return result


def compute_type_validation_metrics(
    df: DataFrame,
    entity: str,
    rules: Sequence[TypeValidationRule],
    run_id: str,
) -> DataFrame:
    """Compute dataset-level type/domain metrics per rule."""
    if not rules:
        raise ValueError(f"No type validation rules configured for entity '{entity}'.")

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
                F.lit(CHECK_CATEGORY_TYPE_BUSINESS).alias("check_category"),
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
    """Count rows where a specific type validation rule failed (for tests)."""
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def run_type_validation(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, DataFrame]:
    """Read Silver/Bronze entity tables, apply type validation, persist results."""
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()
    write_mode = config.silver_write_mode

    ensure_schema_exists(spark, config)

    silver_customers = read_silver_table(spark, config, config.silver_customers_table)
    silver_orders = read_silver_table(spark, config, config.silver_orders_table)
    products_input = read_parent_table(
        spark,
        config,
        config.silver_products_table,
        config.bronze_products_table,
    )

    input_counts = {
        "customers": silver_customers.count(),
        "orders": silver_orders.count(),
        "products": products_input.count(),
    }
    logger.info(
        "Type validation input rows customers=%s orders=%s products=%s run_id=%s",
        input_counts["customers"],
        input_counts["orders"],
        input_counts["products"],
        run_id,
    )

    updated_customers = apply_type_validation_checks(
        silver_customers,
        entity="customers",
        rules=CUSTOMER_TYPE_RULES,
        run_id=run_id,
    )
    updated_orders = apply_type_validation_checks(
        silver_orders,
        entity="orders",
        rules=ORDER_TYPE_RULES,
        run_id=run_id,
    )
    updated_products = apply_type_validation_checks(
        products_input,
        entity="products",
        rules=PRODUCT_TYPE_RULES,
        run_id=run_id,
    )

    for entity, updated_df in (
        ("customers", updated_customers),
        ("orders", updated_orders),
        ("products", updated_products),
    ):
        output_count = updated_df.count()
        if output_count != input_counts[entity]:
            raise RuntimeError(
                f"Row count changed for {entity}: input={input_counts[entity]} "
                f"output={output_count}. Type validation must not delete rows.",
            )

    customer_metrics = compute_type_validation_metrics(
        silver_customers,
        entity="customers",
        rules=CUSTOMER_TYPE_RULES,
        run_id=run_id,
    )
    order_metrics = compute_type_validation_metrics(
        silver_orders,
        entity="orders",
        rules=ORDER_TYPE_RULES,
        run_id=run_id,
    )
    product_metrics = compute_type_validation_metrics(
        products_input,
        entity="products",
        rules=PRODUCT_TYPE_RULES,
        run_id=run_id,
    )
    metrics_df = customer_metrics.unionByName(order_metrics).unionByName(product_metrics)

    write_delta_table(
        updated_customers,
        config.qualified_table_name(config.silver_customers_table),
        write_mode,
    )
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
            "Type validation metric entity=%s rule=%s failed=%s pass_pct=%s fail_pct=%s",
            row["entity"],
            row["rule_id"],
            row["failed_rows"],
            row["pass_pct"],
            row["fail_pct"],
        )

    return {
        "customers": updated_customers,
        "orders": updated_orders,
        "products": updated_products,
        "metrics": metrics_df,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver type/domain validation.")
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
        run_type_validation(config=config)
        logger.info("Silver type validation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver type validation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
