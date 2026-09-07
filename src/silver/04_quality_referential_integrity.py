#!/usr/bin/env python3
"""
Silver referential integrity validation for orders foreign keys.

Relationships validated
-----------------------
- orders.customer_id -> customers.customer_id
- orders.product_id  -> products.product_id

NULL vs orphan failures
-----------------------
| Condition                         | Rule category   | Rule ID                         |
|----------------------------------|-----------------|----------------------------------|
| NULL or blank foreign key        | Completeness    | orders_*_id_not_null             |
| Non-null key missing from parent | Referential integrity | orders_*_id_exists         |

This module **detects** NULL foreign keys only for logging and test assertions.
NULL keys are **not** counted as referential-integrity failures and are **not**
added to `dq_failed_rules` under the RI rule IDs.

Duplicate customer IDs in the customer source
---------------------------------------------
Parent reference keys are built with ``distinct()`` on non-null ``customer_id``
values from the configured customer table (Silver preferred, Bronze fallback).

Implications:
- If ``customer_id = 1001`` appears on two customer rows, ``1001`` is still a valid
  reference key. Orders pointing to ``1001`` **pass** referential integrity.
- Duplicate customer rows are a **uniqueness** issue, not an RI issue.
- RI does not attempt to pick a canonical/survivor customer row.

Evaluation uses Spark left joins against broadcast distinct key sets (no driver
collection of full parent tables).

Columns added/updated
---------------------
| Column                 | Description |
|------------------------|-------------|
| dq_referential_pass    | False when any RI rule fails on the row |
| dq_referential_errors  | Human-readable RI failure messages |
| dq_failed_rules        | Existing failures plus RI rule ids when applicable |
| dq_failure_count       | Size of `dq_failed_rules` |
| dq_is_valid            | completeness AND uniqueness AND referential pass flags |
| dq_run_id              | Updated run id |
| dq_checked_at          | Updated timestamp |
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
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
    CHECK_CATEGORY_REFERENTIAL_INTEGRITY,
    ensure_schema_exists,
    get_spark,
    is_null_or_blank,
    load_reference_keys,
    read_parent_table,
    read_silver_table,
    write_delta_table,
    write_metrics_table,
)

logger = logging.getLogger(__name__)

EXPECTED_INVALID_CUSTOMER_IDS = 50
EXPECTED_INVALID_PRODUCT_IDS = 30


@dataclass(frozen=True)
class ReferentialIntegrityRule:
    rule_id: str
    fk_column: str
    parent_key_alias: str
    description: str
    parent_silver_table: str
    parent_bronze_table: str
    parent_key_column: str


ORDER_REFERENTIAL_RULES: Tuple[ReferentialIntegrityRule, ...] = (
    ReferentialIntegrityRule(
        rule_id="orders_customer_id_exists",
        fk_column="customer_id",
        parent_key_alias="_valid_customer_id",
        description="Order customer_id must exist in customers when present",
        parent_silver_table="silver_customers_table",
        parent_bronze_table="bronze_customers_table",
        parent_key_column="customer_id",
    ),
    ReferentialIntegrityRule(
        rule_id="orders_product_id_exists",
        fk_column="product_id",
        parent_key_alias="_valid_product_id",
        description="Order product_id must exist in products when present",
        parent_silver_table="silver_products_table",
        parent_bronze_table="bronze_products_table",
        parent_key_column="product_id",
    ),
)


def _resolve_table_name(config: PipelineConfig, attribute_name: str) -> str:
    return str(getattr(config, attribute_name))


def orphan_foreign_key_condition(fk_column: str, parent_match_column: str) -> Column:
    """True when FK is present but no parent match exists (orphan key)."""
    return (~is_null_or_blank(fk_column)) & F.col(parent_match_column).isNull()


def _ensure_prior_quality_columns(df: DataFrame) -> DataFrame:
    """Initialize upstream quality columns when earlier modules were not run."""
    result = df
    defaults = {
        "dq_failed_rules": F.array().cast("array<string>"),
        "dq_completeness_errors": F.array().cast("array<string>"),
        "dq_uniqueness_errors": F.array().cast("array<string>"),
        "dq_referential_errors": F.array().cast("array<string>"),
        "dq_type_business_errors": F.array().cast("array<string>"),
        "dq_completeness_pass": F.lit(True),
        "dq_uniqueness_pass": F.lit(True),
        "dq_type_business_pass": F.lit(True),
    }
    for column_name, default_value in defaults.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, default_value)
    return result


def build_reference_key_map(
    spark: SparkSession,
    config: PipelineConfig,
    rules: Sequence[ReferentialIntegrityRule],
) -> Dict[str, DataFrame]:
    """Load distinct parent key DataFrames for each referential rule."""
    reference_map: Dict[str, DataFrame] = {}
    for rule in rules:
        if rule.parent_key_alias in reference_map:
            continue
        parent_df = read_parent_table(
            spark,
            config,
            _resolve_table_name(config, rule.parent_silver_table),
            _resolve_table_name(config, rule.parent_bronze_table),
        )
        reference_map[rule.parent_key_alias] = load_reference_keys(
            parent_df,
            rule.parent_key_column,
            rule.parent_key_alias,
        )
        logger.info(
            "Loaded reference keys alias=%s parent_key=%s",
            rule.parent_key_alias,
            rule.parent_key_column,
        )
    return reference_map


def apply_referential_integrity_checks(
    orders_df: DataFrame,
    reference_keys: Dict[str, DataFrame],
    rules: Sequence[ReferentialIntegrityRule],
    run_id: str,
) -> DataFrame:
    """Apply referential integrity rules to orders without removing rows."""
    if not rules:
        raise ValueError("At least one referential integrity rule is required.")

    checked_at = datetime.now(timezone.utc)
    working = _ensure_prior_quality_columns(orders_df)

    enriched = working
    match_columns: Dict[str, str] = {}
    for rule in rules:
        parent_keys = F.broadcast(reference_keys[rule.parent_key_alias])
        match_column = f"_ri_match_{rule.rule_id}"
        match_columns[rule.rule_id] = match_column
        enriched = enriched.join(
            parent_keys,
            F.col(rule.fk_column) == F.col(rule.parent_key_alias),
            how="left",
        ).drop(rule.parent_key_alias)

    orphan_conditions = [
        orphan_foreign_key_condition(rule.fk_column, match_columns[rule.rule_id])
        for rule in rules
    ]
    any_orphan = orphan_conditions[0]
    for condition in orphan_conditions[1:]:
        any_orphan = any_orphan | condition

    failed_rule_candidates = F.array(
        *[
            F.when(
                orphan_foreign_key_condition(rule.fk_column, match_columns[rule.rule_id]),
                F.lit(rule.rule_id),
            )
            for rule in rules
        ],
    )
    error_candidates = F.array(
        *[
            F.when(
                orphan_foreign_key_condition(rule.fk_column, match_columns[rule.rule_id]),
                F.lit(
                    f"{rule.rule_id}: orphan {rule.fk_column} not found in parent "
                    f"{rule.parent_key_column}",
                ),
            )
            for rule in rules
        ],
    )

    new_referential_failed_rules = F.expr(
        "filter(_dq_referential_failed_candidates, x -> x is not null)",
    )
    new_referential_errors = F.expr(
        "filter(_dq_referential_error_candidates, x -> x is not null)",
    )

    merged_failed_rules = F.array_union(
        F.coalesce(F.col("dq_failed_rules"), F.array().cast("array<string>")),
        new_referential_failed_rules,
    )

    result = (
        enriched.withColumn("_dq_referential_failed_candidates", failed_rule_candidates)
        .withColumn("_dq_referential_error_candidates", error_candidates)
        .withColumn("dq_referential_errors", new_referential_errors)
        .withColumn("dq_referential_pass", ~any_orphan)
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
        .drop(
            "_dq_referential_failed_candidates",
            "_dq_referential_error_candidates",
            *match_columns.values(),
        )
    )

    logger.info(
        "Applied %s referential integrity rule(s) run_id=%s",
        len(rules),
        run_id,
    )
    return result


def compute_referential_integrity_metrics(
    orders_df: DataFrame,
    reference_keys: Dict[str, DataFrame],
    rules: Sequence[ReferentialIntegrityRule],
    run_id: str,
) -> DataFrame:
    """Compute dataset-level RI metrics (orphan keys only; NULLs excluded)."""
    evaluated_at = datetime.now(timezone.utc)

    enriched = orders_df
    match_columns: Dict[str, str] = {}
    for rule in rules:
        parent_keys = F.broadcast(reference_keys[rule.parent_key_alias])
        match_column = f"_ri_metric_match_{rule.rule_id}"
        match_columns[rule.rule_id] = match_column
        enriched = enriched.join(
            parent_keys,
            F.col(rule.fk_column) == F.col(rule.parent_key_alias),
            how="left",
        ).drop(rule.parent_key_alias)

    agg_exprs = [F.count(F.lit(1)).alias("total_rows")]
    for rule in rules:
        agg_exprs.append(
            F.sum(
                F.when(
                    orphan_foreign_key_condition(
                        rule.fk_column,
                        match_columns[rule.rule_id],
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            ).alias(rule.rule_id),
        )

    totals = enriched.agg(*agg_exprs)
    total_rows = F.col("total_rows")

    metric_frames: List[DataFrame] = []
    for rule in rules:
        failed_rows = F.col(rule.rule_id)
        passed_rows = total_rows - failed_rows
        metric_frames.append(
            totals.select(
                F.lit(run_id).alias("run_id"),
                F.lit("orders").alias("entity"),
                F.lit(CHECK_CATEGORY_REFERENTIAL_INTEGRITY).alias("check_category"),
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


def count_null_foreign_keys(df: DataFrame, fk_column: str) -> int:
    """Count NULL/blank foreign keys (completeness category, not RI failures)."""
    return df.filter(is_null_or_blank(fk_column)).count()


def count_orphan_foreign_keys(df: DataFrame, rule_id: str) -> int:
    """Count rows flagged by a referential integrity rule."""
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def run_referential_integrity_validation(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, DataFrame]:
    """Read Silver orders and parent tables, apply RI checks, persist results."""
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()
    write_mode = config.silver_write_mode

    ensure_schema_exists(spark, config)

    silver_orders = read_silver_table(spark, config, config.silver_orders_table)
    input_row_count = silver_orders.count()

    null_customer_count = count_null_foreign_keys(silver_orders, "customer_id")
    null_product_count = count_null_foreign_keys(silver_orders, "product_id")
    logger.info(
        "NULL foreign keys detected (completeness category): customer_id=%s product_id=%s",
        null_customer_count,
        null_product_count,
    )

    reference_keys = build_reference_key_map(spark, config, ORDER_REFERENTIAL_RULES)

    updated_orders = apply_referential_integrity_checks(
        silver_orders,
        reference_keys=reference_keys,
        rules=ORDER_REFERENTIAL_RULES,
        run_id=run_id,
    )

    output_row_count = updated_orders.count()
    if output_row_count != input_row_count:
        raise RuntimeError(
            f"Row count changed for orders: input={input_row_count} "
            f"output={output_row_count}. Referential integrity must not delete rows.",
        )

    metrics_df = compute_referential_integrity_metrics(
        silver_orders,
        reference_keys=reference_keys,
        rules=ORDER_REFERENTIAL_RULES,
        run_id=run_id,
    )

    orders_table = config.qualified_table_name(config.silver_orders_table)
    write_delta_table(updated_orders, orders_table, write_mode)
    write_metrics_table(spark, metrics_df, config, write_mode="append")

    for row in metrics_df.collect():
        logger.info(
            "Referential integrity metric rule=%s orphan_rows=%s pass_pct=%s fail_pct=%s",
            row["rule_id"],
            row["failed_rows"],
            row["pass_pct"],
            row["fail_pct"],
        )

    return {
        "orders": updated_orders,
        "metrics": metrics_df,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver referential integrity validation.")
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
        run_referential_integrity_validation(config=config)
        logger.info("Silver referential integrity validation completed successfully.")
        return 0
    except (ValueError, RuntimeError) as exc:
        logger.error("Silver referential integrity validation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
