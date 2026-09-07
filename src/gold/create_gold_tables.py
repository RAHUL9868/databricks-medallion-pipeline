#!/usr/bin/env python3
"""
Orchestrate creation of all Gold analytical tables from Silver sources.

Executes Gold SQL assets in dependency order, logs row counts, and runs
reconciliation checks. Safe reruns use CREATE OR REPLACE TABLE in each SQL script
(overwrite semantics).

Gold tables created:
  - gold_sales_by_product
  - gold_revenue_by_customer
  - gold_daily_weekly_trends
  - gold_customer_segmentation
"""

from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from config.pipeline_config import PipelineConfig, load_config
from silver.silver_common import ensure_schema_exists, get_spark

sys.modules.setdefault(__name__, sys.modules[__name__])

logger = logging.getLogger(__name__)

REVENUE_TOLERANCE = Decimal("0.01")
COMPLETED_STATUS = "Completed"
RECONCILIATION_LOGIC_VERSION = "v2-sql-baseline"

# Mirrors src/gold/02_revenue_by_customer.sql grain (valid_customers LEFT JOIN order metrics).
SILVER_CUSTOMER_REVENUE_SQL = """
WITH valid_customers AS (
    SELECT customer_id
    FROM (
        SELECT
            customer_id,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM silver_customers
        WHERE dq_is_valid = true
          AND customer_id IS NOT NULL
    ) ranked
    WHERE row_rank = 1
),
qualifying_orders AS (
    SELECT
        order_id,
        customer_id,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND customer_id IS NOT NULL
      AND total_amount IS NOT NULL
),
deduplicated_orders AS (
    SELECT order_id, customer_id, total_amount
    FROM (
        SELECT
            order_id,
            customer_id,
            total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM qualifying_orders
    ) ranked
    WHERE row_rank = 1
),
customer_order_metrics AS (
    SELECT
        customer_id,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY customer_id
)
SELECT CAST(
    SUM(COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))))
    AS DECIMAL(18, 2)
) AS value
FROM valid_customers AS c
LEFT JOIN customer_order_metrics AS m
    ON c.customer_id = m.customer_id
"""

# Mirrors src/gold/01_sales_by_product.sql grain (valid_products LEFT JOIN order metrics).
SILVER_PRODUCT_REVENUE_SQL = """
WITH valid_products AS (
    SELECT product_id
    FROM silver_products
    WHERE dq_is_valid = true
      AND product_id IS NOT NULL
),
qualifying_orders AS (
    SELECT
        order_id,
        product_id,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND product_id IS NOT NULL
      AND total_amount IS NOT NULL
),
deduplicated_orders AS (
    SELECT order_id, product_id, total_amount
    FROM (
        SELECT
            order_id,
            product_id,
            total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM qualifying_orders
    ) ranked
    WHERE row_rank = 1
),
product_order_metrics AS (
    SELECT
        product_id,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY product_id
)
SELECT CAST(
    SUM(COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))))
    AS DECIMAL(18, 2)
) AS value
FROM valid_products AS p
LEFT JOIN product_order_metrics AS m
    ON p.product_id = m.product_id
"""

GOLD_SQL_DIR = Path(__file__).resolve().parent


class GoldTableStep:
    """One Gold table build step."""

    def __init__(self, logical_name: str, sql_file: str, table_config_key: str) -> None:
        self.logical_name = logical_name
        self.sql_file = sql_file
        self.table_config_key = table_config_key


GOLD_TABLE_STEPS: Tuple[GoldTableStep, ...] = (
    GoldTableStep(
        logical_name="sales_by_product",
        sql_file="01_sales_by_product.sql",
        table_config_key="gold_sales_by_product_table",
    ),
    GoldTableStep(
        logical_name="revenue_by_customer",
        sql_file="02_revenue_by_customer.sql",
        table_config_key="gold_revenue_by_customer_table",
    ),
    GoldTableStep(
        logical_name="daily_weekly_trends",
        sql_file="03_daily_weekly_trends.sql",
        table_config_key="gold_daily_weekly_trends_table",
    ),
    GoldTableStep(
        logical_name="customer_segmentation",
        sql_file="04_customer_segmentation.sql",
        table_config_key="gold_customer_segmentation_table",
    ),
)


class GoldPipelineError(RuntimeError):
    """Raised when Gold table creation or reconciliation fails."""


def set_sql_context(spark: SparkSession, config: PipelineConfig) -> None:
    """Set Spark SQL catalog/schema so Gold scripts resolve Silver tables."""
    if config.catalog:
        spark.sql(f"USE CATALOG {config.catalog}")
    spark.sql(f"USE {config.schema_name}")


def _table_name(config: PipelineConfig, attribute: str) -> str:
    return config.qualified_table_name(str(getattr(config, attribute)))


def _assert_table_exists(spark: SparkSession, qualified_name: str) -> None:
    if not spark.catalog.tableExists(qualified_name):
        raise GoldPipelineError(
            f"Required table '{qualified_name}' does not exist. "
            "Run Silver table creation before Gold.",
        )


def ensure_silver_sources_exist(spark: SparkSession, config: PipelineConfig) -> None:
    """Fail fast when upstream Silver tables are missing."""
    for attribute in (
        "silver_customers_table",
        "silver_orders_table",
        "silver_products_table",
    ):
        qualified = _table_name(config, attribute)
        _assert_table_exists(spark, qualified)
        logger.info("Verified Silver source table %s", qualified)


def read_gold_sql(script_name: str) -> str:
    script_path = GOLD_SQL_DIR / script_name
    if not script_path.exists():
        raise GoldPipelineError(f"Gold SQL script not found: {script_path}")
    return script_path.read_text(encoding="utf-8")


def execute_gold_sql(
    spark: SparkSession,
    config: PipelineConfig,
    step: GoldTableStep,
) -> str:
    """Execute one Gold SQL script and return the qualified target table name."""
    set_sql_context(spark, config)
    sql = read_gold_sql(step.sql_file)
    target_table = _table_name(config, step.table_config_key)
    logger.info(
        "Executing Gold SQL logical_name=%s script=%s target=%s",
        step.logical_name,
        step.sql_file,
        target_table,
    )
    spark.sql(sql)
    if not spark.catalog.tableExists(target_table):
        raise GoldPipelineError(
            f"Gold SQL completed but table '{target_table}' was not created "
            f"from script {step.sql_file}.",
        )
    return target_table


def deduplicate_qualifying_orders(orders_df: DataFrame) -> DataFrame:
    """Deduplicate qualifying orders to one row per order_id."""
    window = Window.partitionBy("order_id").orderBy(F.col("_source_row_num"))
    return (
        orders_df.withColumn("_dedupe_rank", F.row_number().over(window))
        .filter(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )


def build_valid_deduplicated_customers(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Valid customers at one row per customer_id (matches Gold valid_customers CTE)."""
    customers = spark.table(_table_name(config, "silver_customers_table"))
    window = Window.partitionBy("customer_id").orderBy(F.col("_source_row_num"))
    return (
        customers.filter(
            (F.col("dq_is_valid") == F.lit(True)) & F.col("customer_id").isNotNull(),
        )
        .withColumn("_dedupe_rank", F.row_number().over(window))
        .filter(F.col("_dedupe_rank") == 1)
        .select("customer_id")
    )


def build_valid_deduplicated_products(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Valid products at one row per product_id (matches Gold valid_products CTE)."""
    products = spark.table(_table_name(config, "silver_products_table"))
    window = Window.partitionBy("product_id").orderBy(F.col("_source_row_num"))
    return (
        products.filter(
            (F.col("dq_is_valid") == F.lit(True)) & F.col("product_id").isNotNull(),
        )
        .withColumn("_dedupe_rank", F.row_number().over(window))
        .filter(F.col("_dedupe_rank") == 1)
        .select("product_id")
    )


def build_qualifying_silver_orders(
    spark: SparkSession,
    config: PipelineConfig,
    require_customer_id: bool = False,
    require_product_id: bool = False,
    require_order_date: bool = False,
    require_valid_customer: bool = False,
    require_valid_product: bool = False,
) -> DataFrame:
    """
    Build the Silver order set used for Gold revenue reconciliation.

    Matches Gold SQL filters: valid, Completed, non-null total_amount, deduped.
    When ``require_valid_customer`` or ``require_valid_product`` is set, only orders
    attributable to valid dimension rows are included (same as Gold LEFT JOIN grain).
    """
    orders = spark.table(_table_name(config, "silver_orders_table"))
    filtered = orders.filter(
        (F.col("dq_is_valid") == F.lit(True))
        & (F.col("order_status") == F.lit(COMPLETED_STATUS))
        & F.col("total_amount").isNotNull(),
    )
    if require_customer_id:
        filtered = filtered.filter(F.col("customer_id").isNotNull())
    if require_product_id:
        filtered = filtered.filter(F.col("product_id").isNotNull())
    if require_order_date:
        filtered = filtered.filter(F.col("order_date").isNotNull())
    if require_valid_customer:
        filtered = filtered.join(
            build_valid_deduplicated_customers(spark, config),
            on="customer_id",
            how="inner",
        )
    if require_valid_product:
        filtered = filtered.join(
            build_valid_deduplicated_products(spark, config),
            on="product_id",
            how="inner",
        )
    return deduplicate_qualifying_orders(filtered)


def silver_qualifying_revenue(
    spark: SparkSession,
    config: PipelineConfig,
    require_customer_id: bool = False,
    require_product_id: bool = False,
    require_order_date: bool = False,
    require_valid_customer: bool = False,
    require_valid_product: bool = False,
) -> Decimal:
    """Sum total_amount for qualifying deduplicated Silver orders."""
    orders = build_qualifying_silver_orders(
        spark,
        config,
        require_customer_id=require_customer_id,
        require_product_id=require_product_id,
        require_order_date=require_order_date,
        require_valid_customer=require_valid_customer,
        require_valid_product=require_valid_product,
    )
    if orders.limit(1).count() == 0:
        return Decimal("0.00")
    total = orders.agg(
        F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("total_revenue"),
    ).first()["total_revenue"]
    if total is None:
        return Decimal("0.00")
    return Decimal(str(total)).quantize(Decimal("0.01"))


def count_valid_deduplicated_customers(spark: SparkSession, config: PipelineConfig) -> int:
    """Count valid Silver customers at one row per customer_id."""
    return build_valid_deduplicated_customers(spark, config).count()


def count_valid_deduplicated_products(spark: SparkSession, config: PipelineConfig) -> int:
    """Count valid Silver products at one row per product_id."""
    return build_valid_deduplicated_products(spark, config).count()


def _sum_gold_revenue(spark: SparkSession, qualified_table: str, column: str = "total_revenue") -> Decimal:
    total = spark.table(qualified_table).agg(
        F.sum(F.col(column).cast("decimal(18,2)")).alias("value"),
    ).first()["value"]
    if total is None:
        return Decimal("0.00")
    return Decimal(str(total)).quantize(Decimal("0.01"))


def _sum_sql_revenue_scalar(spark: SparkSession, config: PipelineConfig, sql: str) -> Decimal:
    """Run a Gold-equivalent Silver revenue query and return a DECIMAL(18,2) total."""
    set_sql_context(spark, config)
    value = spark.sql(sql).first()["value"]
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _assert_revenue_match(
    check_name: str,
    gold_total: Decimal,
    silver_total: Decimal,
    context: str,
) -> None:
    delta = abs(gold_total - silver_total)
    if delta > REVENUE_TOLERANCE:
        raise GoldPipelineError(
            f"Reconciliation failed [{check_name}]: {context}. "
            f"gold_total={gold_total} silver_total={silver_total} delta={delta} "
            f"tolerance={REVENUE_TOLERANCE}.",
        )
    logger.info(
        "Reconciliation passed [%s]: gold_total=%s silver_total=%s",
        check_name,
        gold_total,
        silver_total,
    )


def _assert_count_match(check_name: str, actual: int, expected: int, context: str) -> None:
    if actual != expected:
        raise GoldPipelineError(
            f"Reconciliation failed [{check_name}]: {context}. "
            f"actual={actual} expected={expected}.",
        )
    logger.info("Reconciliation passed [%s]: count=%s", check_name, actual)


def _assert_no_duplicate_keys(
    spark: SparkSession,
    qualified_table: str,
    key_column: str,
    context: str,
) -> None:
    df = spark.table(qualified_table)
    total_rows = df.count()
    distinct_keys = df.select(key_column).distinct().count()
    if total_rows != distinct_keys:
        raise GoldPipelineError(
            f"Reconciliation failed [duplicate_{key_column}]: {context}. "
            f"total_rows={total_rows} distinct_{key_column}={distinct_keys}.",
        )
    logger.info(
        "Reconciliation passed [duplicate_%s]: total_rows=%s on %s",
        key_column,
        total_rows,
        qualified_table,
    )


def run_reconciliation_checks(
    spark: SparkSession,
    config: PipelineConfig,
    gold_tables: Dict[str, str],
) -> None:
    """Validate Gold outputs against Silver baselines and grain constraints."""
    logger.info("Running Gold reconciliation checks logic=%s", RECONCILIATION_LOGIC_VERSION)
    sales_table = gold_tables["sales_by_product"]
    customer_table = gold_tables["revenue_by_customer"]
    trends_table = gold_tables["daily_weekly_trends"]
    segmentation_table = gold_tables["customer_segmentation"]

    gold_product_revenue = _sum_gold_revenue(spark, sales_table)
    silver_product_revenue = _sum_sql_revenue_scalar(
        spark,
        config,
        SILVER_PRODUCT_REVENUE_SQL,
    )
    _assert_revenue_match(
        "sales_by_product_revenue",
        gold_product_revenue,
        silver_product_revenue,
        "SUM(gold_sales_by_product.total_revenue) vs Silver SQL baseline "
        "(valid products LEFT JOIN order metrics)",
    )

    gold_customer_revenue = _sum_gold_revenue(spark, customer_table)
    silver_customer_revenue = _sum_sql_revenue_scalar(
        spark,
        config,
        SILVER_CUSTOMER_REVENUE_SQL,
    )
    _assert_revenue_match(
        "revenue_by_customer_revenue",
        gold_customer_revenue,
        silver_customer_revenue,
        "SUM(gold_revenue_by_customer.total_revenue) vs Silver SQL baseline "
        "(valid customers LEFT JOIN order metrics)",
    )

    gold_daily_revenue = (
        spark.table(trends_table)
        .filter(F.col("period_grain") == F.lit("DAY"))
        .agg(F.sum(F.col("total_revenue").cast("decimal(18,2)")).alias("value"))
        .first()["value"]
    )
    gold_daily_revenue = (
        Decimal("0.00")
        if gold_daily_revenue is None
        else Decimal(str(gold_daily_revenue)).quantize(Decimal("0.01"))
    )
    silver_daily_revenue = silver_qualifying_revenue(
        spark,
        config,
        require_order_date=True,
    )
    _assert_revenue_match(
        "daily_trends_revenue",
        gold_daily_revenue,
        silver_daily_revenue,
        "SUM(daily total_revenue) vs qualifying Silver orders with order_date",
    )

    gold_weekly_revenue = (
        spark.table(trends_table)
        .filter(F.col("period_grain") == F.lit("WEEK"))
        .agg(F.sum(F.col("total_revenue").cast("decimal(18,2)")).alias("value"))
        .first()["value"]
    )
    gold_weekly_revenue = (
        Decimal("0.00")
        if gold_weekly_revenue is None
        else Decimal(str(gold_weekly_revenue)).quantize(Decimal("0.01"))
    )
    _assert_revenue_match(
        "weekly_equals_daily_revenue",
        gold_weekly_revenue,
        gold_daily_revenue,
        "Weekly revenue total should equal daily revenue total (same order set)",
    )

    segmented_customers = (
        spark.table(segmentation_table)
        .agg(F.sum(F.col("customer_count")).alias("value"))
        .first()["value"]
    )
    segmented_customers = int(segmented_customers or 0)
    valid_customers = count_valid_deduplicated_customers(spark, config)
    _assert_count_match(
        "segmentation_customer_population",
        segmented_customers,
        valid_customers,
        "SUM(gold_customer_segmentation.customer_count) vs valid deduplicated Silver customers",
    )

    expected_products = count_valid_deduplicated_products(spark, config)
    actual_products = spark.table(sales_table).count()
    _assert_count_match(
        "sales_by_product_row_count",
        actual_products,
        expected_products,
        "gold_sales_by_product row count vs valid deduplicated Silver products",
    )
    _assert_no_duplicate_keys(
        spark,
        sales_table,
        "product_id",
        "gold_sales_by_product must have one row per product_id",
    )

    expected_customers = valid_customers
    actual_customers = spark.table(customer_table).count()
    _assert_count_match(
        "revenue_by_customer_row_count",
        actual_customers,
        expected_customers,
        "gold_revenue_by_customer row count vs valid deduplicated Silver customers",
    )
    _assert_no_duplicate_keys(
        spark,
        customer_table,
        "customer_id",
        "gold_revenue_by_customer must have one row per customer_id",
    )


def log_gold_row_counts(spark: SparkSession, gold_tables: Dict[str, str]) -> Dict[str, int]:
    """Log and return row counts for each Gold table."""
    counts: Dict[str, int] = {}
    for logical_name, qualified_table in gold_tables.items():
        row_count = spark.table(qualified_table).count()
        counts[logical_name] = row_count
        logger.info("Gold row count logical_name=%s table=%s rows=%s", logical_name, qualified_table, row_count)
    return counts


def build_gold_tables(
    spark: SparkSession,
    config: PipelineConfig,
) -> Dict[str, str]:
    """Execute Gold SQL scripts in dependency order and return qualified table names."""
    ensure_schema_exists(spark, config)
    ensure_silver_sources_exist(spark, config)
    set_sql_context(spark, config)

    run_id = config.resolved_run_id()
    logger.info("Starting Gold table creation run_id=%s write_mode=%s", run_id, config.gold_write_mode)

    gold_tables: Dict[str, str] = {}
    for step in GOLD_TABLE_STEPS:
        gold_tables[step.logical_name] = execute_gold_sql(spark, config, step)
    return gold_tables


def run_create_gold_tables(
    spark: Optional[SparkSession] = None,
    config: Optional[PipelineConfig] = None,
    run_reconciliation: bool = True,
) -> Dict[str, object]:
    """
    Build all Gold tables from Silver and optionally run reconciliation checks.

    Returns a dict with gold table qualified names and row counts.
    """
    spark = get_spark(spark)
    config = config or load_config()
    run_id = config.resolved_run_id()

    gold_tables = build_gold_tables(spark, config)
    row_counts = log_gold_row_counts(spark, gold_tables)

    if run_reconciliation:
        run_reconciliation_checks(spark, config, gold_tables)

    logger.info("Gold table creation completed successfully run_id=%s", run_id)
    return {
        "tables": gold_tables,
        "row_counts": row_counts,
        "run_id": run_id,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Gold analytical tables from Silver.")
    parser.add_argument("--catalog", help="Unity Catalog name (optional).")
    parser.add_argument("--schema", help="Database/schema name (default: ecommerce).")
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        help="Gold write mode label for logging (SQL uses CREATE OR REPLACE).",
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
        gold_write_mode=args.write_mode,
        run_id=args.run_id,
    )

    try:
        run_create_gold_tables(config=config)
        return 0
    except GoldPipelineError as exc:
        logger.error("Gold table creation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
