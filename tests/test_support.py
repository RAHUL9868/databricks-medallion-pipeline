"""Shared helpers for pipeline integration tests."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

BRONZE_METADATA_DEFAULTS = {
    "_ingest_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "_source_file": "test.csv",
    "_source_path": "file:///tmp/test.csv",
    "_batch_id": "test-batch",
    "_source_row_num": 1,
    "_corrupt_record": None,
}


@dataclass(frozen=True)
class ValidationCheck:
    """Documents what a test validates and records expected vs actual outcomes."""

    name: str
    validates: str
    expected: Any
    actual: Any

    @property
    def passed(self) -> bool:
        return self.expected == self.actual

    def assert_pass(self) -> None:
        status = "PASS" if self.passed else "FAIL"
        message = (
            f"[{status}] {self.name}\n"
            f"  Validates : {self.validates}\n"
            f"  Expected  : {self.expected}\n"
            f"  Actual    : {self.actual}"
        )
        assert self.passed, message

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"[{status}] {self.name} | expected={self.expected} actual={self.actual} | "
            f"{self.validates}"
        )


def load_module(relative_path: str, module_name: str):
    module_path = SRC_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_create_silver_tables():
    return load_module("silver/create_silver_tables.py", "create_silver_tables")


def _normalize_row(entity: str, row: Dict[str, Any], row_num: int) -> Dict[str, Any]:
    normalized = dict(row)
    metadata = dict(BRONZE_METADATA_DEFAULTS)
    metadata["_source_row_num"] = row_num
    for key, value in metadata.items():
        normalized.setdefault(key, value)
    if "_corrupt_record" not in normalized:
        normalized["_corrupt_record"] = None
    return normalized


def dicts_to_bronze_dataframe(
    spark: SparkSession,
    entity: str,
    records: Sequence[Dict[str, Any]],
) -> DataFrame:
    """Build a Bronze-shaped DataFrame from plain dict records."""
    rows = [
        Row(**_normalize_row(entity, record, row_num))
        for row_num, record in enumerate(records, start=1)
    ]
    return spark.createDataFrame(rows)


def count_rule_failures(df: DataFrame, rule_id: str) -> int:
    return df.filter(F.array_contains(F.col("dq_failed_rules"), rule_id)).count()


def metric_failed_rows(metrics_df: DataFrame, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    if row is None:
        raise AssertionError(f"Metric row not found for rule_id={rule_id}")
    return int(row["failed_rows"])


def run_silver_pipeline(
    spark: SparkSession,
    bronze_customers: DataFrame,
    bronze_orders: DataFrame,
    bronze_products: DataFrame,
    run_id: str = "silver-test-run",
):
    """Execute the in-memory Silver quality pipeline."""
    cst = load_create_silver_tables()
    from config.pipeline_config import load_config

    config = load_config(run_id=run_id)
    pipeline = cst.apply_quality_pipeline(
        spark,
        bronze_customers,
        bronze_orders,
        bronze_products,
        run_id,
        config=config,
    )
    metrics = cst.collect_pipeline_metrics(
        spark,
        config,
        pipeline["_bronze_customers"],
        pipeline["_bronze_orders"],
        pipeline["_bronze_products"],
        pipeline["_customers_pre_ri"],
        pipeline["_orders_pre_ri"],
        pipeline["_orders_with_ri"],
        pipeline["_products_pre_business"],
        run_id,
        pipeline["_qc"],
        pipeline["_qu"],
        pipeline["_qtv"],
        pipeline["_ri"],
        pipeline["_qbl"],
    )
    report = cst.build_quality_report(metrics)
    return {
        "customers": pipeline["customers"],
        "orders": pipeline["orders"],
        "products": pipeline["products"],
        "metrics": metrics,
        "report": report,
    }


def assert_intentional_rule_counts(
    silver_df: DataFrame,
    metrics_df: DataFrame,
    expectations: Dict[str, int],
) -> List[ValidationCheck]:
    """Compare row-level and metric-level failure counts for a set of rules."""
    checks: List[ValidationCheck] = []
    for rule_id, expected in expectations.items():
        actual_row = count_rule_failures(silver_df, rule_id)
        checks.append(
            ValidationCheck(
                name=f"row_flag:{rule_id}",
                validates=f"Rows with dq_failed_rules containing '{rule_id}'",
                expected=expected,
                actual=actual_row,
            ),
        )
        actual_metric = metric_failed_rows(metrics_df, rule_id)
        checks.append(
            ValidationCheck(
                name=f"metric:{rule_id}",
                validates=f"silver_dq_metrics failed_rows for '{rule_id}'",
                expected=expected,
                actual=actual_metric,
            ),
        )
    return checks


def assert_all_pass(checks: Iterable[ValidationCheck]) -> None:
    failures = [check for check in checks if not check.passed]
    if failures:
        details = "\n".join(str(check) for check in failures)
        raise AssertionError(f"{len(failures)} validation check(s) failed:\n{details}")


def build_isolated_assignment_fixtures() -> Dict[str, List[Dict[str, Any]]]:
    """
    Build a small deterministic dataset with exact intentional issue counts.

    Disjoint corruption indices keep naturally valid rows available for false-positive checks.
    """
    customers: List[Dict[str, Any]] = []

    for customer_id in range(1, 51):
        customers.append(
            {
                "customer_id": str(customer_id),
                "customer_name": f"Customer {customer_id}",
                "email": None,
                "country": "United States",
                "signup_date": "2024-01-01",
                "customer_segment": "Standard",
                "lifetime_value": "100.00",
            },
        )

    for customer_id in range(51, 61):
        customers.append(
            {
                "customer_id": str(customer_id),
                "customer_name": f"Customer {customer_id}",
                "email": f"user{customer_id}@example.com",
                "country": "United States",
                "signup_date": "2024-01-01",
                "customer_segment": "Standard",
                "lifetime_value": "100.00",
            },
        )

    for row in customers[50:60]:
        customers.append(dict(row))

    products = [
        {
            "product_id": str(product_id),
            "product_name": f"Product {product_id}",
            "category": "Electronics",
            "price": "10.00",
            "cost": "5.00",
            "stock_quantity": "100",
            "reorder_level": "10",
        }
        for product_id in range(1, 6)
    ]

    base_order_count = 500
    orders: List[Dict[str, Any]] = []
    for order_id in range(1, base_order_count + 1):
        orders.append(
            {
                "order_id": str(order_id),
                "customer_id": str(((order_id - 1) % 60) + 1),
                "order_date": "2024-06-01",
                "product_id": str(((order_id - 1) % 5) + 1),
                "quantity": "2",
                "unit_price": "10.00",
                "total_amount": "20.00",
                "order_status": "Completed",
                "payment_date": "2024-06-02",
            },
        )

    null_customer = list(range(0, 100))
    null_product = list(range(100, 300))
    invalid_customer = list(range(300, 350))
    invalid_product = list(range(350, 380))
    duplicate_source = list(range(380, 400))

    for idx in null_customer:
        orders[idx]["customer_id"] = None
    for idx in null_product:
        orders[idx]["product_id"] = None
    for offset, idx in enumerate(invalid_customer):
        orders[idx]["customer_id"] = str(9_000_001 + offset)
    for offset, idx in enumerate(invalid_product):
        orders[idx]["product_id"] = str(9_000_001 + offset)
    for idx in duplicate_source:
        orders.append(dict(orders[idx]))

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "clean_order_indices": set(range(400, base_order_count)),
    }
