"""Tests for Silver referential integrity validation."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from silver.silver_common import load_reference_keys


def _load_ri_module():
    module_path = SRC_ROOT / "silver" / "04_quality_referential_integrity.py"
    spec = importlib.util.spec_from_file_location("quality_referential_integrity", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load RI module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ri = _load_ri_module()


def _customer_row(customer_id: str) -> Row:
    return Row(
        customer_id=customer_id,
        customer_name=f"Customer {customer_id}",
        email=f"user{customer_id}@example.com",
        country="United States",
        signup_date="2024-01-01",
        customer_segment="Standard",
        lifetime_value="100.00",
    )


def _product_row(product_id: str) -> Row:
    return Row(
        product_id=product_id,
        product_name=f"Product {product_id}",
        category="Electronics",
        price="10.00",
        cost="5.00",
        stock_quantity="100",
        reorder_level="10",
    )


def _order_row(
    order_id: str,
    customer_id: str | None,
    product_id: str | None,
) -> Row:
    return Row(
        order_id=order_id,
        customer_id=customer_id,
        order_date="2024-06-01",
        product_id=product_id,
        quantity="1",
        unit_price="10.00",
        total_amount="10.00",
        order_status="Completed",
        payment_date="2024-06-02",
    )


def _reference_keys(spark: SparkSession, customers_df, products_df):
    return {
        "_valid_customer_id": load_reference_keys(customers_df, "customer_id", "_valid_customer_id"),
        "_valid_product_id": load_reference_keys(products_df, "product_id", "_valid_product_id"),
    }


def _metric_failed_rows(metrics_df, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    assert row is not None, f"Metric row not found for rule_id={rule_id}"
    return int(row["failed_rows"])


class TestReferentialIntegrityValidation:
    def test_detects_fifty_invalid_customer_ids(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame([_customer_row(str(i)) for i in range(1, 101)])
        products_df = spark.createDataFrame([_product_row("1")])

        orders = []
        for i in range(1, ri.EXPECTED_INVALID_CUSTOMER_IDS + 1):
            orders.append(_order_row(str(i), str(9_000_000 + i), "1"))
        for i in range(ri.EXPECTED_INVALID_CUSTOMER_IDS + 1, 201):
            orders.append(_order_row(str(i), "1", "1"))
        orders_df = spark.createDataFrame(orders)

        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-customer-test",
        )
        metrics_df = ri.compute_referential_integrity_metrics(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-customer-test",
        )

        assert silver_df.count() == len(orders)
        assert (
            ri.count_orphan_foreign_keys(silver_df, "orders_customer_id_exists")
            == ri.EXPECTED_INVALID_CUSTOMER_IDS
        )
        assert (
            _metric_failed_rows(metrics_df, "orders_customer_id_exists")
            == ri.EXPECTED_INVALID_CUSTOMER_IDS
        )

    def test_detects_thirty_invalid_product_ids(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame([_customer_row("1")])
        products_df = spark.createDataFrame([_product_row(str(i)) for i in range(1, 51)])

        orders = []
        for i in range(1, ri.EXPECTED_INVALID_PRODUCT_IDS + 1):
            orders.append(_order_row(str(i), "1", str(9_000_000 + i)))
        for i in range(ri.EXPECTED_INVALID_PRODUCT_IDS + 1, 151):
            orders.append(_order_row(str(i), "1", "1"))
        orders_df = spark.createDataFrame(orders)

        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-product-test",
        )
        metrics_df = ri.compute_referential_integrity_metrics(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-product-test",
        )

        assert (
            ri.count_orphan_foreign_keys(silver_df, "orders_product_id_exists")
            == ri.EXPECTED_INVALID_PRODUCT_IDS
        )
        assert (
            _metric_failed_rows(metrics_df, "orders_product_id_exists")
            == ri.EXPECTED_INVALID_PRODUCT_IDS
        )

    def test_null_foreign_keys_are_not_ri_failures(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame([_customer_row("1")])
        products_df = spark.createDataFrame([_product_row("1")])
        orders_df = spark.createDataFrame(
            [
                _order_row("1", None, "1"),
                _order_row("2", "", "1"),
                _order_row("3", "1", None),
                _order_row("4", "1", ""),
            ],
        )

        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-null-test",
        )
        metrics_df = ri.compute_referential_integrity_metrics(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-null-test",
        )

        assert ri.count_orphan_foreign_keys(silver_df, "orders_customer_id_exists") == 0
        assert ri.count_orphan_foreign_keys(silver_df, "orders_product_id_exists") == 0
        assert _metric_failed_rows(metrics_df, "orders_customer_id_exists") == 0
        assert _metric_failed_rows(metrics_df, "orders_product_id_exists") == 0
        assert silver_df.filter(~F.col("dq_referential_pass")).count() == 0
        assert ri.count_null_foreign_keys(orders_df, "customer_id") == 2
        assert ri.count_null_foreign_keys(orders_df, "product_id") == 2

    def test_duplicate_customer_ids_still_satisfy_referential_integrity(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame(
            [_customer_row("1"), _customer_row("1")],
        )
        products_df = spark.createDataFrame([_product_row("10")])
        orders_df = spark.createDataFrame([_order_row("100", "1", "10")])

        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="ri-dup-customer-test",
        )

        assert ri.count_orphan_foreign_keys(silver_df, "orders_customer_id_exists") == 0
        assert ri.count_orphan_foreign_keys(silver_df, "orders_product_id_exists") == 0
        assert silver_df.first()["dq_referential_pass"] is True

    def test_assignment_intentional_orphan_counts_together(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame([_customer_row(str(i)) for i in range(1, 201)])
        products_df = spark.createDataFrame([_product_row(str(i)) for i in range(1, 101)])

        orders = []
        order_id = 1
        for i in range(1, ri.EXPECTED_INVALID_CUSTOMER_IDS + 1):
            orders.append(_order_row(str(order_id), str(9_000_000 + i), "1"))
            order_id += 1
        for i in range(1, ri.EXPECTED_INVALID_PRODUCT_IDS + 1):
            orders.append(_order_row(str(order_id), "1", str(9_000_000 + i)))
            order_id += 1
        for i in range(100):
            orders.append(_order_row(str(order_id), None, "1"))
            order_id += 1
        for i in range(200):
            orders.append(_order_row(str(order_id), "1", None))
            order_id += 1
        for i in range(50):
            orders.append(_order_row(str(order_id), "1", "1"))
            order_id += 1

        orders_df = spark.createDataFrame(orders)
        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="assignment-ri-test",
        )
        metrics_df = ri.compute_referential_integrity_metrics(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="assignment-ri-test",
        )

        assert (
            ri.count_orphan_foreign_keys(silver_df, "orders_customer_id_exists")
            == ri.EXPECTED_INVALID_CUSTOMER_IDS
        )
        assert (
            ri.count_orphan_foreign_keys(silver_df, "orders_product_id_exists")
            == ri.EXPECTED_INVALID_PRODUCT_IDS
        )
        assert _metric_failed_rows(metrics_df, "orders_customer_id_exists") == 50
        assert _metric_failed_rows(metrics_df, "orders_product_id_exists") == 30
        assert silver_df.count() == len(orders)

    def test_does_not_delete_invalid_rows(self, spark: SparkSession) -> None:
        customers_df = spark.createDataFrame([_customer_row("1")])
        products_df = spark.createDataFrame([_product_row("1")])
        orders_df = spark.createDataFrame(
            [_order_row("1", "999999", "1"), _order_row("2", "1", "888888")],
        )

        reference_keys = _reference_keys(spark, customers_df, products_df)
        silver_df = ri.apply_referential_integrity_checks(
            orders_df,
            reference_keys=reference_keys,
            rules=ri.ORDER_REFERENTIAL_RULES,
            run_id="no-delete",
        )

        assert orders_df.count() == silver_df.count() == 2
