"""Tests for Silver uniqueness validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _load_uniqueness_module():
    module_path = SRC_ROOT / "silver" / "02_quality_uniqueness.py"
    spec = importlib.util.spec_from_file_location("quality_uniqueness", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load uniqueness module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_completeness_module():
    module_path = SRC_ROOT / "silver" / "01_quality_completeness.py"
    spec = importlib.util.spec_from_file_location("quality_completeness", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load completeness module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


uq = _load_uniqueness_module()
qc = _load_completeness_module()


def _metric_failed_rows(metrics_df, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    assert row is not None, f"Metric row not found for rule_id={rule_id}"
    return int(row["failed_rows"])


def _customer_row(customer_id: str, suffix: str = "") -> Row:
    return Row(
        customer_id=customer_id,
        customer_name=f"Customer {customer_id}{suffix}",
        email=f"user{customer_id}@example.com",
        country="United States",
        signup_date="2024-01-01",
        customer_segment="Standard",
        lifetime_value="100.00",
    )


def _order_row(order_id: str, suffix: str = "") -> Row:
    return Row(
        order_id=order_id,
        customer_id="1",
        order_date="2024-06-01",
        product_id="10",
        quantity="1",
        unit_price="10.00",
        total_amount="10.00",
        order_status="Completed",
        payment_date="2024-06-02",
    )


class TestUniquenessValidation:
    def test_detects_duplicate_customer_ids_flags_all_group_members(self, spark: SparkSession) -> None:
        """
        Assignment pattern: 10 duplicate customer_id values -> 20 rows in duplicate groups.
        Both rows in each duplicate group must be flagged.
        """
        duplicate_key_count = 10
        rows_per_key = 2
        rows = []
        for key in range(1, duplicate_key_count + 1):
            key_str = str(key)
            rows.append(_customer_row(key_str, suffix="-a"))
            rows.append(_customer_row(key_str, suffix="-b"))
        for key in range(duplicate_key_count + 1, 101):
            rows.append(_customer_row(str(key)))

        bronze_df = spark.createDataFrame(rows)
        silver_df = uq.apply_uniqueness_checks(
            bronze_df,
            entity="customers",
            rules=uq.CUSTOMER_UNIQUENESS_RULES,
            run_id="test-customers",
        )
        metrics_df = uq.compute_uniqueness_metrics(
            bronze_df,
            entity="customers",
            rules=uq.CUSTOMER_UNIQUENESS_RULES,
            run_id="test-customers",
        )

        expected_duplicate_rows = duplicate_key_count * rows_per_key
        assert silver_df.count() == len(rows)
        assert (
            uq.count_duplicate_rows(silver_df, "customers_customer_id_unique")
            == expected_duplicate_rows
        )
        assert (
            _metric_failed_rows(metrics_df, "customers_customer_id_unique")
            == expected_duplicate_rows
        )
        assert uq.count_distinct_duplicate_keys(silver_df, "customer_id") == duplicate_key_count
        assert silver_df.filter(~F.col("dq_uniqueness_pass")).count() == expected_duplicate_rows

    def test_detects_duplicate_order_ids_flags_all_group_members(self, spark: SparkSession) -> None:
        """20 duplicate order_id records -> 40 rows participating in duplicate groups."""
        duplicate_key_count = 20
        rows_per_key = 2
        rows = []
        for key in range(1, duplicate_key_count + 1):
            key_str = str(key)
            rows.append(_order_row(key_str, suffix="-a"))
            rows.append(_order_row(key_str, suffix="-b"))
        for key in range(duplicate_key_count + 1, 201):
            rows.append(_order_row(str(key)))

        bronze_df = spark.createDataFrame(rows)
        silver_df = uq.apply_uniqueness_checks(
            bronze_df,
            entity="orders",
            rules=uq.ORDER_UNIQUENESS_RULES,
            run_id="test-orders",
        )
        metrics_df = uq.compute_uniqueness_metrics(
            bronze_df,
            entity="orders",
            rules=uq.ORDER_UNIQUENESS_RULES,
            run_id="test-orders",
        )

        expected_duplicate_rows = duplicate_key_count * rows_per_key
        assert silver_df.count() == len(rows)
        assert (
            uq.count_duplicate_rows(silver_df, "orders_order_id_unique")
            == expected_duplicate_rows
        )
        assert (
            _metric_failed_rows(metrics_df, "orders_order_id_unique")
            == expected_duplicate_rows
        )
        assert uq.count_distinct_duplicate_keys(silver_df, "order_id") == duplicate_key_count

    def test_null_keys_are_not_flagged_as_duplicates(self, spark: SparkSession) -> None:
        """Multiple NULL customer_id rows must not fail uniqueness (completeness handles NULLs)."""
        rows = [
            _order_row("1"),
            Row(
                order_id="2",
                customer_id=None,
                order_date="2024-06-01",
                product_id="10",
                quantity="1",
                unit_price="10.00",
                total_amount="10.00",
                order_status="Completed",
                payment_date="2024-06-02",
            ),
            Row(
                order_id="3",
                customer_id="",
                order_date="2024-06-01",
                product_id="10",
                quantity="1",
                unit_price="10.00",
                total_amount="10.00",
                order_status="Completed",
                payment_date="2024-06-02",
            ),
        ]
        bronze_df = spark.createDataFrame(rows)
        silver_df = uq.apply_uniqueness_checks(
            bronze_df,
            entity="orders",
            rules=uq.ORDER_UNIQUENESS_RULES,
            run_id="null-key-test",
        )

        assert uq.count_duplicate_rows(silver_df, "orders_order_id_unique") == 0
        assert silver_df.filter(~F.col("dq_uniqueness_pass")).count() == 0

    def test_merges_with_existing_completeness_failures(self, spark: SparkSession) -> None:
        rows = [
            Row(
                customer_id="1",
                customer_name="A",
                email=None,
                country="US",
                signup_date="2024-01-01",
                customer_segment="Basic",
                lifetime_value="10.00",
            ),
            Row(
                customer_id="1",
                customer_name="B",
                email="b@example.com",
                country="US",
                signup_date="2024-01-01",
                customer_segment="Basic",
                lifetime_value="10.00",
            ),
        ]
        bronze_df = spark.createDataFrame(rows)
        after_completeness = qc.apply_completeness_checks(
            bronze_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="merge-test",
        )
        after_uniqueness = uq.apply_uniqueness_checks(
            after_completeness,
            entity="customers",
            rules=uq.CUSTOMER_UNIQUENESS_RULES,
            run_id="merge-test",
        )

        row_a = after_uniqueness.filter(F.col("customer_name") == "A").first()
        row_b = after_uniqueness.filter(F.col("customer_name") == "B").first()

        assert "customers_email_not_null" in row_a["dq_failed_rules"]
        assert "customers_customer_id_unique" in row_a["dq_failed_rules"]
        assert "customers_customer_id_unique" in row_b["dq_failed_rules"]
        assert "customers_email_not_null" not in row_b["dq_failed_rules"]
        assert row_a["dq_is_valid"] is False
        assert row_b["dq_is_valid"] is False

    def test_does_not_delete_duplicate_rows(self, spark: SparkSession) -> None:
        rows = [_customer_row("1"), _customer_row("1"), _customer_row("2")]
        bronze_df = spark.createDataFrame(rows)
        silver_df = uq.apply_uniqueness_checks(
            bronze_df,
            entity="customers",
            rules=uq.CUSTOMER_UNIQUENESS_RULES,
            run_id="no-delete",
        )
        assert bronze_df.count() == silver_df.count() == 3
