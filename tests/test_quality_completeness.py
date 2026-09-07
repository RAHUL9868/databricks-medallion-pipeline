"""Tests for Silver completeness validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _load_completeness_module():
    module_path = SRC_ROOT / "silver" / "01_quality_completeness.py"
    spec = importlib.util.spec_from_file_location("quality_completeness", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load completeness module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


qc = _load_completeness_module()


def _metric_failed_rows(metrics_df, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    assert row is not None, f"Metric row not found for rule_id={rule_id}"
    return int(row["failed_rows"])


def _build_customers_df(spark: SparkSession, null_email_count: int, total_rows: int):
    rows = []
    for i in range(1, total_rows + 1):
        email = None if i <= null_email_count else f"user{i}@example.com"
        rows.append(
            Row(
                customer_id=str(i),
                customer_name=f"Customer {i}",
                email=email,
                country="United States",
                signup_date="2024-01-01",
                customer_segment="Standard",
                lifetime_value="100.00",
            ),
        )
    return spark.createDataFrame(rows)


def _build_orders_df(
    spark: SparkSession,
    null_customer_id_count: int,
    null_product_id_count: int,
    total_rows: int,
):
    rows = []
    for i in range(1, total_rows + 1):
        customer_id = None if i <= null_customer_id_count else str((i % 100) + 1)
        if i <= null_product_id_count:
            product_id = None
        else:
            product_id = str((i % 50) + 1)
        rows.append(
            Row(
                order_id=str(i),
                customer_id=customer_id,
                order_date="2024-06-01",
                product_id=product_id,
                quantity="1",
                unit_price="10.00",
                total_amount="10.00",
                order_status="Completed",
                payment_date="2024-06-02",
            ),
        )
    return spark.createDataFrame(rows)


class TestCompletenessValidation:
    EXPECTED_NULL_EMAILS = 50
    EXPECTED_NULL_CUSTOMER_IDS = 100
    EXPECTED_NULL_PRODUCT_IDS = 200

    def test_detects_null_customer_emails(self, spark: SparkSession) -> None:
        total_rows = 1_000
        bronze_df = _build_customers_df(
            spark,
            null_email_count=self.EXPECTED_NULL_EMAILS,
            total_rows=total_rows,
        )

        silver_df = qc.apply_completeness_checks(
            bronze_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="test-run",
        )
        metrics_df = qc.compute_completeness_metrics(
            bronze_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="test-run",
        )

        assert silver_df.count() == total_rows
        assert qc.count_rule_failures(silver_df, "customers_email_not_null") == self.EXPECTED_NULL_EMAILS
        assert _metric_failed_rows(metrics_df, "customers_email_not_null") == self.EXPECTED_NULL_EMAILS
        assert silver_df.filter(~F.col("dq_completeness_pass")).count() == self.EXPECTED_NULL_EMAILS

    def test_detects_null_order_customer_ids(self, spark: SparkSession) -> None:
        total_rows = 2_000
        bronze_df = _build_orders_df(
            spark,
            null_customer_id_count=self.EXPECTED_NULL_CUSTOMER_IDS,
            null_product_id_count=0,
            total_rows=total_rows,
        )

        silver_df = qc.apply_completeness_checks(
            bronze_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="test-run",
        )
        metrics_df = qc.compute_completeness_metrics(
            bronze_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="test-run",
        )

        assert silver_df.count() == total_rows
        assert (
            qc.count_rule_failures(silver_df, "orders_customer_id_not_null")
            == self.EXPECTED_NULL_CUSTOMER_IDS
        )
        assert (
            _metric_failed_rows(metrics_df, "orders_customer_id_not_null")
            == self.EXPECTED_NULL_CUSTOMER_IDS
        )

    def test_detects_null_order_product_ids(self, spark: SparkSession) -> None:
        total_rows = 2_500
        bronze_df = _build_orders_df(
            spark,
            null_customer_id_count=0,
            null_product_id_count=self.EXPECTED_NULL_PRODUCT_IDS,
            total_rows=total_rows,
        )

        silver_df = qc.apply_completeness_checks(
            bronze_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="test-run",
        )
        metrics_df = qc.compute_completeness_metrics(
            bronze_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="test-run",
        )

        assert silver_df.count() == total_rows
        assert (
            qc.count_rule_failures(silver_df, "orders_product_id_not_null")
            == self.EXPECTED_NULL_PRODUCT_IDS
        )
        assert (
            _metric_failed_rows(metrics_df, "orders_product_id_not_null")
            == self.EXPECTED_NULL_PRODUCT_IDS
        )

    def test_assignment_intentional_issue_counts_together(self, spark: SparkSession) -> None:
        """Regression guard using assignment issue counts in one combined scenario."""
        customers_df = _build_customers_df(
            spark,
            null_email_count=self.EXPECTED_NULL_EMAILS,
            total_rows=10_000,
        )
        orders_df = _build_orders_df(
            spark,
            null_customer_id_count=self.EXPECTED_NULL_CUSTOMER_IDS,
            null_product_id_count=self.EXPECTED_NULL_PRODUCT_IDS,
            total_rows=100_000,
        )

        customer_silver = qc.apply_completeness_checks(
            customers_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="assignment-test",
        )
        order_silver = qc.apply_completeness_checks(
            orders_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="assignment-test",
        )
        customer_metrics = qc.compute_completeness_metrics(
            customers_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="assignment-test",
        )
        order_metrics = qc.compute_completeness_metrics(
            orders_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="assignment-test",
        )

        assert qc.count_rule_failures(customer_silver, "customers_email_not_null") == 50
        assert qc.count_rule_failures(order_silver, "orders_customer_id_not_null") == 100
        assert qc.count_rule_failures(order_silver, "orders_product_id_not_null") == 200
        assert _metric_failed_rows(customer_metrics, "customers_email_not_null") == 50
        assert _metric_failed_rows(order_metrics, "orders_customer_id_not_null") == 100
        assert _metric_failed_rows(order_metrics, "orders_product_id_not_null") == 200

    def test_blank_strings_treated_as_missing(self, spark: SparkSession) -> None:
        rows = [
            Row(
                customer_id="1",
                customer_name="A",
                email="   ",
                country="US",
                signup_date="2024-01-01",
                customer_segment="Basic",
                lifetime_value="10.00",
            ),
        ]
        bronze_df = spark.createDataFrame(rows)
        silver_df = qc.apply_completeness_checks(
            bronze_df,
            entity="customers",
            rules=qc.CUSTOMER_COMPLETENESS_RULES,
            run_id="blank-test",
        )
        assert qc.count_rule_failures(silver_df, "customers_email_not_null") == 1
        errors = silver_df.select("dq_completeness_errors").first()[0]
        assert "customers_email_not_null" in errors[0]

    def test_does_not_delete_failed_rows(self, spark: SparkSession) -> None:
        bronze_df = _build_orders_df(
            spark,
            null_customer_id_count=5,
            null_product_id_count=7,
            total_rows=100,
        )
        silver_df = qc.apply_completeness_checks(
            bronze_df,
            entity="orders",
            rules=qc.ORDER_COMPLETENESS_RULES,
            run_id="no-delete-test",
        )
        assert bronze_df.count() == silver_df.count() == 100
