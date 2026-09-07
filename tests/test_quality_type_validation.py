"""Tests for Silver type and domain validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _load_type_validation_module():
    module_path = SRC_ROOT / "silver" / "03_quality_type_validation.py"
    spec = importlib.util.spec_from_file_location("quality_type_validation", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load type validation module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


qtv = _load_type_validation_module()


def _metric_failed_rows(metrics_df, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    assert row is not None, f"Metric row not found for rule_id={rule_id}"
    return int(row["failed_rows"])


def _valid_customer_row(**overrides):
    base = {
        "customer_id": "1",
        "customer_name": "Customer 1",
        "email": "user1@example.com",
        "country": "United States",
        "signup_date": "2024-01-01",
        "customer_segment": "Standard",
        "lifetime_value": "100.00",
    }
    base.update(overrides)
    return Row(**base)


def _valid_order_row(**overrides):
    base = {
        "order_id": "1",
        "customer_id": "10",
        "order_date": "2024-06-01",
        "product_id": "5",
        "quantity": "2",
        "unit_price": "10.00",
        "total_amount": "20.00",
        "order_status": "Completed",
        "payment_date": "2024-06-02",
    }
    base.update(overrides)
    return Row(**base)


def _valid_product_row(**overrides):
    base = {
        "product_id": "1",
        "product_name": "Widget",
        "category": "Electronics",
        "price": "19.99",
        "cost": "8.50",
        "stock_quantity": "100",
        "reorder_level": "10",
    }
    base.update(overrides)
    return Row(**base)


class TestCustomerTypeValidation:
    def test_invalid_customer_segment(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_customer_row(customer_segment="Gold"),
                _valid_customer_row(customer_id="2", customer_segment="Standard"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="test-run",
        )
        metrics_df = qtv.compute_type_validation_metrics(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="test-run",
        )

        assert silver_df.count() == 2
        assert qtv.count_rule_failures(silver_df, "customers_segment_valid") == 1
        assert _metric_failed_rows(metrics_df, "customers_segment_valid") == 1
        assert silver_df.filter(~F.col("dq_type_business_pass")).count() == 1

    def test_invalid_signup_date(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_customer_row(signup_date="2024-13-45"),
                _valid_customer_row(customer_id="2", signup_date="2099-01-01"),
                _valid_customer_row(customer_id="3"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="test-run",
        )
        metrics_df = qtv.compute_type_validation_metrics(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "customers_signup_date_valid") == 2
        assert _metric_failed_rows(metrics_df, "customers_signup_date_valid") == 2

    def test_invalid_customer_id_and_lifetime_value(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_customer_row(customer_id="abc"),
                _valid_customer_row(customer_id="2", lifetime_value="-5.00"),
                _valid_customer_row(customer_id="3"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "customers_customer_id_valid") == 1
        assert qtv.count_rule_failures(silver_df, "customers_lifetime_value_valid") == 1
        assert silver_df.filter(F.col("dq_failure_count") >= 2).count() == 1


class TestOrderTypeValidation:
    def test_invalid_order_status_and_quantity(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(order_status="Shipped"),
                _valid_order_row(order_id="2", quantity="0"),
                _valid_order_row(order_id="3", quantity="-1"),
                _valid_order_row(order_id="4"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )
        metrics_df = qtv.compute_type_validation_metrics(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "orders_status_valid") == 1
        assert qtv.count_rule_failures(silver_df, "orders_quantity_positive") == 2
        assert _metric_failed_rows(metrics_df, "orders_status_valid") == 1
        assert _metric_failed_rows(metrics_df, "orders_quantity_positive") == 2

    def test_invalid_prices_and_order_date(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(unit_price="-1.00"),
                _valid_order_row(order_id="2", total_amount="not-a-number"),
                _valid_order_row(order_id="3", order_date="bad-date"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "orders_unit_price_non_negative") == 1
        assert qtv.count_rule_failures(silver_df, "orders_total_amount_non_negative") == 1
        assert qtv.count_rule_failures(silver_df, "orders_order_date_valid") == 1

    def test_null_foreign_keys_skipped_for_type_validation(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(customer_id=None, product_id=None),
                _valid_order_row(order_id="2", customer_id="10", product_id="5"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "orders_customer_id_valid") == 0
        assert qtv.count_rule_failures(silver_df, "orders_product_id_valid") == 0

    def test_invalid_foreign_keys_flagged_when_present(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(customer_id="abc", product_id="xyz"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "orders_customer_id_valid") == 1
        assert qtv.count_rule_failures(silver_df, "orders_product_id_valid") == 1

    def test_payment_date_optional_and_invalid_when_present(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(payment_date=None),
                _valid_order_row(order_id="2", payment_date="2024-06-01"),
                _valid_order_row(order_id="3", payment_date="not-a-date"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "orders_payment_date_valid") == 1


class TestProductTypeValidation:
    def test_invalid_product_numeric_fields(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_product_row(price="-1.00"),
                _valid_product_row(product_id="2", cost="bad"),
                _valid_product_row(product_id="3", stock_quantity="-5"),
                _valid_product_row(product_id="4", reorder_level="x"),
                _valid_product_row(product_id="5"),
            ],
        )

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="products",
            rules=qtv.PRODUCT_TYPE_RULES,
            run_id="test-run",
        )
        metrics_df = qtv.compute_type_validation_metrics(
            bronze_df,
            entity="products",
            rules=qtv.PRODUCT_TYPE_RULES,
            run_id="test-run",
        )

        assert silver_df.count() == 5
        assert qtv.count_rule_failures(silver_df, "products_price_non_negative") == 1
        assert qtv.count_rule_failures(silver_df, "products_cost_non_negative") == 1
        assert qtv.count_rule_failures(silver_df, "products_stock_quantity_non_negative") == 1
        assert qtv.count_rule_failures(silver_df, "products_reorder_level_non_negative") == 1
        assert _metric_failed_rows(metrics_df, "products_price_non_negative") == 1

    def test_invalid_product_id(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame([_valid_product_row(product_id="P-100")])

        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="products",
            rules=qtv.PRODUCT_TYPE_RULES,
            run_id="test-run",
        )

        assert qtv.count_rule_failures(silver_df, "products_product_id_valid") == 1


class TestTypeValidationIntegration:
    def test_does_not_delete_failed_rows(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(order_status="Bad"),
                _valid_order_row(order_id="2", quantity="-1"),
            ],
        )
        silver_df = qtv.apply_type_validation_checks(
            bronze_df,
            entity="orders",
            rules=qtv.ORDER_TYPE_RULES,
            run_id="no-delete-test",
        )
        assert bronze_df.count() == silver_df.count() == 2

    def test_merges_with_prior_dq_columns(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame([_valid_customer_row(customer_segment="Gold")])
        with_prior = (
            bronze_df.withColumn("dq_failed_rules", F.array(F.lit("customers_email_not_null")))
            .withColumn("dq_completeness_pass", F.lit(False))
            .withColumn("dq_uniqueness_pass", F.lit(True))
            .withColumn("dq_completeness_errors", F.array(F.lit("email missing")))
            .withColumn("dq_uniqueness_errors", F.array().cast("array<string>"))
        )

        silver_df = qtv.apply_type_validation_checks(
            with_prior,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="merge-test",
        )
        row = silver_df.first()
        assert "customers_email_not_null" in row["dq_failed_rules"]
        assert "customers_segment_valid" in row["dq_failed_rules"]
        assert row["dq_failure_count"] == 2
        assert row["dq_is_valid"] is False

    def test_metrics_use_type_business_category(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame([_valid_customer_row(customer_segment="Gold")])
        metrics_df = qtv.compute_type_validation_metrics(
            bronze_df,
            entity="customers",
            rules=qtv.CUSTOMER_TYPE_RULES,
            run_id="category-test",
        )
        assert metrics_df.filter(F.col("check_category") == "type_business").count() == len(
            qtv.CUSTOMER_TYPE_RULES,
        )
