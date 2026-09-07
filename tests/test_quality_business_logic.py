"""Tests for Silver business-logic validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _load_business_logic_module():
    module_path = SRC_ROOT / "silver" / "05_quality_business_logic.py"
    spec = importlib.util.spec_from_file_location("quality_business_logic", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load business-logic module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


qbl = _load_business_logic_module()


def _metric_failed_rows(metrics_df, rule_id: str) -> int:
    row = metrics_df.filter(F.col("rule_id") == rule_id).first()
    assert row is not None, f"Metric row not found for rule_id={rule_id}"
    return int(row["failed_rows"])


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


class TestAssignmentRequiredOrderRules:
    """Rules from data-quality-strategy.md CHECK-B01 through CHECK-B03."""

    def test_total_amount_mismatch_flagged(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(total_amount="25.00"),
                _valid_order_row(order_id="2"),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )
        metrics_df = qbl.compute_business_logic_metrics(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "orders_total_amount_matches") == 1
        assert _metric_failed_rows(metrics_df, "orders_total_amount_matches") == 1
        rule = next(r for r in qbl.ORDER_BUSINESS_RULES if r.rule_id == "orders_total_amount_matches")
        assert rule.scope == qbl.RuleScope.ASSIGNMENT_REQUIRED

    def test_total_amount_within_tolerance_passes(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(quantity="3", unit_price="10.00", total_amount="30.00"),
                _valid_order_row(order_id="2", quantity="1", unit_price="10.00", total_amount="10.00"),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "orders_total_amount_matches") == 0

    def test_total_amount_check_skipped_when_operands_unparseable(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(quantity="abc", unit_price="10.00", total_amount="99.00"),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "orders_total_amount_matches") == 0

    def test_completed_order_missing_payment_date(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(order_status="Completed", payment_date=None),
                _valid_order_row(order_id="2", order_status="Pending", payment_date=None),
                _valid_order_row(order_id="3", order_status="Cancelled", payment_date=None),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )
        metrics_df = qbl.compute_business_logic_metrics(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "orders_completed_has_payment_date") == 1
        assert _metric_failed_rows(metrics_df, "orders_completed_has_payment_date") == 1

    def test_payment_date_before_order_date(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(order_date="2024-06-10", payment_date="2024-06-01"),
                _valid_order_row(order_id="2", order_date="2024-06-01", payment_date="2024-06-01"),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "orders_payment_date_after_order_date") == 1


class TestEngineeringAssumptionProductRules:
    def test_cost_exceeds_price_flagged(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_product_row(price="10.00", cost="12.00"),
                _valid_product_row(product_id="2", price="10.00", cost="10.00"),
                _valid_product_row(product_id="3", price="10.00", cost="9.99"),
            ],
        )

        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="products",
            rules=qbl.PRODUCT_BUSINESS_RULES,
            run_id="test-run",
        )
        metrics_df = qbl.compute_business_logic_metrics(
            bronze_df,
            entity="products",
            rules=qbl.PRODUCT_BUSINESS_RULES,
            run_id="test-run",
        )

        assert qbl.count_rule_failures(silver_df, "products_cost_not_exceeds_price") == 1
        assert _metric_failed_rows(metrics_df, "products_cost_not_exceeds_price") == 1
        rule = next(r for r in qbl.PRODUCT_BUSINESS_RULES if r.rule_id == "products_cost_not_exceeds_price")
        assert rule.scope == qbl.RuleScope.ENGINEERING_ASSUMPTION
        description = metrics_df.filter(
            F.col("rule_id") == "products_cost_not_exceeds_price",
        ).first()["rule_description"]
        assert "engineering_assumption" in description


class TestTypeRulesOwnedByModule03:
    """User-listed single-column rules are enforced in 03, not duplicated here."""

    def test_module_03_owns_quantity_and_price_rules(self) -> None:
        assert "orders_quantity_positive" in qbl.TYPE_RULES_OWNED_BY_MODULE_03
        assert "orders_unit_price_non_negative" in qbl.TYPE_RULES_OWNED_BY_MODULE_03
        assert "orders_status_valid" in qbl.TYPE_RULES_OWNED_BY_MODULE_03
        assert "customers_signup_date_valid" in qbl.TYPE_RULES_OWNED_BY_MODULE_03

    def test_business_module_does_not_define_duplicate_rule_ids(self) -> None:
        business_rule_ids = {rule.rule_id for rule in qbl.ORDER_BUSINESS_RULES}
        business_rule_ids.update(rule.rule_id for rule in qbl.PRODUCT_BUSINESS_RULES)
        overlap = business_rule_ids.intersection(qbl.TYPE_RULES_OWNED_BY_MODULE_03)
        assert overlap == set()


class TestBusinessLogicIntegration:
    def test_does_not_modify_source_values(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [_valid_order_row(total_amount="25.00")],
        )
        before = bronze_df.collect()[0].asDict()
        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="no-modify-test",
        )
        after = silver_df.select(
            "order_id",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "payment_date",
        ).collect()[0].asDict()
        assert before == after

    def test_does_not_delete_failed_rows(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame(
            [
                _valid_order_row(total_amount="99.00"),
                _valid_order_row(order_id="2", order_status="Completed", payment_date=None),
            ],
        )
        silver_df = qbl.apply_business_logic_checks(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="no-delete-test",
        )
        assert bronze_df.count() == silver_df.count() == 2

    def test_merges_with_prior_dq_columns(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame([_valid_order_row(total_amount="99.00")])
        with_prior = (
            bronze_df.withColumn("dq_failed_rules", F.array(F.lit("orders_order_id_unique")))
            .withColumn("dq_completeness_pass", F.lit(True))
            .withColumn("dq_uniqueness_pass", F.lit(False))
            .withColumn("dq_type_business_pass", F.lit(True))
            .withColumn("dq_referential_pass", F.lit(True))
            .withColumn("dq_completeness_errors", F.array().cast("array<string>"))
            .withColumn("dq_uniqueness_errors", F.array(F.lit("duplicate order_id")))
            .withColumn("dq_type_business_errors", F.array().cast("array<string>"))
            .withColumn("dq_referential_errors", F.array().cast("array<string>"))
        )

        silver_df = qbl.apply_business_logic_checks(
            with_prior,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="merge-test",
        )
        row = silver_df.first()
        assert "orders_order_id_unique" in row["dq_failed_rules"]
        assert "orders_total_amount_matches" in row["dq_failed_rules"]
        assert row["dq_failure_count"] == 2
        assert row["dq_business_logic_pass"] is False
        assert row["dq_type_business_pass"] is False
        assert row["dq_is_valid"] is False
        assert len(row["dq_business_logic_errors"]) >= 1

    def test_metrics_use_type_business_category(self, spark: SparkSession) -> None:
        bronze_df = spark.createDataFrame([_valid_order_row(total_amount="99.00")])
        metrics_df = qbl.compute_business_logic_metrics(
            bronze_df,
            entity="orders",
            rules=qbl.ORDER_BUSINESS_RULES,
            run_id="category-test",
        )
        assert metrics_df.filter(F.col("check_category") == "type_business").count() == len(
            qbl.ORDER_BUSINESS_RULES,
        )
