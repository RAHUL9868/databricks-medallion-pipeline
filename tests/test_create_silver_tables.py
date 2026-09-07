"""Tests for consolidated Silver table creation."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _load_create_silver_module():
    module_path = SRC_ROOT / "silver" / "create_silver_tables.py"
    spec = importlib.util.spec_from_file_location("create_silver_tables", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load create_silver_tables from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cst = _load_create_silver_module()

BRONZE_METADATA = {
    "_ingest_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "_source_file": "test.csv",
    "_source_path": "dbfs:/test/test.csv",
    "_batch_id": "batch-001",
    "_source_row_num": 1,
    "_corrupt_record": None,
}


def _customer_row(**overrides):
    base = {
        "customer_id": "1",
        "customer_name": "Customer One",
        "email": "user1@example.com",
        "country": "United States",
        "signup_date": "2024-01-01",
        "customer_segment": "Standard",
        "lifetime_value": "100.00",
        **BRONZE_METADATA,
    }
    base.update(overrides)
    return Row(**base)


def _order_row(**overrides):
    base = {
        "order_id": "1",
        "customer_id": "1",
        "order_date": "2024-06-01",
        "product_id": "1",
        "quantity": "2",
        "unit_price": "10.00",
        "total_amount": "20.00",
        "order_status": "Completed",
        "payment_date": "2024-06-02",
        **BRONZE_METADATA,
    }
    base.update(overrides)
    return Row(**base)


def _product_row(**overrides):
    base = {
        "product_id": "1",
        "product_name": "Widget",
        "category": "Electronics",
        "price": "19.99",
        "cost": "8.50",
        "stock_quantity": "100",
        "reorder_level": "10",
        **BRONZE_METADATA,
    }
    base.update(overrides)
    return Row(**base)


def _bronze_frames(spark: SparkSession):
    customers = spark.createDataFrame(
        [
            _customer_row(),
            _customer_row(
                customer_id="2",
                email=None,
                _source_row_num=2,
            ),
            _customer_row(
                customer_id="2",
                customer_name="Duplicate Customer",
                email="dup@example.com",
                _source_row_num=3,
            ),
        ],
    )
    products = spark.createDataFrame([_product_row()])
    orders = spark.createDataFrame(
        [
            _order_row(),
            _order_row(
                order_id="2",
                customer_id=None,
                product_id=None,
                total_amount="10.00",
                quantity="1",
                unit_price="10.00",
                _source_row_num=2,
            ),
            _order_row(
                order_id="3",
                total_amount="99.00",
                _source_row_num=3,
            ),
            _order_row(
                order_id="4",
                order_status="Completed",
                payment_date=None,
                _source_row_num=4,
            ),
        ],
    )
    return customers, orders, products


class TestConsolidateQualityModel:
    def test_recomputes_category_flags_from_failed_rules(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                Row(
                    customer_id="1",
                    dq_failed_rules=["customers_email_not_null", "customers_segment_valid"],
                    dq_completeness_pass=True,
                    dq_type_business_pass=True,
                ),
            ],
        )
        result = cst.consolidate_quality_model(df, "customers", "test-run")
        row = result.first()
        assert row["dq_completeness_pass"] is False
        assert row["dq_type_business_pass"] is False
        assert row["dq_is_valid"] is False
        assert row["dq_record_status"] == "invalid_multiple"
        assert row["dq_failure_count"] == 2

    def test_valid_record_status(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([Row(customer_id="1", dq_failed_rules=[])])
        row = cst.consolidate_quality_model(df, "customers", "test-run").first()
        assert row["dq_record_status"] == "valid"
        assert row["dq_is_valid"] is True

    def test_single_failure_status(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [Row(customer_id="1", dq_failed_rules=["customers_email_not_null"])],
        )
        row = cst.consolidate_quality_model(df, "customers", "test-run").first()
        assert row["dq_record_status"] == "invalid"


class TestFinalizeSilverSchema:
    def test_preserves_source_traceability_and_casts_types(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_customer_row(customer_id="42", lifetime_value="12.50")])
        result = cst.finalize_silver_schema(df, "customers")
        row = result.first()
        assert row["_source_customer_id"] == "42"
        assert row["customer_id"] == 42
        assert str(row["lifetime_value"]) == "12.50"
        assert row["_batch_id"] == "batch-001"


class TestQualityReport:
    def test_report_schema(self, spark: SparkSession) -> None:
        metrics = spark.createDataFrame(
            [
                Row(
                    run_id="run-1",
                    entity="orders",
                    check_category="completeness",
                    rule_id="orders_customer_id_not_null",
                    rule_description="desc",
                    total_rows=100,
                    failed_rows=5,
                    passed_rows=95,
                    pass_pct=95.0,
                    fail_pct=5.0,
                    evaluated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
                ),
            ],
        )
        report = cst.build_quality_report(metrics)
        columns = set(report.columns)
        assert columns == {
            "dataset",
            "check_name",
            "total_records",
            "passed_records",
            "failed_records",
            "pass_percentage",
            "execution_timestamp",
        }
        row = report.first()
        assert row["dataset"] == "orders"
        assert row["check_name"] == "orders_customer_id_not_null"
        assert row["failed_records"] == 5


class TestApplyQualityPipeline:
    def test_row_count_invariant(self, spark: SparkSession) -> None:
        customers, orders, products = _bronze_frames(spark)
        result = cst.apply_quality_pipeline(
            spark,
            customers,
            orders,
            products,
            run_id="pipeline-test",
        )
        assert result["customers"].count() == customers.count()
        assert result["orders"].count() == orders.count()
        assert result["products"].count() == products.count()

    def test_detects_known_failure_patterns(self, spark: SparkSession) -> None:
        customers, orders, products = _bronze_frames(spark)
        result = cst.apply_quality_pipeline(
            spark,
            customers,
            orders,
            products,
            run_id="pipeline-test",
        )

        customer_silver = result["customers"]
        order_silver = result["orders"]

        assert customer_silver.filter(F.col("dq_record_status") == "invalid").count() >= 1
        assert customer_silver.filter(F.col("dq_record_status") == "invalid_multiple").count() >= 1
        assert order_silver.filter(F.array_contains(F.col("dq_failed_rules"), "orders_customer_id_not_null")).count() == 1
        assert order_silver.filter(F.array_contains(F.col("dq_failed_rules"), "orders_product_id_not_null")).count() == 1
        assert order_silver.filter(F.array_contains(F.col("dq_failed_rules"), "orders_total_amount_matches")).count() == 1
        assert order_silver.filter(F.array_contains(F.col("dq_failed_rules"), "orders_completed_has_payment_date")).count() == 1

    def test_consolidated_flags_are_consistent(self, spark: SparkSession) -> None:
        customers, orders, products = _bronze_frames(spark)
        result = cst.apply_quality_pipeline(
            spark,
            customers,
            orders,
            products,
            run_id="consistency-test",
        )
        inconsistent = result["orders"].filter(
            (F.col("dq_is_valid") & (F.col("dq_failure_count") > 0))
            | (~F.col("dq_is_valid") & (F.col("dq_failure_count") == 0)),
        )
        assert inconsistent.count() == 0

    def test_collects_metrics_for_all_rule_groups(self, spark: SparkSession) -> None:
        customers, orders, products = _bronze_frames(spark)
        pipeline = cst.apply_quality_pipeline(
            spark,
            customers,
            orders,
            products,
            run_id="metrics-test",
        )
        from config.pipeline_config import load_config

        config = load_config(run_id="metrics-test")
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
            "metrics-test",
            pipeline["_qc"],
            pipeline["_qu"],
            pipeline["_qtv"],
            pipeline["_ri"],
            pipeline["_qbl"],
        )
        rule_ids = {row["rule_id"] for row in metrics.collect()}
        assert "customers_email_not_null" in rule_ids
        assert "orders_customer_id_not_null" in rule_ids
        assert "orders_total_amount_matches" in rule_ids
        assert "products_cost_not_exceeds_price" in rule_ids

        report = cst.build_quality_report(metrics)
        assert report.count() == metrics.count()

    def test_invalid_rows_retained_in_primary_silver_tables(self, spark: SparkSession) -> None:
        customers, orders, products = _bronze_frames(spark)
        result = cst.apply_quality_pipeline(
            spark,
            customers,
            orders,
            products,
            run_id="audit-test",
        )
        invalid_orders = result["orders"].filter(~F.col("dq_is_valid"))
        assert invalid_orders.count() > 0
        assert invalid_orders.filter(F.col("_source_order_id").isNotNull()).count() == invalid_orders.count()
