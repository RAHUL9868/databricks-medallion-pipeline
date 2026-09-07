"""
Gold-layer tests.

Each test documents:
- what it validates
- expected result
- actual result (via ValidationCheck on failure)
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

TESTS_ROOT = Path(__file__).resolve().parent
SRC_ROOT = TESTS_ROOT.parent / "src"
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from config.pipeline_config import load_config
from test_support import ValidationCheck, dicts_to_bronze_dataframe


GOLD_SQL_ASSETS_PENDING: tuple[str, ...] = ()

GOLD_SQL_ASSETS_IMPLEMENTED = (
    "01_sales_by_product.sql",
    "02_revenue_by_customer.sql",
    "03_daily_weekly_trends.sql",
    "04_customer_segmentation.sql",
)


def _load_create_gold_tables():
    module_path = SRC_ROOT / "gold" / "create_gold_tables.py"
    spec = importlib.util.spec_from_file_location("create_gold_tables", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load create_gold_tables from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGoldOrchestration:
    def test_gold_steps_cover_required_tables(self) -> None:
        """
        Validates: orchestrator defines all four required Gold datasets.
        Expected: four steps with expected logical names.
        """
        cgt = _load_create_gold_tables()
        logical_names = {step.logical_name for step in cgt.GOLD_TABLE_STEPS}
        ValidationCheck(
            name="gold_step_coverage",
            validates="Gold orchestration includes all required logical datasets",
            expected={
                "sales_by_product",
                "revenue_by_customer",
                "daily_weekly_trends",
                "customer_segmentation",
            },
            actual=logical_names,
        ).assert_pass()

    def test_create_gold_tables_module_exists(self) -> None:
        """
        Validates: Gold orchestration entrypoint is present.
        Expected: src/gold/create_gold_tables.py exists.
        """
        ValidationCheck(
            name="create_gold_tables_exists",
            validates="Gold orchestrator script exists",
            expected=True,
            actual=(SRC_ROOT / "gold" / "create_gold_tables.py").exists(),
        ).assert_pass()

    def test_silver_revenue_baseline_matches_qualifying_orders(self, spark: SparkSession) -> None:
        """
        Validates: Silver revenue baseline helper sums only valid Completed orders.
        Expected: baseline revenue equals 10.00 on a three-order fixture.
        """
        cgt = _load_create_gold_tables()
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_test")

        metadata = {
            "_ingest_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "_source_file": "orders.csv",
            "_source_path": "file:///tmp/orders.csv",
            "_batch_id": "batch",
            "_corrupt_record": None,
        }
        orders = [
            Row(
                order_id="1",
                customer_id="1",
                order_date="2024-06-01",
                product_id="1",
                quantity="1",
                unit_price="10.00",
                total_amount="10.00",
                order_status="Completed",
                payment_date="2024-06-02",
                dq_is_valid=True,
                _source_row_num=1,
                **metadata,
            ),
            Row(
                order_id="2",
                customer_id="2",
                order_date="2024-06-01",
                product_id="1",
                quantity="1",
                unit_price="20.00",
                total_amount="20.00",
                order_status="Pending",
                payment_date=None,
                dq_is_valid=True,
                _source_row_num=2,
                **metadata,
            ),
            Row(
                order_id="3",
                customer_id="3",
                order_date="2024-06-01",
                product_id="1",
                quantity="1",
                unit_price="30.00",
                total_amount="30.00",
                order_status="Completed",
                payment_date="2024-06-02",
                dq_is_valid=False,
                _source_row_num=3,
                **metadata,
            ),
        ]
        customers = dicts_to_bronze_dataframe(
            spark,
            "customers",
            [
                {
                    "customer_id": "1",
                    "customer_name": "A",
                    "email": "a@x.com",
                    "country": "US",
                    "signup_date": "2024-01-01",
                    "customer_segment": "Standard",
                    "lifetime_value": "1.00",
                },
            ],
        )
        products = dicts_to_bronze_dataframe(
            spark,
            "products",
            [
                {
                    "product_id": "1",
                    "product_name": "P",
                    "category": "C",
                    "price": "10.00",
                    "cost": "5.00",
                    "stock_quantity": "1",
                    "reorder_level": "1",
                },
            ],
        )
        orders_df = spark.createDataFrame(orders)
        for table_name, df in (
            ("silver_customers", customers),
            ("silver_products", products),
            ("silver_orders", orders_df),
        ):
            df.write.format("delta").mode("overwrite").saveAsTable(f"gold_test.{table_name}")

        config = load_config(schema_name="gold_test", run_id="gold-test")
        baseline = cgt.silver_qualifying_revenue(
            spark,
            config,
            require_product_id=True,
            require_customer_id=True,
            require_order_date=True,
        )
        ValidationCheck(
            name="qualifying_revenue_baseline",
            validates="Only valid Completed order revenue is counted",
            expected=Decimal("10.00"),
            actual=baseline,
        ).assert_pass()


class TestGoldLayerReadiness:
    @pytest.mark.parametrize("sql_asset", GOLD_SQL_ASSETS_IMPLEMENTED)
    def test_gold_sql_asset_exists(self, sql_asset: str) -> None:
        """
        Validates: implemented Gold SQL assets are present in the repository.
        Expected: SQL asset file exists under src/gold/.
        """
        repo_root = Path(__file__).resolve().parents[1]
        sql_path = repo_root / "src" / "gold" / sql_asset
        ValidationCheck(
            name=f"gold_asset_exists:{sql_asset}",
            validates="Gold SQL asset file exists",
            expected=True,
            actual=sql_path.exists(),
        ).assert_pass()

    def test_all_expected_gold_assets_implemented(self) -> None:
        """
        Validates: all planned Gold SQL assets exist.
        Expected: four Gold SQL files under src/gold/.
        """
        repo_root = Path(__file__).resolve().parents[1]
        gold_dir = repo_root / "src" / "gold"
        ValidationCheck(
            name="gold_sql_file_count",
            validates="Count of implemented Gold SQL assets",
            expected=len(GOLD_SQL_ASSETS_IMPLEMENTED),
            actual=len(list(gold_dir.glob("*.sql"))),
        ).assert_pass()

    def test_gold_layer_test_suite_reserved(self) -> None:
        """
        Validates: Gold test module remains available for future revenue/filter tests.
        Expected: placeholder passes until Gold tables exist.
        """
        ValidationCheck(
            name="gold_placeholder",
            validates="Gold layer test placeholder",
            expected=True,
            actual=True,
        ).assert_pass()


class TestCustomerSegmentationSql:
    def test_segmentation_sql_documents_assumptions_and_labels(self) -> None:
        """
        Validates: segmentation SQL documents thresholds and required segment labels.
        Expected: file contains assumption markers and all four segment_type values.
        """
        sql_path = Path(__file__).resolve().parents[1] / "src" / "gold" / "04_customer_segmentation.sql"
        content = sql_path.read_text(encoding="utf-8")
        checks = [
            ValidationCheck(
                name="documents_assumption_a1",
                validates="High-Value threshold documented as engineering assumption",
                expected=True,
                actual="ASSUMPTION A1" in content,
            ),
            ValidationCheck(
                name="documents_precedence",
                validates="Segment precedence documented for non-overlapping assignment",
                expected=True,
                actual="PRECEDENCE" in content,
            ),
            ValidationCheck(
                name="includes_high_value_label",
                validates="High-Value segment label present",
                expected=True,
                actual="'High-Value'" in content,
            ),
            ValidationCheck(
                name="includes_repeat_label",
                validates="Repeat segment label present",
                expected=True,
                actual="'Repeat'" in content,
            ),
            ValidationCheck(
                name="includes_one_time_label",
                validates="One-Time segment label present",
                expected=True,
                actual="'One-Time'" in content,
            ),
            ValidationCheck(
                name="includes_inactive_label",
                validates="Inactive segment label present",
                expected=True,
                actual="'Inactive'" in content,
            ),
        ]
        failures = [check for check in checks if not check.passed]
        if failures:
            details = "\n".join(str(check) for check in failures)
            raise AssertionError(f"{len(failures)} validation check(s) failed:\n{details}")
