"""
Silver-layer quality test suite.

Verifies intentional assignment data-quality issues are detected with correct counts,
quality status columns are populated, errors are traceable, metrics match row flags,
and naturally valid records are not incorrectly flagged.

Each test documents:
- what it validates
- expected result
- actual result (via ValidationCheck on failure)
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from quality_expectations import (
    CUSTOMER_TOTAL_COUNT,
    DUPLICATE_CUSTOMER_ROW_COUNT,
    DUPLICATE_ORDER_ROW_COUNT,
    INTENTIONAL_ISSUE_RULES,
    NULL_CUSTOMER_ID_COUNT,
    NULL_EMAIL_COUNT,
    NULL_PRODUCT_ID_COUNT,
    ORDER_TOTAL_COUNT,
    PRODUCT_TOTAL_COUNT,
    RULE_CUSTOMERS_CUSTOMER_ID_UNIQUE,
    RULE_CUSTOMERS_EMAIL_NOT_NULL,
    RULE_ORDERS_CUSTOMER_ID_EXISTS,
    RULE_ORDERS_CUSTOMER_ID_NOT_NULL,
    RULE_ORDERS_ORDER_ID_UNIQUE,
    RULE_ORDERS_PRODUCT_ID_EXISTS,
    RULE_ORDERS_PRODUCT_ID_NOT_NULL,
)
from test_support import (
    ValidationCheck,
    assert_all_pass,
    assert_intentional_rule_counts,
    build_isolated_assignment_fixtures,
    count_rule_failures,
    dicts_to_bronze_dataframe,
    metric_failed_rows,
    run_silver_pipeline,
)


class TestIsolatedIntentionalIssues:
    """Fast deterministic fixtures with exact assignment issue counts at small scale."""

    def test_isolated_fixture_detects_all_assignment_issue_counts(
        self,
        spark: SparkSession,
    ) -> None:
        """
        Validates: Silver pipeline detects every intentional issue on isolated data.
        Expected: row flags and metrics match INTENTIONAL_ISSUE_RULES counts.
        """
        fixtures = build_isolated_assignment_fixtures()
        bronze_customers = dicts_to_bronze_dataframe(spark, "customers", fixtures["customers"])
        bronze_orders = dicts_to_bronze_dataframe(spark, "orders", fixtures["orders"])
        bronze_products = dicts_to_bronze_dataframe(spark, "products", fixtures["products"])

        result = run_silver_pipeline(
            spark,
            bronze_customers,
            bronze_orders,
            bronze_products,
            run_id="isolated-assignment-issues",
        )

        customer_checks = assert_intentional_rule_counts(
            result["customers"],
            result["metrics"],
            {
                RULE_CUSTOMERS_EMAIL_NOT_NULL: NULL_EMAIL_COUNT,
                RULE_CUSTOMERS_CUSTOMER_ID_UNIQUE: DUPLICATE_CUSTOMER_ROW_COUNT,
            },
        )
        order_checks = assert_intentional_rule_counts(
            result["orders"],
            result["metrics"],
            {
                RULE_ORDERS_CUSTOMER_ID_NOT_NULL: NULL_CUSTOMER_ID_COUNT,
                RULE_ORDERS_PRODUCT_ID_NOT_NULL: NULL_PRODUCT_ID_COUNT,
                RULE_ORDERS_CUSTOMER_ID_EXISTS: 50,
                RULE_ORDERS_PRODUCT_ID_EXISTS: 30,
                RULE_ORDERS_ORDER_ID_UNIQUE: DUPLICATE_ORDER_ROW_COUNT,
            },
        )
        assert_all_pass(customer_checks + order_checks)

    def test_isolated_clean_orders_not_incorrectly_flagged(self, spark: SparkSession) -> None:
        """
        Validates: naturally valid orders in the isolated fixture remain valid.
        Expected: 100 clean base orders have dq_is_valid=true and empty dq_failed_rules.
        """
        fixtures = build_isolated_assignment_fixtures()
        bronze_customers = dicts_to_bronze_dataframe(spark, "customers", fixtures["customers"])
        bronze_orders = dicts_to_bronze_dataframe(spark, "orders", fixtures["orders"])
        bronze_products = dicts_to_bronze_dataframe(spark, "products", fixtures["products"])

        result = run_silver_pipeline(
            spark,
            bronze_customers,
            bronze_orders,
            bronze_products,
            run_id="isolated-clean-orders",
        )

        clean_indices = fixtures["clean_order_indices"]
        source_rows = bronze_orders.collect()
        clean_source_row_nums = {idx + 1 for idx in clean_indices}

        silver_orders = result["orders"]
        clean_silver = silver_orders.filter(F.col("_source_row_num").isin(list(clean_source_row_nums)))

        checks = [
            ValidationCheck(
                name="clean_order_count",
                validates="Isolated naturally valid order rows selected",
                expected=len(clean_indices),
                actual=clean_silver.count(),
            ),
            ValidationCheck(
                name="clean_orders_valid_flag",
                validates="Clean orders have dq_is_valid=true",
                expected=len(clean_indices),
                actual=clean_silver.filter(F.col("dq_is_valid")).count(),
            ),
            ValidationCheck(
                name="clean_orders_no_failed_rules",
                validates="Clean orders have empty dq_failed_rules",
                expected=0,
                actual=clean_silver.filter(F.size(F.col("dq_failed_rules")) > 0).count(),
            ),
        ]
        assert_all_pass(checks)


class TestGeneratedSampleDataIntentionalIssues:
    """Assignment-scale verification using seed=42 generated sample data."""

    def test_customer_intentional_issues_on_generated_sample(
        self,
        silver_sample_pipeline,
    ) -> None:
        """
        Validates: customer intentional issues on full generated sample (seed=42).
        Expected: 50 NULL emails; 20 rows in duplicate customer_id groups.
        """
        checks = assert_intentional_rule_counts(
            silver_sample_pipeline["customers"],
            silver_sample_pipeline["metrics"],
            {
                RULE_CUSTOMERS_EMAIL_NOT_NULL: NULL_EMAIL_COUNT,
                RULE_CUSTOMERS_CUSTOMER_ID_UNIQUE: DUPLICATE_CUSTOMER_ROW_COUNT,
            },
        )
        assert_all_pass(checks)

    def test_order_intentional_issues_on_generated_sample(
        self,
        silver_sample_pipeline,
    ) -> None:
        """
        Validates: order intentional issues on full generated sample (seed=42).
        Expected: NULL FK, orphan FK, and duplicate order_id counts per assignment.
        """
        checks = assert_intentional_rule_counts(
            silver_sample_pipeline["orders"],
            silver_sample_pipeline["metrics"],
            {
                RULE_ORDERS_CUSTOMER_ID_NOT_NULL: NULL_CUSTOMER_ID_COUNT,
                RULE_ORDERS_PRODUCT_ID_NOT_NULL: NULL_PRODUCT_ID_COUNT,
                RULE_ORDERS_CUSTOMER_ID_EXISTS: INTENTIONAL_ISSUE_RULES[RULE_ORDERS_CUSTOMER_ID_EXISTS],
                RULE_ORDERS_PRODUCT_ID_EXISTS: INTENTIONAL_ISSUE_RULES[RULE_ORDERS_PRODUCT_ID_EXISTS],
                RULE_ORDERS_ORDER_ID_UNIQUE: DUPLICATE_ORDER_ROW_COUNT,
            },
        )
        assert_all_pass(checks)

    def test_row_count_invariant_on_generated_sample(
        self,
        bronze_sample_frames,
        silver_sample_pipeline,
    ) -> None:
        """
        Validates: Silver pipeline never deletes rows on assignment-scale data.
        Expected: Bronze and Silver row counts match per entity.
        """
        checks = [
            ValidationCheck(
                name="customers_row_count",
                validates="silver_customers row count equals Bronze customers",
                expected=bronze_sample_frames["customers"].count(),
                actual=silver_sample_pipeline["customers"].count(),
            ),
            ValidationCheck(
                name="orders_row_count",
                validates="silver_orders row count equals Bronze orders",
                expected=bronze_sample_frames["orders"].count(),
                actual=silver_sample_pipeline["orders"].count(),
            ),
            ValidationCheck(
                name="products_row_count",
                validates="silver_products row count equals Bronze products",
                expected=bronze_sample_frames["products"].count(),
                actual=silver_sample_pipeline["products"].count(),
            ),
            ValidationCheck(
                name="assignment_customer_volume",
                validates="Generated customer volume",
                expected=CUSTOMER_TOTAL_COUNT,
                actual=silver_sample_pipeline["customers"].count(),
            ),
            ValidationCheck(
                name="assignment_order_volume",
                validates="Generated order volume",
                expected=ORDER_TOTAL_COUNT,
                actual=silver_sample_pipeline["orders"].count(),
            ),
            ValidationCheck(
                name="assignment_product_volume",
                validates="Generated product volume",
                expected=PRODUCT_TOTAL_COUNT,
                actual=silver_sample_pipeline["products"].count(),
            ),
        ]
        assert_all_pass(checks)


class TestSilverQualityStatusAndTraceability:
    def test_quality_status_columns_populated(self, silver_sample_pipeline) -> None:
        """
        Validates: consolidated quality status columns are populated on every row.
        Expected: no NULL dq_is_valid / dq_record_status / dq_failure_count.
        """
        for entity in ("customers", "orders", "products"):
            df = silver_sample_pipeline[entity]
            checks = [
                ValidationCheck(
                    name=f"{entity}_dq_is_valid_populated",
                    validates=f"{entity} dq_is_valid is never NULL",
                    expected=0,
                    actual=df.filter(F.col("dq_is_valid").isNull()).count(),
                ),
                ValidationCheck(
                    name=f"{entity}_dq_record_status_populated",
                    validates=f"{entity} dq_record_status is never NULL",
                    expected=0,
                    actual=df.filter(F.col("dq_record_status").isNull()).count(),
                ),
                ValidationCheck(
                    name=f"{entity}_dq_failure_count_populated",
                    validates=f"{entity} dq_failure_count is never NULL",
                    expected=0,
                    actual=df.filter(F.col("dq_failure_count").isNull()).count(),
                ),
            ]
            assert_all_pass(checks)

    def test_quality_errors_are_traceable(self, silver_sample_pipeline) -> None:
        """
        Validates: failed rows expose rule ids and human-readable error messages.
        Expected: NULL-email failures include rule id and non-empty dq_error_messages.
        """
        invalid_customers = silver_sample_pipeline["customers"].filter(~F.col("dq_is_valid"))
        email_failures = invalid_customers.filter(
            F.array_contains(F.col("dq_failed_rules"), RULE_CUSTOMERS_EMAIL_NOT_NULL),
        )

        checks = [
            ValidationCheck(
                name="invalid_customers_exist",
                validates="Some invalid customer rows exist in sample data",
                expected=True,
                actual=invalid_customers.count() > 0,
            ),
            ValidationCheck(
                name="email_failure_rows",
                validates="NULL email failures detected",
                expected=NULL_EMAIL_COUNT,
                actual=email_failures.count(),
            ),
            ValidationCheck(
                name="email_failure_messages",
                validates="NULL email rows have dq_error_messages entries",
                expected=0,
                actual=email_failures.filter(F.size(F.col("dq_error_messages")) == 0).count(),
            ),
            ValidationCheck(
                name="bronze_traceability_preserved",
                validates="Invalid customers retain Bronze _source_row_num",
                expected=0,
                actual=invalid_customers.filter(F.col("_source_row_num").isNull()).count(),
            ),
            ValidationCheck(
                name="source_value_preserved",
                validates="Invalid customers retain _source_email raw value column",
                expected=True,
                actual="_source_email" in silver_sample_pipeline["customers"].columns,
            ),
        ]
        assert_all_pass(checks)

    def test_multiple_failures_distinguished(self, silver_sample_pipeline) -> None:
        """
        Validates: rows with multiple failed rules are distinguishable.
        Expected: some orders have dq_record_status='invalid_multiple'.
        """
        orders = silver_sample_pipeline["orders"]
        multi = orders.filter(F.col("dq_record_status") == "invalid_multiple")

        checks = [
            ValidationCheck(
                name="multi_failure_rows_exist",
                validates="At least one order has multiple quality failures",
                expected=True,
                actual=multi.count() > 0,
            ),
            ValidationCheck(
                name="multi_failure_count_consistency",
                validates="invalid_multiple rows have dq_failure_count > 1",
                expected=0,
                actual=multi.filter(F.col("dq_failure_count") <= 1).count(),
            ),
        ]
        assert_all_pass(checks)


class TestSilverQualityMetrics:
    def test_metrics_match_row_flags_for_all_intentional_rules(
        self,
        silver_sample_pipeline,
    ) -> None:
        """
        Validates: dataset-level metrics equal row-level failure counts for intentional rules.
        Expected: metrics.failed_rows == count(array_contains(dq_failed_rules, rule_id)).
        """
        customer_checks = assert_intentional_rule_counts(
            silver_sample_pipeline["customers"],
            silver_sample_pipeline["metrics"],
            {
                RULE_CUSTOMERS_EMAIL_NOT_NULL: NULL_EMAIL_COUNT,
                RULE_CUSTOMERS_CUSTOMER_ID_UNIQUE: DUPLICATE_CUSTOMER_ROW_COUNT,
            },
        )
        order_checks = assert_intentional_rule_counts(
            silver_sample_pipeline["orders"],
            silver_sample_pipeline["metrics"],
            INTENTIONAL_ISSUE_RULES,
        )
        assert_all_pass(customer_checks + order_checks)

    def test_quality_report_schema_and_values(self, silver_sample_pipeline) -> None:
        """
        Validates: reporting dataset uses required columns and matches metrics counts.
        Expected: report row count equals metrics row count; NULL email check shows 50 failures.
        """
        report = silver_sample_pipeline["report"]
        metrics = silver_sample_pipeline["metrics"]

        checks = [
            ValidationCheck(
                name="report_column_set",
                validates="silver_dq_report column contract",
                expected={
                    "dataset",
                    "check_name",
                    "total_records",
                    "passed_records",
                    "failed_records",
                    "pass_percentage",
                    "execution_timestamp",
                },
                actual=set(report.columns),
            ),
            ValidationCheck(
                name="report_row_count",
                validates="Report includes one row per rule metric",
                expected=metrics.count(),
                actual=report.count(),
            ),
            ValidationCheck(
                name="report_null_email_failures",
                validates="Report failed_records for customers_email_not_null",
                expected=NULL_EMAIL_COUNT,
                actual=int(
                    report.filter(F.col("check_name") == RULE_CUSTOMERS_EMAIL_NOT_NULL)
                    .first()["failed_records"],
                ),
            ),
        ]
        assert_all_pass(checks)


class TestValidRecordsNotIncorrectlyFlagged:
    def test_products_mostly_valid_on_generated_sample(self, silver_sample_pipeline) -> None:
        """
        Validates: product catalog rows (no intentional corruptions) remain valid.
        Expected: all 500 generated products pass quality checks.
        """
        products = silver_sample_pipeline["products"]
        checks = [
            ValidationCheck(
                name="all_products_valid",
                validates="Generated products have no intentional DQ corruptions",
                expected=PRODUCT_TOTAL_COUNT,
                actual=products.filter(F.col("dq_is_valid")).count(),
            ),
            ValidationCheck(
                name="no_product_failed_rules",
                validates="Generated products have empty dq_failed_rules",
                expected=0,
                actual=products.filter(F.size(F.col("dq_failed_rules")) > 0).count(),
            ),
        ]
        assert_all_pass(checks)

    def test_category_flags_consistent_with_failed_rules(self, silver_sample_pipeline) -> None:
        """
        Validates: category pass flags are consistent with dq_failed_rules (no contradictions).
        Expected: rows with dq_completeness_pass=false contain a completeness rule id.
        """
        orders = silver_sample_pipeline["orders"]
        inconsistent = orders.filter(
            (~F.col("dq_completeness_pass"))
            & (
                ~F.array_contains(F.col("dq_failed_rules"), RULE_ORDERS_CUSTOMER_ID_NOT_NULL)
                & ~F.array_contains(F.col("dq_failed_rules"), RULE_ORDERS_PRODUCT_ID_NOT_NULL)
            ),
        )

        ValidationCheck(
            name="orders_completeness_flag_consistency",
            validates="dq_completeness_pass false implies completeness rule in dq_failed_rules",
            expected=0,
            actual=inconsistent.count(),
        ).assert_pass()

    def test_valid_customers_without_intentional_issues_remain_valid(
        self,
        silver_sample_pipeline,
    ) -> None:
        """
        Validates: customers outside intentional corruption patterns remain valid.
        Expected: at least one fully valid customer row exists and is not flagged.
        """
        customers = silver_sample_pipeline["customers"]
        valid_rows = customers.filter(F.col("dq_is_valid"))
        sample = valid_rows.first()
        assert sample is not None, "Expected at least one valid customer in generated sample"

        checks = [
            ValidationCheck(
                name="valid_customer_rows_exist",
                validates="Generated sample contains valid customers",
                expected=True,
                actual=valid_rows.count() > 0,
            ),
            ValidationCheck(
                name="sample_valid_customer_failure_count",
                validates="Sample valid customer has zero failed rules",
                expected=0,
                actual=int(sample["dq_failure_count"]),
            ),
            ValidationCheck(
                name="sample_valid_customer_status",
                validates="Sample valid customer dq_record_status is 'valid'",
                expected="valid",
                actual=sample["dq_record_status"],
            ),
        ]
        assert_all_pass(checks)
