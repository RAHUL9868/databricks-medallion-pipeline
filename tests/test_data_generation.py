"""
Tests for deterministic sample data generation.

Each test documents:
- what it validates
- expected result
- actual result (via ValidationCheck on failure)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from data_generation.generate_sample_data import (
    CUSTOMER_TOTAL_COUNT,
    DUPLICATE_CUSTOMER_ID_COUNT,
    DUPLICATE_ORDER_ID_COUNT,
    INVALID_CUSTOMER_ID_COUNT,
    INVALID_PRODUCT_ID_COUNT,
    NULL_CUSTOMER_ID_COUNT,
    NULL_EMAIL_COUNT,
    NULL_PRODUCT_ID_COUNT,
    ORDER_TOTAL_COUNT,
    PRODUCT_COUNT,
    duplicate_row_count,
    extra_duplicate_rows,
    generate_all,
    is_null,
)
from quality_expectations import DEFAULT_GENERATION_SEED
from test_support import ValidationCheck, assert_all_pass


class TestDataGenerationVolumes:
    def test_seed_42_produces_assignment_row_counts(self) -> None:
        """
        Validates: generator row counts for customers, products, and orders.
        Expected: 10,000 customers, 500 products, 100,000 orders.
        """
        customers, products, orders = generate_all(DEFAULT_GENERATION_SEED)
        checks = [
            ValidationCheck(
                name="customer_row_count",
                validates="Total customer rows including duplicate append rows",
                expected=CUSTOMER_TOTAL_COUNT,
                actual=len(customers),
            ),
            ValidationCheck(
                name="product_row_count",
                validates="Total product rows",
                expected=PRODUCT_COUNT,
                actual=len(products),
            ),
            ValidationCheck(
                name="order_row_count",
                validates="Total order rows including duplicate append rows",
                expected=ORDER_TOTAL_COUNT,
                actual=len(orders),
            ),
        ]
        assert_all_pass(checks)

    def test_seed_42_is_deterministic(self) -> None:
        """
        Validates: repeated generation with seed=42 is identical.
        Expected: same customer_id for first and last row on both runs.
        """
        first = generate_all(DEFAULT_GENERATION_SEED)
        second = generate_all(DEFAULT_GENERATION_SEED)
        checks = [
            ValidationCheck(
                name="deterministic_customer_count",
                validates="Customer count stable across runs",
                expected=len(first[0]),
                actual=len(second[0]),
            ),
            ValidationCheck(
                name="deterministic_first_customer_id",
                validates="First customer_id stable across runs",
                expected=first[0][0]["customer_id"],
                actual=second[0][0]["customer_id"],
            ),
            ValidationCheck(
                name="deterministic_last_order_id",
                validates="Last order_id stable across runs",
                expected=first[2][-1]["order_id"],
                actual=second[2][-1]["order_id"],
            ),
        ]
        assert_all_pass(checks)


class TestIntentionalCustomerCorruptions:
    def test_null_email_count(self) -> None:
        """
        Validates: intentional NULL email corruption count.
        Expected: exactly 50 customers with NULL/blank email.
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        actual = sum(1 for row in customers if is_null(row.get("email")))
        ValidationCheck(
            name="null_email_count",
            validates="Customers with NULL email in generated CSV data",
            expected=NULL_EMAIL_COUNT,
            actual=actual,
        ).assert_pass()

    def test_duplicate_customer_id_keys_and_rows(self) -> None:
        """
        Validates: intentional duplicate customer_id corruption.

        Expected:
        - 10 duplicated customer_id key values
        - 20 rows participating in duplicate groups (all members flagged in Silver)
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        dup_keys = DUPLICATE_CUSTOMER_ID_COUNT
        dup_rows_in_groups = duplicate_row_count(customers, "customer_id")
        extra_dup_rows = extra_duplicate_rows(customers, "customer_id")

        checks = [
            ValidationCheck(
                name="duplicate_customer_extra_rows",
                validates="Extra appended duplicate customer rows",
                expected=DUPLICATE_CUSTOMER_ID_COUNT,
                actual=extra_dup_rows,
            ),
            ValidationCheck(
                name="duplicate_customer_group_rows",
                validates="Rows in duplicate customer_id groups (2 per key)",
                expected=dup_keys * 2,
                actual=dup_rows_in_groups,
            ),
        ]
        assert_all_pass(checks)


class TestIntentionalOrderCorruptions:
    def test_null_foreign_key_counts(self) -> None:
        """
        Validates: intentional NULL FK corruptions in orders.
        Expected: 100 NULL customer_id, 200 NULL product_id.
        """
        _, _, orders = generate_all(DEFAULT_GENERATION_SEED)
        checks = [
            ValidationCheck(
                name="null_customer_id",
                validates="Orders with NULL customer_id",
                expected=NULL_CUSTOMER_ID_COUNT,
                actual=sum(1 for row in orders if is_null(row.get("customer_id"))),
            ),
            ValidationCheck(
                name="null_product_id",
                validates="Orders with NULL product_id",
                expected=NULL_PRODUCT_ID_COUNT,
                actual=sum(1 for row in orders if is_null(row.get("product_id"))),
            ),
        ]
        assert_all_pass(checks)

    def test_invalid_foreign_key_counts(self) -> None:
        """
        Validates: intentional orphan FK corruptions in orders.
        Expected: 50 invalid customer_id, 30 invalid product_id.
        """
        customers, products, orders = generate_all(DEFAULT_GENERATION_SEED)
        valid_customer_ids = {row["customer_id"] for row in customers}
        valid_product_ids = {row["product_id"] for row in products}

        invalid_customer = sum(
            1
            for row in orders
            if not is_null(row.get("customer_id")) and row["customer_id"] not in valid_customer_ids
        )
        invalid_product = sum(
            1
            for row in orders
            if not is_null(row.get("product_id")) and row["product_id"] not in valid_product_ids
        )

        checks = [
            ValidationCheck(
                name="invalid_customer_id",
                validates="Orders with customer_id not in customer dataset",
                expected=INVALID_CUSTOMER_ID_COUNT,
                actual=invalid_customer,
            ),
            ValidationCheck(
                name="invalid_product_id",
                validates="Orders with product_id not in product dataset",
                expected=INVALID_PRODUCT_ID_COUNT,
                actual=invalid_product,
            ),
        ]
        assert_all_pass(checks)

    def test_duplicate_order_id_keys_and_rows(self) -> None:
        """
        Validates: intentional duplicate order_id corruption.

        Expected:
        - 20 duplicated order_id key values
        - 40 rows participating in duplicate groups
        """
        _, _, orders = generate_all(DEFAULT_GENERATION_SEED)
        checks = [
            ValidationCheck(
                name="duplicate_order_extra_rows",
                validates="Extra appended duplicate order rows",
                expected=DUPLICATE_ORDER_ID_COUNT,
                actual=extra_duplicate_rows(orders, "order_id"),
            ),
            ValidationCheck(
                name="duplicate_order_group_rows",
                validates="Rows in duplicate order_id groups (2 per key)",
                expected=DUPLICATE_ORDER_ID_COUNT * 2,
                actual=duplicate_row_count(orders, "order_id"),
            ),
        ]
        assert_all_pass(checks)
