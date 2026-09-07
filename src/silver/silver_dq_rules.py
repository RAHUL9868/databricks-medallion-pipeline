"""Central registry of data-quality rule IDs grouped by category and entity."""

from __future__ import annotations

from typing import Dict, FrozenSet

COMPLETENESS_RULE_IDS: FrozenSet[str] = frozenset(
    {
        "customers_email_not_null",
        "orders_customer_id_not_null",
        "orders_product_id_not_null",
    },
)

UNIQUENESS_RULE_IDS: FrozenSet[str] = frozenset(
    {
        "customers_customer_id_unique",
        "orders_order_id_unique",
    },
)

REFERENTIAL_RULE_IDS: FrozenSet[str] = frozenset(
    {
        "orders_customer_id_exists",
        "orders_product_id_exists",
    },
)

TYPE_RULE_IDS: FrozenSet[str] = frozenset(
    {
        "customers_customer_id_valid",
        "customers_signup_date_valid",
        "customers_segment_valid",
        "customers_lifetime_value_valid",
        "orders_order_id_valid",
        "orders_customer_id_valid",
        "orders_order_date_valid",
        "orders_product_id_valid",
        "orders_quantity_positive",
        "orders_unit_price_non_negative",
        "orders_total_amount_non_negative",
        "orders_status_valid",
        "orders_payment_date_valid",
        "products_product_id_valid",
        "products_price_non_negative",
        "products_cost_non_negative",
        "products_stock_quantity_non_negative",
        "products_reorder_level_non_negative",
    },
)

BUSINESS_RULE_IDS: FrozenSet[str] = frozenset(
    {
        "orders_total_amount_matches",
        "orders_completed_has_payment_date",
        "orders_payment_date_after_order_date",
        "products_cost_not_exceeds_price",
    },
)

TYPE_BUSINESS_RULE_IDS: FrozenSet[str] = TYPE_RULE_IDS | BUSINESS_RULE_IDS

ENTITY_COMPLETENESS_RULE_IDS: Dict[str, FrozenSet[str]] = {
    "customers": frozenset({"customers_email_not_null"}),
    "orders": frozenset(
        {
            "orders_customer_id_not_null",
            "orders_product_id_not_null",
        },
    ),
    "products": frozenset(),
}

ENTITY_UNIQUENESS_RULE_IDS: Dict[str, FrozenSet[str]] = {
    "customers": frozenset({"customers_customer_id_unique"}),
    "orders": frozenset({"orders_order_id_unique"}),
    "products": frozenset(),
}

ENTITY_REFERENTIAL_RULE_IDS: Dict[str, FrozenSet[str]] = {
    "customers": frozenset(),
    "orders": REFERENTIAL_RULE_IDS,
    "products": frozenset(),
}

ENTITY_TYPE_RULE_IDS: Dict[str, FrozenSet[str]] = {
    "customers": frozenset(
        {
            "customers_customer_id_valid",
            "customers_signup_date_valid",
            "customers_segment_valid",
            "customers_lifetime_value_valid",
        },
    ),
    "orders": frozenset(
        {
            "orders_order_id_valid",
            "orders_customer_id_valid",
            "orders_order_date_valid",
            "orders_product_id_valid",
            "orders_quantity_positive",
            "orders_unit_price_non_negative",
            "orders_total_amount_non_negative",
            "orders_status_valid",
            "orders_payment_date_valid",
        },
    ),
    "products": frozenset(
        {
            "products_product_id_valid",
            "products_price_non_negative",
            "products_cost_non_negative",
            "products_stock_quantity_non_negative",
            "products_reorder_level_non_negative",
        },
    ),
}

ENTITY_BUSINESS_RULE_IDS: Dict[str, FrozenSet[str]] = {
    "customers": frozenset(),
    "orders": frozenset(
        {
            "orders_total_amount_matches",
            "orders_completed_has_payment_date",
            "orders_payment_date_after_order_date",
        },
    ),
    "products": frozenset({"products_cost_not_exceeds_price"}),
}
