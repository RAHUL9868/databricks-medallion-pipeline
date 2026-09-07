#!/usr/bin/env python3
"""
Generate deterministic sample e-commerce CSV datasets for the Medallion pipeline.

Runs independently of Databricks. Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# ---------------------------------------------------------------------------
# Volume and corruption targets (assignment specification)
# ---------------------------------------------------------------------------

CUSTOMER_UNIQUE_COUNT = 9_990
CUSTOMER_DUPLICATE_ROW_COUNT = 10
CUSTOMER_TOTAL_COUNT = CUSTOMER_UNIQUE_COUNT + CUSTOMER_DUPLICATE_ROW_COUNT  # 10,000

PRODUCT_COUNT = 500

ORDER_UNIQUE_COUNT = 99_980
ORDER_DUPLICATE_ROW_COUNT = 20
ORDER_TOTAL_COUNT = ORDER_UNIQUE_COUNT + ORDER_DUPLICATE_ROW_COUNT  # 100,000

NULL_EMAIL_COUNT = 50
DUPLICATE_CUSTOMER_ID_COUNT = 10  # number of duplicate rows appended (10 keys appear twice)

NULL_CUSTOMER_ID_COUNT = 100
NULL_PRODUCT_ID_COUNT = 200
INVALID_CUSTOMER_ID_COUNT = 50
INVALID_PRODUCT_ID_COUNT = 30
DUPLICATE_ORDER_ID_COUNT = 20  # duplicate rows appended (20 keys appear twice -> 40 rows in dup groups)

DEFAULT_SEED = 42

INVALID_CUSTOMER_ID_START = 9_000_001
INVALID_PRODUCT_ID_START = 9_000_001

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")

FIRST_NAMES = (
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy", "Deborah",
    "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura",
    "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Angela",
    "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda", "Larry", "Pamela",
    "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen", "Benjamin", "Samantha",
    "Samuel", "Katherine", "Gregory", "Christine", "Alexander", "Debra", "Patrick", "Rachel",
    "Frank", "Carolyn", "Raymond", "Janet", "Jack", "Catherine", "Dennis", "Maria",
    "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth", "Jose", "Julie",
    "Adam", "Olivia", "Henry", "Joyce", "Nathan", "Virginia", "Douglas", "Victoria",
    "Zachary", "Kelly", "Peter", "Lauren", "Kyle", "Christina", "Noah", "Joan",
)

LAST_NAMES = (
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
)

COUNTRIES = (
    "United States", "United Kingdom", "Canada", "Germany", "France",
    "Australia", "India", "Japan", "Brazil", "Mexico", "Spain", "Italy",
    "Netherlands", "Sweden", "Singapore", "Ireland", "New Zealand", "South Africa",
)

PRODUCT_CATEGORIES = (
    "Electronics", "Clothing", "Home & Kitchen", "Sports & Outdoors",
    "Books", "Beauty & Personal Care", "Toys & Games", "Garden & Outdoor",
)

PRODUCT_ADJECTIVES = (
    "Pro", "Essential", "Classic", "Ultra", "Compact", "Premium", "Everyday",
    "Deluxe", "Smart", "Eco", "Advanced", "Portable", "Wireless", "Organic",
)

PRODUCT_NOUNS = (
    "Headphones", "Backpack", "Blender", "Sneakers", "Notebook", "Lamp", "Jacket",
    "Watch", "Keyboard", "Bottle", "Speaker", "Desk Chair", "Sunglasses", "Tablet",
    "Cookware Set", "Yoga Mat", "Camera", "Wallet", "Router", "Monitor", "Mug",
    "Running Shoes", "Novel", "Moisturizer", "Puzzle", "Planter", "Gloves", "Toolkit",
    "Power Bank", "Hoodie", "Vacuum", "Skateboard", "E-Reader", "Diffuser", "Drone",
)

CUSTOMER_CSV_COLUMNS = [
    "customer_id", "customer_name", "email", "country",
    "signup_date", "customer_segment", "lifetime_value",
]

PRODUCT_CSV_COLUMNS = [
    "product_id", "product_name", "category", "price", "cost",
    "stock_quantity", "reorder_level",
]

ORDER_CSV_COLUMNS = [
    "order_id", "customer_id", "order_date", "product_id", "quantity",
    "unit_price", "total_amount", "order_status", "payment_date",
]

logger = logging.getLogger(__name__)


@dataclass
class CorruptionAudit:
    """Tracks intentionally corrupted records (not written to CSV)."""

    null_email_customer_ids: Set[int] = field(default_factory=set)
    duplicate_customer_source_ids: Set[int] = field(default_factory=set)
    duplicate_customer_row_indices: List[int] = field(default_factory=list)

    null_customer_id_order_indices: Set[int] = field(default_factory=set)
    null_product_id_order_indices: Set[int] = field(default_factory=set)
    invalid_customer_id_order_indices: Set[int] = field(default_factory=set)
    invalid_product_id_order_indices: Set[int] = field(default_factory=set)
    duplicate_order_source_indices: Set[int] = field(default_factory=set)
    duplicate_order_row_indices: List[int] = field(default_factory=list)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Generate deterministic e-commerce sample CSV datasets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "data",
        help="Directory for output CSV files (default: <repo>/data)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser.parse_args(argv)


def money(value: float | Decimal) -> str:
    """Format a monetary value to two decimal places."""
    quantized = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:.2f}"


def random_date(rng: random.Random, start: date, end: date) -> date:
    """Return a uniform random date in [start, end]."""
    if start > end:
        raise ValueError(f"start date {start} must be <= end date {end}")
    delta_days = (end - start).days
    return start + timedelta(days=rng.randint(0, delta_days))


def pick_disjoint_index_sets(
    rng: random.Random,
    pool_size: int,
    sizes: Dict[str, int],
) -> Dict[str, List[int]]:
    """Partition a shuffled index pool into named disjoint sets."""
    total_needed = sum(sizes.values())
    if total_needed > pool_size:
        raise ValueError(
            f"Cannot pick {total_needed} disjoint indices from pool of size {pool_size}",
        )
    pool = list(range(pool_size))
    rng.shuffle(pool)
    result: Dict[str, List[int]] = {}
    offset = 0
    for name, size in sizes.items():
        result[name] = sorted(pool[offset : offset + size])
        offset += size
    return result


def generate_customers(rng: random.Random) -> Tuple[List[Dict[str, Any]], CorruptionAudit]:
    """Generate naturally valid customers, then apply intentional corruptions."""
    audit = CorruptionAudit()
    customers: List[Dict[str, Any]] = []
    signup_start = date(2018, 1, 1)
    signup_end = date(2025, 6, 30)

    segment_weights = (0.20, 0.50, 0.30)  # Premium, Standard, Basic

    for customer_id in range(1, CUSTOMER_UNIQUE_COUNT + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        segment = rng.choices(CUSTOMER_SEGMENTS, weights=segment_weights, k=1)[0]
        if segment == "Premium":
            ltv = rng.uniform(2_500, 15_000)
        elif segment == "Standard":
            ltv = rng.uniform(500, 4_000)
        else:
            ltv = rng.uniform(50, 1_200)

        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{customer_id}@mail.example.com",
                "country": rng.choice(COUNTRIES),
                "signup_date": random_date(rng, signup_start, signup_end).isoformat(),
                "customer_segment": segment,
                "lifetime_value": money(ltv),
            }
        )

    corruption_sets = pick_disjoint_index_sets(
        rng,
        CUSTOMER_UNIQUE_COUNT,
        {
            "null_email": NULL_EMAIL_COUNT,
            "duplicate_source": DUPLICATE_CUSTOMER_ID_COUNT,
        },
    )

    for idx in corruption_sets["null_email"]:
        customer_id = customers[idx]["customer_id"]
        customers[idx]["email"] = None
        audit.null_email_customer_ids.add(customer_id)

    for idx in corruption_sets["duplicate_source"]:
        source = customers[idx]
        audit.duplicate_customer_source_ids.add(source["customer_id"])
        duplicate_row = dict(source)
        customers.append(duplicate_row)
        audit.duplicate_customer_row_indices.append(len(customers) - 1)

    return customers, audit


def generate_products(rng: random.Random) -> List[Dict[str, Any]]:
    """Generate naturally valid products (no intentional corruptions)."""
    products: List[Dict[str, Any]] = []
    used_names: Set[str] = set()

    for product_id in range(1, PRODUCT_COUNT + 1):
        category = PRODUCT_CATEGORIES[(product_id - 1) % len(PRODUCT_CATEGORIES)]
        for _ in range(50):
            name = f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS)}"
            if name not in used_names:
                used_names.add(name)
                break
        else:
            name = f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS)} {product_id}"

        price = Decimal(str(rng.uniform(5, 500))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        margin = Decimal(str(rng.uniform(0.15, 0.55)))
        cost = (price * (Decimal("1") - margin)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        stock = rng.randint(0, 2_000)
        reorder = max(10, int(stock * rng.uniform(0.10, 0.35)))

        products.append(
            {
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "price": money(price),
                "cost": money(cost),
                "stock_quantity": stock,
                "reorder_level": reorder,
            }
        )

    return products


def generate_orders(
    rng: random.Random,
    customers: Sequence[Dict[str, Any]],
    products: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], CorruptionAudit]:
    """Generate naturally valid orders, then apply intentional corruptions."""
    audit = CorruptionAudit()

    valid_customer_ids = {c["customer_id"] for c in customers}
    valid_product_ids = {p["product_id"] for p in products}
    product_price = {p["product_id"]: Decimal(p["price"]) for p in products}
    customer_signup = {
        c["customer_id"]: date.fromisoformat(c["signup_date"])
        for c in customers
        if c["signup_date"]
    }

    orders: List[Dict[str, Any]] = []
    order_start = date(2019, 1, 1)
    order_end = date(2025, 8, 31)
    status_weights = (0.20, 0.70, 0.10)  # Pending, Completed, Cancelled

    for order_id in range(1, ORDER_UNIQUE_COUNT + 1):
        customer_id = rng.choice(list(valid_customer_ids))
        product_id = rng.choice(list(valid_product_ids))
        signup = customer_signup[customer_id]
        order_date = random_date(rng, max(order_start, signup), order_end)
        quantity = rng.randint(1, 5)
        base_price = product_price[product_id]
        unit_price = (base_price * Decimal(str(rng.uniform(0.95, 1.05)))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP,
        )
        total_amount = (unit_price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        status = rng.choices(ORDER_STATUSES, weights=status_weights, k=1)[0]
        payment_date: Optional[str]
        if status == "Completed":
            payment_date = random_date(
                rng, order_date, min(order_end, order_date + timedelta(days=14)),
            ).isoformat()
        else:
            payment_date = None

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": money(unit_price),
                "total_amount": money(total_amount),
                "order_status": status,
                "payment_date": payment_date,
            }
        )

    corruption_sets = pick_disjoint_index_sets(
        rng,
        ORDER_UNIQUE_COUNT,
        {
            "null_customer_id": NULL_CUSTOMER_ID_COUNT,
            "null_product_id": NULL_PRODUCT_ID_COUNT,
            "invalid_customer_id": INVALID_CUSTOMER_ID_COUNT,
            "invalid_product_id": INVALID_PRODUCT_ID_COUNT,
            "duplicate_source": DUPLICATE_ORDER_ID_COUNT,
        },
    )

    for idx in corruption_sets["null_customer_id"]:
        orders[idx]["customer_id"] = None
        audit.null_customer_id_order_indices.add(idx)

    for idx in corruption_sets["null_product_id"]:
        orders[idx]["product_id"] = None
        audit.null_product_id_order_indices.add(idx)

    for i, idx in enumerate(corruption_sets["invalid_customer_id"]):
        invalid_id = INVALID_CUSTOMER_ID_START + i
        if invalid_id in valid_customer_ids:
            raise RuntimeError(f"Generated invalid customer id collides with valid id: {invalid_id}")
        orders[idx]["customer_id"] = invalid_id
        audit.invalid_customer_id_order_indices.add(idx)

    for i, idx in enumerate(corruption_sets["invalid_product_id"]):
        invalid_id = INVALID_PRODUCT_ID_START + i
        if invalid_id in valid_product_ids:
            raise RuntimeError(f"Generated invalid product id collides with valid id: {invalid_id}")
        orders[idx]["product_id"] = invalid_id
        audit.invalid_product_id_order_indices.add(idx)

    for idx in corruption_sets["duplicate_source"]:
        source = orders[idx]
        audit.duplicate_order_source_indices.add(idx)
        duplicate_row = dict(source)
        orders.append(duplicate_row)
        audit.duplicate_order_row_indices.append(len(orders) - 1)

    return orders, audit


def is_null(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def duplicate_row_count(records: Sequence[Dict[str, Any]], key: str) -> int:
    """Count rows whose key value appears more than once (all members of duplicate groups)."""
    counts = Counter(r[key] for r in records if not is_null(r.get(key)))
    return sum(count for count in counts.values() if count > 1)


def duplicate_key_count(records: Sequence[Dict[str, Any]], key: str) -> int:
    """Count key values that appear more than once."""
    counts = Counter(r[key] for r in records if not is_null(r.get(key)))
    return sum(1 for count in counts.values() if count > 1)


def extra_duplicate_rows(records: Sequence[Dict[str, Any]], key: str) -> int:
    """Count rows beyond the first occurrence for each duplicated key."""
    counts = Counter(r[key] for r in records if not is_null(r.get(key)))
    return sum(count - 1 for count in counts.values() if count > 1)


def validate_customers(
    customers: Sequence[Dict[str, Any]],
    audit: CorruptionAudit,
) -> None:
    """Fail fast if customer data or intentional issues are incorrect."""
    if len(customers) != CUSTOMER_TOTAL_COUNT:
        raise ValueError(f"Expected {CUSTOMER_TOTAL_COUNT} customers, got {len(customers)}")

    null_email_rows = sum(1 for c in customers if is_null(c.get("email")))
    if null_email_rows != NULL_EMAIL_COUNT:
        raise ValueError(
            f"Expected exactly {NULL_EMAIL_COUNT} NULL email rows, got {null_email_rows}",
        )

    extra_dup_rows = extra_duplicate_rows(customers, "customer_id")
    if extra_dup_rows != DUPLICATE_CUSTOMER_ID_COUNT:
        raise ValueError(
            f"Expected exactly {DUPLICATE_CUSTOMER_ID_COUNT} duplicate customer rows, "
            f"got {extra_dup_rows}",
        )

    dup_keys = duplicate_key_count(customers, "customer_id")
    if dup_keys != DUPLICATE_CUSTOMER_ID_COUNT:
        raise ValueError(
            f"Expected exactly {DUPLICATE_CUSTOMER_ID_COUNT} duplicated customer_id values, "
            f"got {dup_keys}",
        )

    dup_group_rows = duplicate_row_count(customers, "customer_id")
    if dup_group_rows != DUPLICATE_CUSTOMER_ID_COUNT * 2:
        raise ValueError(
            f"Expected {DUPLICATE_CUSTOMER_ID_COUNT * 2} rows in duplicate customer_id groups, "
            f"got {dup_group_rows}",
        )

    if audit.null_email_customer_ids != {
        customers[i]["customer_id"] for i in range(CUSTOMER_UNIQUE_COUNT)
        if is_null(customers[i].get("email"))
    }:
        raise ValueError("NULL email audit does not match generated customer rows")

    for field_name in CUSTOMER_CSV_COLUMNS:
        if field_name not in customers[0]:
            raise ValueError(f"Missing customer field: {field_name}")

    non_null_ids = [c["customer_id"] for c in customers if not is_null(c.get("customer_id"))]
    if len(set(non_null_ids)) != len(customers) - DUPLICATE_CUSTOMER_ID_COUNT:
        raise ValueError("Unexpected extra primary-key duplication beyond intentional customer duplicates")

    logger.info(
        "Customer validation passed: rows=%s, null_emails=%s, duplicate_rows=%s",
        len(customers),
        null_email_rows,
        extra_dup_rows,
    )


def validate_products(products: Sequence[Dict[str, Any]]) -> None:
    """Fail fast if product data is incorrect."""
    if len(products) != PRODUCT_COUNT:
        raise ValueError(f"Expected {PRODUCT_COUNT} products, got {len(products)}")

    product_ids = [p["product_id"] for p in products]
    if len(set(product_ids)) != len(product_ids):
        raise ValueError("Accidental duplicate product_id values detected")

    for product in products:
        price = Decimal(product["price"])
        cost = Decimal(product["cost"])
        if price < 0 or cost < 0:
            raise ValueError(f"Negative price/cost for product_id={product['product_id']}")
        if product["stock_quantity"] < 0 or product["reorder_level"] < 0:
            raise ValueError(f"Negative inventory for product_id={product['product_id']}")

    logger.info("Product validation passed: rows=%s", len(products))


def validate_orders(
    orders: Sequence[Dict[str, Any]],
    customers: Sequence[Dict[str, Any]],
    products: Sequence[Dict[str, Any]],
    audit: CorruptionAudit,
) -> None:
    """Fail fast if order data or intentional issues are incorrect."""
    if len(orders) != ORDER_TOTAL_COUNT:
        raise ValueError(f"Expected {ORDER_TOTAL_COUNT} orders, got {len(orders)}")

    valid_customer_ids = {c["customer_id"] for c in customers}
    valid_product_ids = {p["product_id"] for p in products}

    null_customer = sum(1 for o in orders if is_null(o.get("customer_id")))
    null_product = sum(1 for o in orders if is_null(o.get("product_id")))
    if null_customer != NULL_CUSTOMER_ID_COUNT:
        raise ValueError(f"Expected {NULL_CUSTOMER_ID_COUNT} NULL customer_id rows, got {null_customer}")
    if null_product != NULL_PRODUCT_ID_COUNT:
        raise ValueError(f"Expected {NULL_PRODUCT_ID_COUNT} NULL product_id rows, got {null_product}")

    invalid_customer = sum(
        1 for o in orders
        if not is_null(o.get("customer_id")) and o["customer_id"] not in valid_customer_ids
    )
    invalid_product = sum(
        1 for o in orders
        if not is_null(o.get("product_id")) and o["product_id"] not in valid_product_ids
    )
    if invalid_customer != INVALID_CUSTOMER_ID_COUNT:
        raise ValueError(
            f"Expected {INVALID_CUSTOMER_ID_COUNT} invalid customer_id rows, got {invalid_customer}",
        )
    if invalid_product != INVALID_PRODUCT_ID_COUNT:
        raise ValueError(
            f"Expected {INVALID_PRODUCT_ID_COUNT} invalid product_id rows, got {invalid_product}",
        )

    extra_dup_rows = extra_duplicate_rows(orders, "order_id")
    if extra_dup_rows != DUPLICATE_ORDER_ID_COUNT:
        raise ValueError(
            f"Expected exactly {DUPLICATE_ORDER_ID_COUNT} duplicate order rows, got {extra_dup_rows}",
        )

    dup_group_rows = duplicate_row_count(orders, "order_id")
    if dup_group_rows != DUPLICATE_ORDER_ID_COUNT * 2:
        raise ValueError(
            f"Expected {DUPLICATE_ORDER_ID_COUNT * 2} rows in duplicate order_id groups, "
            f"got {dup_group_rows}",
        )

    # Disjoint intentional corruption on the base unique order set (pre-append indices).
    base_indices = set(range(ORDER_UNIQUE_COUNT))
    corruption_indices = (
        audit.null_customer_id_order_indices
        | audit.null_product_id_order_indices
        | audit.invalid_customer_id_order_indices
        | audit.invalid_product_id_order_indices
        | audit.duplicate_order_source_indices
    )
    if not corruption_indices.issubset(base_indices):
        raise ValueError("Corruption indices exceed unique order index range")
    if len(corruption_indices) != (
        NULL_CUSTOMER_ID_COUNT
        + NULL_PRODUCT_ID_COUNT
        + INVALID_CUSTOMER_ID_COUNT
        + INVALID_PRODUCT_ID_COUNT
        + DUPLICATE_ORDER_ID_COUNT
    ):
        raise ValueError("Intentional order corruptions overlap on the same rows")

    # Naturally generated rows (excluding intentional corruptions) must have valid FKs.
    for idx, order in enumerate(orders[:ORDER_UNIQUE_COUNT]):
        if idx in corruption_indices:
            continue
        if order["customer_id"] not in valid_customer_ids:
            raise ValueError(f"Accidental invalid customer_id at natural order index {idx}")
        if order["product_id"] not in valid_product_ids:
            raise ValueError(f"Accidental invalid product_id at natural order index {idx}")

    # total_amount consistency for rows with numeric fields present.
    for order in orders:
        if is_null(order.get("quantity")) or is_null(order.get("unit_price")) or is_null(order.get("total_amount")):
            continue
        quantity = int(order["quantity"])
        unit_price = Decimal(order["unit_price"])
        total_amount = Decimal(order["total_amount"])
        expected = (unit_price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if total_amount != expected:
            raise ValueError(
                f"total_amount mismatch for order_id={order['order_id']}: "
                f"expected {expected}, got {total_amount}",
            )

    # Date sanity for non-corrupted logical rows.
    for order in orders:
        if is_null(order.get("order_date")):
            continue
        order_date = date.fromisoformat(order["order_date"])
        if order_date > date(2025, 12, 31):
            raise ValueError(f"order_date out of range for order_id={order['order_id']}")
        if not is_null(order.get("payment_date")):
            payment_date = date.fromisoformat(order["payment_date"])
            if payment_date < order_date:
                raise ValueError(
                    f"payment_date before order_date for order_id={order['order_id']}",
                )
        if order["order_status"] == "Completed" and is_null(order.get("payment_date")):
            raise ValueError(
                f"Completed order missing payment_date for order_id={order['order_id']}",
            )

    logger.info(
        "Order validation passed: rows=%s, null_customer_id=%s, null_product_id=%s, "
        "invalid_customer_id=%s, invalid_product_id=%s, duplicate_rows=%s",
        len(orders),
        null_customer,
        null_product,
        invalid_customer,
        invalid_product,
        extra_dup_rows,
    )


def validate_output_dir(output_dir: Path) -> Path:
    """Validate and prepare output directory."""
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"Output path exists but is not a directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir.resolve()


def write_csv(path: Path, columns: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    """Write records to CSV; None values become empty fields (NULL)."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: ("" if row.get(col) is None else row[col]) for col in columns})
    logger.info("Wrote %s rows to %s", len(rows), path)


def generate_all(seed: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Generate customers, products, and orders with validation."""
    rng = random.Random(seed)
    logger.info("Generating datasets with seed=%s", seed)

    customers, customer_audit = generate_customers(rng)
    validate_customers(customers, customer_audit)

    products = generate_products(rng)
    validate_products(products)

    orders, order_audit = generate_orders(rng, customers, products)
    validate_orders(orders, customers, products, order_audit)

    return customers, products, orders


def write_sample_datasets(output_dir: Path, seed: int = DEFAULT_SEED) -> Path:
    """Generate, validate, and write all sample CSV files to ``output_dir``."""
    resolved = validate_output_dir(output_dir)
    customers, products, orders = generate_all(seed)

    write_csv(resolved / "customers.csv", CUSTOMER_CSV_COLUMNS, customers)
    write_csv(resolved / "products.csv", PRODUCT_CSV_COLUMNS, products)
    write_csv(resolved / "orders.csv", ORDER_CSV_COLUMNS, orders)

    logger.info("Sample datasets written to %s (seed=%s)", resolved, seed)
    return resolved


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        output_dir = validate_output_dir(args.output_dir)
        write_sample_datasets(output_dir, seed=args.seed)

        logger.info("Data generation completed successfully. Output directory: %s", output_dir)
        return 0
    except (ValueError, RuntimeError, OSError) as exc:
        logger.error("Data generation failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
