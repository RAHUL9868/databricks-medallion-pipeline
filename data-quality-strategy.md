# Data Quality Strategy

This document defines the data quality (DQ) framework for the e-commerce Medallion pipeline. It aligns with `requirements-analysis.md`, `design-notes.md`, and `data-model.md`.

**Core principle:** Invalid records are **never deleted** to improve quality metrics. All source rows flow from Bronze to Silver; failures are flagged at row level and measured at dataset level.

---

## Framework Overview

### Quality Check Categories

| # | Category | Silver Module | Scope |
|---|----------|---------------|-------|
| 1 | Completeness | `01_quality_completeness.py` | Required fields present and non-blank |
| 2 | Uniqueness | `02_quality_uniqueness.py` | Primary keys unique within entity |
| 3 | Referential integrity | `04_quality_referential_integrity.py` | Foreign keys resolve to parent entities |
| 4 | Type validation | `03_quality_type_validation.py` | Parseable types, valid enums, non-negative ranges |
| 5 | Business-rule validation | `05_quality_business_logic.py` | Cross-field and status-dependent rules |

Type validation and business-rule validation are separate modules but share the `type_business` category flag and `check_category = 'type_business'` in metrics (business rules are a sub-type of type/business validation per assignment).

### Execution Order

```
Bronze (no DQ)
    ↓
Cast / standardize types (in create_silver_tables or shared prep)
    ↓
01 Completeness
    ↓
02 Uniqueness
    ↓
03 Type validation
    ↓
04 Referential integrity (orders only; needs customer + product key sets)
    ↓
05 Business-rule validation
    ↓
Consolidate row-level flags → write silver_* tables
    ↓
Aggregate rule results → write silver_dq_metrics
```

---

## Quality Representation

### Design Choice

Alternatives considered:

| Pattern | Pros | Cons |
|---------|------|------|
| `quality_status` + `quality_errors` (string) | Simple to read | Hard to query; ambiguous with multiple errors |
| `quality_check_result` (JSON map) | Flexible | Verbose; harder to test in Spark |
| **`dq_is_valid` + `dq_failed_rules` (array) + category booleans** | Queryable, testable, matches `data-model.md` | Slightly more columns |

**Selected representation (row-level, on each `silver_*` entity table):**

| Column | Type | Role |
|--------|------|------|
| `dq_is_valid` | BOOLEAN | Overall pass/fail for the row |
| `dq_failed_rules` | ARRAY&lt;STRING&gt; | All failed rule IDs for the row |
| `dq_failure_count` | INT | `size(dq_failed_rules)` |
| `dq_completeness_pass` | BOOLEAN | Category pass if no completeness rule failed |
| `dq_uniqueness_pass` | BOOLEAN | Category pass if no uniqueness rule failed |
| `dq_referential_pass` | BOOLEAN | Category pass (always `true` for customers/products) |
| `dq_type_business_pass` | BOOLEAN | Category pass if no type or business rule failed |
| `dq_checked_at` | TIMESTAMP | Last evaluation time |
| `dq_run_id` | STRING | Pipeline run identifier |

This maps conceptually to the assignment terms:

| Assignment concept | Implementation |
|--------------------|----------------|
| `quality_status` | `dq_is_valid` (`true` = pass, `false` = fail) |
| `quality_errors` | `dq_failed_rules` (machine-readable rule IDs) |
| `quality_check_result` | Combination of category booleans + `dq_failed_rules` |

### Multiple Failures on the Same Row

A single row may fail more than one rule (e.g., an order with NULL `customer_id` and NULL `product_id`).

**Representation rules:**

1. Each failed rule appends its stable `rule_id` to `dq_failed_rules` (no duplicates in the array).
2. `dq_failure_count` equals the number of entries in `dq_failed_rules`.
3. `dq_is_valid = false` if `dq_failure_count > 0`.
4. Category boolean is `false` if **any** rule in that category failed; otherwise `true`.
5. A row failing both completeness and referential integrity will have both `dq_completeness_pass = false` and `dq_referential_pass = false`, with multiple entries in `dq_failed_rules`.

**Example:**

```
customer_id = 1001, email = NULL, customer_id duplicated
dq_failed_rules = ['customers_email_not_null', 'customers_customer_id_unique']
dq_failure_count = 2
dq_is_valid = false
dq_completeness_pass = false
dq_uniqueness_pass = false
```

**Per-rule boolean columns are not used** (e.g., no `dq_customers_email_not_null` column) to avoid column sprawl. Rule-level detail is available via `dq_failed_rules` and `silver_dq_metrics`. Tests assert on `dq_failed_rules` and metrics table counts.

### Dataset-Level Metrics Persistence

All aggregated metrics are written to **`silver_dq_metrics`** after Silver tables are built.

| Column | Description |
|--------|-------------|
| `run_id` | Pipeline execution ID |
| `entity` | `customers`, `orders`, or `products` |
| `check_category` | `completeness`, `uniqueness`, `referential_integrity`, `type_business` |
| `rule_id` | Stable rule identifier |
| `rule_description` | Human-readable description |
| `total_rows` | Rows evaluated for this entity |
| `failed_rows` | Rows where this rule failed |
| `passed_rows` | `total_rows - failed_rows` |
| `pass_pct` | `ROUND(100.0 * passed_rows / total_rows, 2)` |
| `fail_pct` | `ROUND(100.0 * failed_rows / total_rows, 2)` |
| `evaluated_at` | Metric computation timestamp |

**Write mode:** Overwrite for the current `run_id` on full pipeline rerun (Community Edition default per `design-notes.md`).

**Querying latest run:**

```sql
SELECT * FROM silver_dq_metrics
WHERE run_id = (SELECT MAX(run_id) FROM silver_dq_metrics)
ORDER BY entity, check_category, rule_id;
```

**Row count invariant:** `silver_customers` row count = `bronze_customers` row count (same for orders and products). Metrics must never be improved by deleting rows.

---

## Metric Calculation Standard

For every rule:

```
total_rows  = COUNT(*) FROM silver_<entity>   -- all rows, including invalid
failed_rows = COUNT(*) WHERE <rule failure condition>
passed_rows = total_rows - failed_rows
pass_pct    = ROUND(100.0 * passed_rows / total_rows, 2)
fail_pct    = ROUND(100.0 * failed_rows / total_rows, 2)
```

Uniqueness rules flag **all rows** in a duplicate key group (not just the "second" occurrence).

Referential integrity rules: NULL FK fails completeness first; orphan non-NULL FK fails the RI rule.

---

# 1. Completeness Checks

---

### CHECK-C01: Customer ID Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer ID Not Null |
| **Rule ID** | `customers_customer_id_not_null` |
| **Business purpose** | Every customer must have a unique identifier for CRM, marketing, and order linkage |
| **Source table** | `silver_customers` (from `bronze_customers`) |
| **Columns** | `customer_id` |
| **Rule** | `customer_id IS NOT NULL` after cast from Bronze |
| **Expected result** | All rows have a non-null `customer_id` |
| **Failure condition** | `customer_id IS NULL` |
| **Row-level flag** | Rule ID added to `dq_failed_rules`; `dq_completeness_pass = false`; `dq_is_valid = false` |
| **Dataset-level metric** | Row in `silver_dq_metrics` where `entity = 'customers'`, `rule_id = 'customers_customer_id_not_null'` |
| **Pass percentage** | `pass_pct` in metrics row |
| **Failure percentage** | `fail_pct` in metrics row |
| **Example bad record** | `{ customer_id: NULL, customer_name: "Jane Doe", email: "jane@example.com", ... }` |
| **Test proof** | Unit test with one NULL `customer_id` row → `failed_rows = 1`. Full dataset: `failed_rows = 0` unless generator injects NULL PKs (not an assignment intentional issue). |

---

### CHECK-C02: Customer Name Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Name Not Null |
| **Rule ID** | `customers_customer_name_not_null` |
| **Business purpose** | Customer name is required for support, invoicing, and reporting |
| **Source table** | `silver_customers` |
| **Columns** | `customer_name` |
| **Rule** | `customer_name IS NOT NULL AND TRIM(customer_name) <> ''` |
| **Expected result** | All rows have a non-blank name |
| **Failure condition** | NULL or empty/whitespace-only `customer_name` |
| **Row-level flag** | `dq_failed_rules` += `customers_customer_name_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` row for `customers_customer_name_not_null` |
| **Pass percentage** | `(total - failed) / total * 100` |
| **Failure percentage** | `failed / total * 100` |
| **Example bad record** | `{ customer_id: 42, customer_name: "", email: "x@y.com", ... }` |
| **Test proof** | Fixture with blank name → row appears in `WHERE array_contains(dq_failed_rules, 'customers_customer_name_not_null')`. |

---

### CHECK-C03: Customer Email Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Email Not Null |
| **Rule ID** | `customers_email_not_null` |
| **Business purpose** | Email is the primary digital contact channel for notifications and marketing |
| **Source table** | `silver_customers` |
| **Columns** | `email` |
| **Rule** | `email IS NOT NULL AND TRIM(email) <> ''` |
| **Expected result** | All rows have a populated email |
| **Failure condition** | NULL or empty/whitespace `email` |
| **Row-level flag** | `dq_failed_rules` += `customers_email_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` → `failed_rows` |
| **Pass percentage** | `pass_pct` ≈ 99.50% on full generated data (~9,950 / 10,000) |
| **Failure percentage** | `fail_pct` ≈ 0.50% (~50 / 10,000) |
| **Example bad record** | `{ customer_id: 1001, customer_name: "Alice Smith", email: NULL, country: "US", ... }` |
| **Test proof** | **Assignment intentional issue.** After full pipeline run: `SELECT failed_rows FROM silver_dq_metrics WHERE rule_id = 'customers_email_not_null'` ≈ **50**. `SELECT COUNT(*) FROM silver_customers WHERE array_contains(dq_failed_rules, 'customers_email_not_null')` ≈ **50**. Silver row count remains **~10,000**. |

---

### CHECK-C04: Order ID Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Order ID Not Null |
| **Rule ID** | `orders_order_id_not_null` |
| **Business purpose** | Every order line must be uniquely identifiable |
| **Source table** | `silver_orders` |
| **Columns** | `order_id` |
| **Rule** | `order_id IS NOT NULL` |
| **Expected result** | All order rows have `order_id` |
| **Failure condition** | `order_id IS NULL` |
| **Row-level flag** | `dq_failed_rules` += `orders_order_id_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` for `orders_order_id_not_null` |
| **Pass percentage** | ~100% on generated data (not an intentional issue) |
| **Failure percentage** | ~0% unless generator adds NULL PKs |
| **Example bad record** | `{ order_id: NULL, customer_id: 1, product_id: 10, ... }` |
| **Test proof** | Unit fixture asserts flag; full data expects `failed_rows = 0`. |

---

### CHECK-C05: Order Customer ID Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Customer ID Not Null |
| **Rule ID** | `orders_customer_id_not_null` |
| **Business purpose** | Every order must be attributable to a customer for revenue and support |
| **Source table** | `silver_orders` |
| **Columns** | `customer_id` |
| **Rule** | `customer_id IS NOT NULL` |
| **Expected result** | All orders reference a customer |
| **Failure condition** | `customer_id IS NULL` |
| **Row-level flag** | `dq_failed_rules` += `orders_customer_id_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` → `failed_rows` |
| **Pass percentage** | `pass_pct` ≈ 99.90% (~99,900 / 100,000) |
| **Failure percentage** | `fail_pct` ≈ 0.10% (~100 / 100,000) |
| **Example bad record** | `{ order_id: 50001, customer_id: NULL, product_id: 25, order_status: "Completed", ... }` |
| **Test proof** | **Assignment intentional issue.** `failed_rows` for `orders_customer_id_not_null` ≈ **100**. Row retained in `silver_orders`; count still **~100,000**. |

---

### CHECK-C06: Order Product ID Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Product ID Not Null |
| **Rule ID** | `orders_product_id_not_null` |
| **Business purpose** | Every order must reference a product for catalog and revenue reporting |
| **Source table** | `silver_orders` |
| **Columns** | `product_id` |
| **Rule** | `product_id IS NOT NULL` |
| **Expected result** | All orders reference a product |
| **Failure condition** | `product_id IS NULL` |
| **Row-level flag** | `dq_failed_rules` += `orders_product_id_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` → `failed_rows` |
| **Pass percentage** | `pass_pct` ≈ 99.80% (~99,800 / 100,000) |
| **Failure percentage** | `fail_pct` ≈ 0.20% (~200 / 100,000) |
| **Example bad record** | `{ order_id: 50002, customer_id: 10, product_id: NULL, ... }` |
| **Test proof** | **Assignment intentional issue.** `failed_rows` ≈ **200**. |

---

### CHECK-C07: Product ID Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Product ID Not Null |
| **Rule ID** | `products_product_id_not_null` |
| **Business purpose** | Every product must have a stable catalog identifier |
| **Source table** | `silver_products` |
| **Columns** | `product_id` |
| **Rule** | `product_id IS NOT NULL` |
| **Expected result** | All product rows have `product_id` |
| **Failure condition** | `product_id IS NULL` |
| **Row-level flag** | `dq_failed_rules` += `products_product_id_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` for `products_product_id_not_null` |
| **Pass percentage** | ~100% on generated data |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: NULL, product_name: "Widget", ... }` |
| **Test proof** | Unit fixture only; no assignment intentional NULL PKs for products. |

---

### CHECK-C08: Product Name Not Null

| Attribute | Value |
|-----------|-------|
| **Check name** | Product Name Not Null |
| **Rule ID** | `products_product_name_not_null` |
| **Business purpose** | Products must be named for catalog display and sales reporting |
| **Source table** | `silver_products` |
| **Columns** | `product_name` |
| **Rule** | `product_name IS NOT NULL AND TRIM(product_name) <> ''` |
| **Expected result** | All products have a non-blank name |
| **Failure condition** | NULL or blank `product_name` |
| **Row-level flag** | `dq_failed_rules` += `products_product_name_not_null`; `dq_completeness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% on generated data |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: 1, product_name: "  ", category: "Electronics", ... }` |
| **Test proof** | Unit fixture with blank name. |

---

# 2. Uniqueness Checks

---

### CHECK-U01: Customer ID Unique

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer ID Unique |
| **Rule ID** | `customers_customer_id_unique` |
| **Business purpose** | Primary key must uniquely identify one customer; duplicates break joins and inflate metrics |
| **Source table** | `silver_customers` |
| **Columns** | `customer_id` |
| **Rule** | `customer_id` appears exactly once in `silver_customers` (among non-null IDs) |
| **Expected result** | No duplicate `customer_id` values |
| **Failure condition** | Row's `customer_id` is in a group with `COUNT(*) > 1` |
| **Row-level flag** | **All rows** in duplicate groups get `dq_failed_rules` += `customers_customer_id_unique`; `dq_uniqueness_pass = false` |
| **Dataset-level metric** | `failed_rows` = count of rows in duplicate groups |
| **Pass percentage** | `pass_pct` ≈ 99.79–99.90% depending on duplicate row count (see Expected Issues) |
| **Failure percentage** | `fail_pct` ≈ 0.10–0.21% |
| **Example bad record** | Two rows: `{ customer_id: 2001, ... }` and `{ customer_id: 2001, ... }` (different names/emails) |
| **Test proof** | **Assignment intentional issue:** 10 duplicate `customer_id` **values** → at least **11 rows** fail (minimum 10 pairs = 20 rows if each key duplicated once). Generator must document exact row count; test asserts `failed_rows >= 11` and `COUNT(DISTINCT customer_id WHERE uniqueness failed) = 10`. All duplicate rows retained. |

---

### CHECK-U02: Order ID Unique

| Attribute | Value |
|-----------|-------|
| **Check name** | Order ID Unique |
| **Rule ID** | `orders_order_id_unique` |
| **Business purpose** | Order primary key must be unique for transactional integrity |
| **Source table** | `silver_orders` |
| **Columns** | `order_id` |
| **Rule** | `order_id` appears exactly once among non-null IDs |
| **Expected result** | No duplicate `order_id` values |
| **Failure condition** | Row's `order_id` in duplicate group |
| **Row-level flag** | All rows in duplicate groups flagged; `dq_uniqueness_pass = false` |
| **Dataset-level metric** | `failed_rows` in `silver_dq_metrics` |
| **Pass percentage** | `pass_pct` ≈ 99.98% if 20 rows in duplicate groups / 100,000 |
| **Failure percentage** | `fail_pct` ≈ 0.02% |
| **Example bad record** | Two rows with `order_id: 88001`, different `quantity` or `total_amount` |
| **Test proof** | **Assignment intentional issue.** `failed_rows` for `orders_order_id_unique` = **20** (all rows participating in duplicate `order_id` groups). `SELECT COUNT(*) FROM silver_orders WHERE array_contains(dq_failed_rules, 'orders_order_id_unique')` = **20**. |

---

### CHECK-U03: Product ID Unique

| Attribute | Value |
|-----------|-------|
| **Check name** | Product ID Unique |
| **Rule ID** | `products_product_id_unique` |
| **Business purpose** | Catalog primary key must be unique |
| **Source table** | `silver_products` |
| **Columns** | `product_id` |
| **Rule** | `product_id` unique among non-null values |
| **Expected result** | No duplicate `product_id` |
| **Failure condition** | Row in duplicate `product_id` group |
| **Row-level flag** | `dq_failed_rules` += `products_product_id_unique`; `dq_uniqueness_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | Two rows with `product_id: 99` |
| **Test proof** | Unit fixture; no assignment intentional duplicates for products. |

---

# 3. Referential Integrity Checks

RI checks apply to **`silver_orders` only**. Parent key sets are built from **`silver_customers`** and **`silver_products`** (or Bronze if Silver not yet finalized — implementation uses distinct valid `customer_id` / `product_id` from ingested customer/product data).

NULL foreign keys are caught by completeness checks; RI rules apply when FK is **not null**.

---

### CHECK-R01: Order Customer ID Exists

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Customer ID Exists |
| **Rule ID** | `orders_customer_id_exists` |
| **Business purpose** | Orders must reference a real customer to support customer revenue and segmentation |
| **Source table** | `silver_orders` |
| **Columns** | `customer_id` |
| **Rule** | If `customer_id IS NOT NULL`, then `customer_id IN (SELECT customer_id FROM silver_customers WHERE customer_id IS NOT NULL)` |
| **Expected result** | Every non-null `customer_id` resolves to a customer |
| **Failure condition** | `customer_id IS NOT NULL AND customer_id NOT IN (valid customer ids)` |
| **Row-level flag** | `dq_failed_rules` += `orders_customer_id_exists`; `dq_referential_pass = false` |
| **Dataset-level metric** | `failed_rows` in `silver_dq_metrics` |
| **Pass percentage** | `pass_pct` ≈ 99.95% (~99,950 / 100,000) |
| **Failure percentage** | `fail_pct` ≈ 0.05% (~50 / 100,000) |
| **Example bad record** | `{ order_id: 60001, customer_id: 999999, product_id: 5, ... }` where 999999 not in customers |
| **Test proof** | **Assignment intentional issue.** `failed_rows` ≈ **50**. Rows with NULL `customer_id` fail CHECK-C05 but are **not** counted as RI failures (RI skipped when NULL). |

---

### CHECK-R02: Order Product ID Exists

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Product ID Exists |
| **Rule ID** | `orders_product_id_exists` |
| **Business purpose** | Orders must reference a valid catalog product for product sales reporting |
| **Source table** | `silver_orders` |
| **Columns** | `product_id` |
| **Rule** | If `product_id IS NOT NULL`, then `product_id IN (SELECT product_id FROM silver_products WHERE product_id IS NOT NULL)` |
| **Expected result** | Every non-null `product_id` exists in products |
| **Failure condition** | `product_id IS NOT NULL AND product_id NOT IN (valid product ids)` |
| **Row-level flag** | `dq_failed_rules` += `orders_product_id_exists`; `dq_referential_pass = false` |
| **Dataset-level metric** | `failed_rows` |
| **Pass percentage** | `pass_pct` ≈ 99.97% (~99,970 / 100,000) |
| **Failure percentage** | `fail_pct` ≈ 0.03% (~30 / 100,000) |
| **Example bad record** | `{ order_id: 60002, customer_id: 10, product_id: 888888, ... }` |
| **Test proof** | **Assignment intentional issue.** `failed_rows` ≈ **30**. |

---

# 4. Type Validation Checks

Type validation runs after Bronze → Silver casting. Unparseable values result in NULL typed columns **and** a type validation failure where applicable.

---

### CHECK-T01: Customer Segment Valid Enum

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Segment Valid |
| **Rule ID** | `customers_segment_valid` |
| **Business purpose** | Segmentation drives marketing and Gold segment reporting |
| **Source table** | `silver_customers` |
| **Columns** | `customer_segment` |
| **Rule** | `customer_segment IN ('Premium', 'Standard', 'Basic')` |
| **Expected result** | All segments are valid enum values |
| **Failure condition** | NULL or value not in allowed set |
| **Row-level flag** | `dq_failed_rules` += `customers_segment_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics`, `check_category = 'type_business'` |
| **Pass percentage** | ~100% on generated data |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ customer_id: 1, customer_segment: "Gold", ... }` |
| **Test proof** | Unit fixture with invalid segment. |

---

### CHECK-T02: Customer Signup Date Valid

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Signup Date Valid |
| **Rule ID** | `customers_signup_date_valid` |
| **Business purpose** | Signup date supports cohort and tenure analysis |
| **Source table** | `silver_customers` |
| **Columns** | `signup_date` |
| **Rule** | `signup_date IS NOT NULL` and parseable from Bronze; `signup_date <= current_date()` |
| **Expected result** | Valid date, not in the future |
| **Failure condition** | Unparseable Bronze value (NULL after cast), or future date |
| **Row-level flag** | `dq_failed_rules` += `customers_signup_date_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | Bronze `signup_date = "2026-13-45"` → NULL or invalid |
| **Test proof** | Unit fixture with bad date string. |

---

### CHECK-T03: Customer Lifetime Value Valid

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Lifetime Value Valid |
| **Rule ID** | `customers_lifetime_value_valid` |
| **Business purpose** | Source LTV must be a valid non-negative monetary value |
| **Source table** | `silver_customers` |
| **Columns** | `lifetime_value` |
| **Rule** | Parseable DECIMAL; `lifetime_value >= 0` |
| **Expected result** | Non-negative decimal |
| **Failure condition** | Unparseable or negative value |
| **Row-level flag** | `dq_failed_rules` += `customers_lifetime_value_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ customer_id: 1, lifetime_value: -100.00, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T04: Order Status Valid Enum

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Status Valid |
| **Rule ID** | `orders_status_valid` |
| **Business purpose** | Status drives fulfillment workflow and Gold revenue filtering |
| **Source table** | `silver_orders` |
| **Columns** | `order_status` |
| **Rule** | `order_status IN ('Pending', 'Completed', 'Cancelled')` |
| **Expected result** | Valid status enum |
| **Failure condition** | NULL or not in allowed set |
| **Row-level flag** | `dq_failed_rules` += `orders_status_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_id: 1, order_status: "Shipped", ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T05: Order Quantity Positive

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Quantity Positive |
| **Rule ID** | `orders_quantity_positive` |
| **Business purpose** | Order quantity must be a positive integer for inventory and revenue |
| **Source table** | `silver_orders` |
| **Columns** | `quantity` |
| **Rule** | Parseable INT; `quantity > 0` |
| **Expected result** | Positive integer quantity |
| **Failure condition** | NULL, unparseable, zero, or negative |
| **Row-level flag** | `dq_failed_rules` += `orders_quantity_positive`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_id: 1, quantity: -2, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T06: Order Unit Price Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Unit Price Non-Negative |
| **Rule ID** | `orders_unit_price_non_negative` |
| **Business purpose** | Unit price must be a valid monetary value |
| **Source table** | `silver_orders` |
| **Columns** | `unit_price` |
| **Rule** | Parseable DECIMAL; `unit_price >= 0` |
| **Expected result** | Non-negative decimal |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `orders_unit_price_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_id: 1, unit_price: -10.00, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T07: Order Total Amount Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Total Amount Non-Negative |
| **Rule ID** | `orders_total_amount_non_negative` |
| **Business purpose** | Total amount must be a valid monetary value |
| **Source table** | `silver_orders` |
| **Columns** | `total_amount` |
| **Rule** | Parseable DECIMAL; `total_amount >= 0` |
| **Expected result** | Non-negative decimal |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `orders_total_amount_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_id: 1, total_amount: -50.00, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T08: Order Date Valid

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Date Valid |
| **Rule ID** | `orders_order_date_valid` |
| **Business purpose** | Order date is required for time-series and Gold filtering |
| **Source table** | `silver_orders` |
| **Columns** | `order_date` |
| **Rule** | Parseable DATE; `order_date <= current_date()` |
| **Expected result** | Valid date not in future |
| **Failure condition** | NULL after cast or future date |
| **Row-level flag** | `dq_failed_rules` += `orders_order_date_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_id: 1, order_date: NULL, ... }` (unparseable Bronze) |
| **Test proof** | Unit fixture. |

---

### CHECK-T09: Product Price Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Product Price Non-Negative |
| **Rule ID** | `products_price_non_negative` |
| **Business purpose** | List price must be valid for catalog and margin analysis |
| **Source table** | `silver_products` |
| **Columns** | `price` |
| **Rule** | Parseable DECIMAL; `price >= 0` |
| **Expected result** | Non-negative price |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `products_price_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: 1, price: -1.00, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T10: Product Cost Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Product Cost Non-Negative |
| **Rule ID** | `products_cost_non_negative` |
| **Business purpose** | Cost must be valid for margin reporting |
| **Source table** | `silver_products` |
| **Columns** | `cost` |
| **Rule** | Parseable DECIMAL; `cost >= 0` |
| **Expected result** | Non-negative cost |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `products_cost_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: 1, cost: -5.00, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T11: Product Stock Quantity Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Product Stock Quantity Non-Negative |
| **Rule ID** | `products_stock_quantity_non_negative` |
| **Business purpose** | Inventory counts cannot be negative |
| **Source table** | `silver_products` |
| **Columns** | `stock_quantity` |
| **Rule** | Parseable INT; `stock_quantity >= 0` |
| **Expected result** | Non-negative integer |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `products_stock_quantity_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: 1, stock_quantity: -10, ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-T12: Product Reorder Level Non-Negative

| Attribute | Value |
|-----------|-------|
| **Check name** | Product Reorder Level Non-Negative |
| **Rule ID** | `products_reorder_level_non_negative` |
| **Business purpose** | Reorder threshold must be a valid inventory parameter |
| **Source table** | `silver_products` |
| **Columns** | `reorder_level` |
| **Rule** | Parseable INT; `reorder_level >= 0` |
| **Expected result** | Non-negative integer |
| **Failure condition** | Unparseable or negative |
| **Row-level flag** | `dq_failed_rules` += `products_reorder_level_non_negative`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ product_id: 1, reorder_level: -1, ... }` |
| **Test proof** | Unit fixture. |

---

# 5. Business-Rule Validation Checks

---

### CHECK-B01: Order Total Amount Matches Quantity × Unit Price

| Attribute | Value |
|-----------|-------|
| **Check name** | Order Total Amount Matches |
| **Rule ID** | `orders_total_amount_matches` |
| **Business purpose** | Line total must be arithmetically consistent for revenue accuracy |
| **Source table** | `silver_orders` |
| **Columns** | `quantity`, `unit_price`, `total_amount` |
| **Rule** | When all three are non-null: `ABS(total_amount - (quantity * unit_price)) < 0.01` (tolerance for DECIMAL rounding) |
| **Expected result** | Total equals quantity × unit price within tolerance |
| **Failure condition** | Mismatch beyond tolerance |
| **Row-level flag** | `dq_failed_rules` += `orders_total_amount_matches`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics`, `check_category = 'type_business'` |
| **Pass percentage** | ~100% on generated clean data |
| **Failure percentage** | ~0% unless generator injects mismatches |
| **Example bad record** | `{ order_id: 1, quantity: 2, unit_price: 10.00, total_amount: 25.00, ... }` |
| **Test proof** | Unit fixture with deliberate mismatch. |

---

### CHECK-B02: Completed Order Has Payment Date

| Attribute | Value |
|-----------|-------|
| **Check name** | Completed Order Has Payment Date |
| **Rule ID** | `orders_completed_has_payment_date` |
| **Business purpose** | Completed orders should have payment recorded for cash-flow reporting |
| **Source table** | `silver_orders` |
| **Columns** | `order_status`, `payment_date` |
| **Rule** | If `order_status = 'Completed'`, then `payment_date IS NOT NULL` |
| **Expected result** | Completed orders have payment date |
| **Failure condition** | `order_status = 'Completed' AND payment_date IS NULL` |
| **Row-level flag** | `dq_failed_rules` += `orders_completed_has_payment_date`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | Depends on generator (Pending/Cancelled may have NULL payment legitimately) |
| **Failure percentage** | Small % if generator creates Completed + NULL payment |
| **Example bad record** | `{ order_id: 1, order_status: "Completed", payment_date: NULL, ... }` |
| **Test proof** | Unit fixture; not an assignment-counted intentional issue unless generator adds them. |

---

### CHECK-B03: Payment Date Not Before Order Date

| Attribute | Value |
|-----------|-------|
| **Check name** | Payment Date Not Before Order Date |
| **Rule ID** | `orders_payment_date_after_order_date` |
| **Business purpose** | Payment cannot occur before order placement |
| **Source table** | `silver_orders` |
| **Columns** | `order_date`, `payment_date` |
| **Rule** | If both non-null: `payment_date >= order_date` |
| **Expected result** | Payment on or after order date |
| **Failure condition** | `payment_date < order_date` |
| **Row-level flag** | `dq_failed_rules` += `orders_payment_date_after_order_date`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ order_date: "2024-06-01", payment_date: "2024-05-01", ... }` |
| **Test proof** | Unit fixture. |

---

### CHECK-B04: Customer Email Format (Optional)

| Attribute | Value |
|-----------|-------|
| **Check name** | Customer Email Format |
| **Rule ID** | `customers_email_format_valid` |
| **Business purpose** | Basic format check for contactability |
| **Source table** | `silver_customers` |
| **Columns** | `email` |
| **Rule** | If `email IS NOT NULL`: matches pattern `^[^@]+@[^@]+\.[^@]+$` (simple check) |
| **Expected result** | Plausible email format when present |
| **Failure condition** | Non-null email fails pattern |
| **Row-level flag** | `dq_failed_rules` += `customers_email_format_valid`; `dq_type_business_pass = false` |
| **Dataset-level metric** | `silver_dq_metrics` |
| **Pass percentage** | ~100% on generated data |
| **Failure percentage** | ~0% |
| **Example bad record** | `{ customer_id: 1, email: "not-an-email", ... }` |
| **Test proof** | Unit fixture only. **Optional** — not required for assignment acceptance. |

---

## Consolidation Logic

After all checks run, `create_silver_tables.py` applies:

```text
dq_is_valid           = (dq_failure_count = 0)
dq_failure_count      = size(dq_failed_rules)
dq_completeness_pass  = NOT array_contains(dq_failed_rules, <any completeness rule id>)
dq_uniqueness_pass    = NOT array_contains(dq_failed_rules, <any uniqueness rule id>)
dq_referential_pass   = NOT array_contains(dq_failed_rules, <any RI rule id>)
dq_type_business_pass = NOT array_contains(dq_failed_rules, <any type/business rule id>)
```

Category booleans are computed by checking whether **any** rule from that category appears in `dq_failed_rules`.

---

## Gold Layer Interaction

Gold reads Silver with explicit filters (documented in Gold SQL):

- `dq_is_valid = true`
- `order_status = 'Completed'` for revenue metrics

Invalid rows remain in Silver for audit; they are **excluded from aggregates only**, not deleted.

---

## Test Strategy Summary

| Test type | What it validates |
|-----------|-------------------|
| **Generator test** | Intentional issue row counts in CSV before pipeline |
| **Row count invariant** | `COUNT(bronze_*) = COUNT(silver_*)` per entity |
| **Rule metric test** | `silver_dq_metrics.failed_rows` matches expected for each intentional issue |
| **Row flag test** | `array_contains(dq_failed_rules, '<rule_id>')` count matches metric |
| **No-delete test** | Total Silver rows unchanged after DQ; only flags updated |
| **Multi-failure test** | Single row with NULL `customer_id` + NULL `product_id` has `dq_failure_count >= 2` |
| **Gold exclusion test** | Invalid rows do not appear in Gold numerators |

---

## Expected Intentional Data Quality Issues

Every issue specified in the assignment, with expected approximate detection counts after a full pipeline run on generated sample data.

### customers.csv (~10,000 rows)

| # | Issue | Assignment Count | Rule ID(s) | Expected `failed_rows` in Metrics | Notes |
|---|-------|------------------|------------|-----------------------------------|-------|
| 1 | NULL `email` | **50 rows** | `customers_email_not_null` | **50** | Completeness failure; rows retained in Silver |
| 2 | Duplicate `customer_id` values | **10 duplicate key values** | `customers_customer_id_unique` | **≥ 11 rows** (typically **20** if each of 10 keys appears exactly twice) | All rows in duplicate groups flagged; exact row count documented in `DATA_GENERATION_NOTES.md` |

**Products:** No intentional issues specified in the assignment.

### orders.csv (~100,000 rows)

| # | Issue | Assignment Count | Rule ID(s) | Expected `failed_rows` in Metrics | Notes |
|---|-------|------------------|------------|-----------------------------------|-------|
| 3 | NULL `customer_id` | **100 rows** | `orders_customer_id_not_null` | **100** | Completeness; RI rule not applied when NULL |
| 4 | NULL `product_id` | **200 rows** | `orders_product_id_not_null` | **200** | Completeness |
| 5 | `customer_id` not in customers | **50 rows** | `orders_customer_id_exists` | **50** | Referential integrity; requires non-null orphan FK |
| 6 | `product_id` not in products | **30 rows** | `orders_product_id_exists` | **30** | Referential integrity |
| 7 | Duplicate `order_id` rows | **20 rows** | `orders_order_id_unique` | **20** | All rows participating in duplicate groups |

### Cross-Rule Overlap

Some rows may fail **multiple** rules (e.g., NULL `customer_id` also cannot pass RI). Each rule's `failed_rows` is counted **independently** per rule — metrics sum of failures can exceed count of distinct invalid rows. Distinct invalid row count:

```sql
SELECT COUNT(*) FROM silver_orders WHERE dq_is_valid = false;
```

This may be **less than** the sum of per-rule `failed_rows` due to overlap.

### Validation Query Template

```sql
-- Verify intentional customer email NULLs caught
SELECT rule_id, failed_rows, fail_pct
FROM silver_dq_metrics
WHERE run_id = :latest_run_id
  AND rule_id IN (
    'customers_email_not_null',
    'customers_customer_id_unique',
    'orders_customer_id_not_null',
    'orders_product_id_not_null',
    'orders_customer_id_exists',
    'orders_product_id_exists',
    'orders_order_id_unique'
  );
```

Expected approximate results:

| rule_id | failed_rows |
|---------|-------------|
| `customers_email_not_null` | 50 |
| `customers_customer_id_unique` | ≥ 11 (confirm in generator notes) |
| `orders_customer_id_not_null` | 100 |
| `orders_product_id_not_null` | 200 |
| `orders_customer_id_exists` | 50 |
| `orders_product_id_exists` | 30 |
| `orders_order_id_unique` | 20 |

### Row Count Sanity (Proves No Silent Deletion)

| Table | Expected Row Count |
|-------|-------------------|
| `bronze_customers` | ~10,000 |
| `silver_customers` | ~10,000 (equal to Bronze) |
| `bronze_orders` | ~100,000 |
| `silver_orders` | ~100,000 (equal to Bronze) |
| `bronze_products` | ~500 |
| `silver_products` | ~500 (equal to Bronze) |

---

## Related Documents

- `requirements-analysis.md` — Requirements and acceptance criteria
- `design-notes.md` — Architecture and rerun strategy
- `data-model.md` — Table schemas and column definitions
