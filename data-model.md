# Data Model

This document defines source entities, layer-specific table schemas, and naming conventions for the e-commerce Medallion pipeline. It aligns with `requirements-analysis.md` and `design-notes.md`.

**Naming convention:** `{catalog}.{schema}.{table}` — on Databricks Community Edition, use `hive_metastore.ecommerce` or a single configured schema (e.g., `ecommerce.bronze_customers` if schema is `ecommerce`).

---

## Table Name Summary

### Bronze Tables

| Table Name | Source | Description |
|------------|--------|-------------|
| `bronze_customers` | `customers.csv` | Raw customer records |
| `bronze_orders` | `orders.csv` | Raw order records |
| `bronze_products` | `products.csv` | Raw product records |

### Silver Tables

| Table Name | Description |
|------------|-------------|
| `silver_customers` | Typed customers with row-level DQ columns |
| `silver_orders` | Typed orders with row-level DQ columns |
| `silver_products` | Typed products with row-level DQ columns |
| `silver_dq_metrics` | Aggregated pass/fail metrics per rule and run |

**Optional:**

| Table Name | Description |
|------------|-------------|
| `silver_dq_row_results` | Normalized long-format row-level rule results (optional audit table) |

**Optional views (convenience, not required):**

| View Name | Definition |
|-----------|------------|
| `silver_customers_valid` | `SELECT * FROM silver_customers WHERE dq_is_valid = true` |
| `silver_orders_valid` | `SELECT * FROM silver_orders WHERE dq_is_valid = true` |
| `silver_products_valid` | `SELECT * FROM silver_products WHERE dq_is_valid = true` |

### Gold Tables

| Table Name | SQL Asset | Description |
|------------|-----------|-------------|
| `gold_sales_by_product` | `01_sales_by_product.sql` | Product-level sales metrics |
| `gold_revenue_by_customer` | `02_revenue_by_customer.sql` | Customer-level revenue metrics |
| `gold_customer_segmentation` | `04_customer_segmentation.sql` | Segment-level summary |

**Optional:**

| Table Name | SQL Asset | Description |
|------------|-----------|-------------|
| `gold_daily_weekly_trends` | `03_daily_weekly_trends.sql` | Time-based trends (stretch goal) |

---

## Source Entities

These are the logical models for the three CSV source datasets. Silver typed columns follow these definitions. Bronze stores source values as strings (see Bronze section).

---

### customers

**Description:** Customer master data from the customer database.

**Primary key:** `customer_id`

**Expected volume:** ~10,000 rows

| Column | Data Type | Nullable | PK/FK | Business Meaning | Validation Rule |
|--------|-----------|----------|-------|------------------|-----------------|
| `customer_id` | INT | NOT NULL | PK | Unique identifier for a customer | Must be present; must be unique across all customer rows |
| `customer_name` | STRING | NOT NULL | — | Full or display name of the customer | Must be present; non-blank after trim |
| `email` | STRING | NULL allowed in source | — | Customer contact email address | Completeness: expected present for valid rows; NULL flagged (50 intentional bad rows). If present, should match basic email format |
| `country` | STRING | NOT NULL | — | Country of the customer | Must be present; non-blank after trim |
| `signup_date` | DATE | NOT NULL | — | Date the customer registered | Must be present; valid date; not in the future |
| `customer_segment` | STRING | NOT NULL | — | Marketing/service tier | Must be one of: `Premium`, `Standard`, `Basic` |
| `lifetime_value` | DECIMAL(18,2) | NOT NULL | — | Declared lifetime value from source system | Must be present; must be >= 0 |

**Intentional DQ issues (assignment):** 50 NULL `email`; 10 duplicate `customer_id` values.

---

### orders

**Description:** Order line transactions from the order system.

**Primary key:** `order_id`

**Foreign keys:**

- `customer_id` → `customers.customer_id`
- `product_id` → `products.product_id`

**Expected volume:** ~100,000 rows

| Column | Data Type | Nullable | PK/FK | Business Meaning | Validation Rule |
|--------|-----------|----------|-------|------------------|-----------------|
| `order_id` | INT | NOT NULL | PK | Unique identifier for an order line | Must be present; must be unique across all order rows |
| `customer_id` | INT | NULL allowed in source | FK → `customers.customer_id` | Customer who placed the order | Completeness: expected present for valid rows. RI: must exist in `customers` when not NULL |
| `order_date` | DATE | NOT NULL | — | Date the order was placed | Must be present; valid date; not in the future |
| `product_id` | INT | NULL allowed in source | FK → `products.product_id` | Product ordered | Completeness: expected present for valid rows. RI: must exist in `products` when not NULL |
| `quantity` | INT | NOT NULL | — | Number of units ordered | Must be present; must be > 0 |
| `unit_price` | DECIMAL(18,2) | NOT NULL | — | Price per unit at time of order | Must be present; must be >= 0 |
| `total_amount` | DECIMAL(18,2) | NOT NULL | — | Total monetary amount for the order line | Must be present; must be >= 0; should equal `quantity * unit_price` (business rule) |
| `order_status` | STRING | NOT NULL | — | Current status of the order | Must be one of: `Pending`, `Completed`, `Cancelled` |
| `payment_date` | DATE | NULL | — | Date payment was received | Nullable; if `order_status = 'Completed'`, expected to be present and >= `order_date` |

**Intentional DQ issues (assignment):** 100 NULL `customer_id`; 200 NULL `product_id`; 50 orphan `customer_id`; 30 orphan `product_id`; 20 duplicate `order_id` rows.

**Gold revenue filter (design decision):** Only `order_status = 'Completed'` orders with `dq_is_valid = true` contribute to Gold revenue metrics.

---

### products

**Description:** Product catalog master data.

**Primary key:** `product_id`

**Expected volume:** ~500 rows

| Column | Data Type | Nullable | PK/FK | Business Meaning | Validation Rule |
|--------|-----------|----------|-------|------------------|-----------------|
| `product_id` | INT | NOT NULL | PK | Unique identifier for a product | Must be present; must be unique across all product rows |
| `product_name` | STRING | NOT NULL | — | Name of the product | Must be present; non-blank after trim |
| `category` | STRING | NOT NULL | — | Product category for reporting | Must be present; non-blank after trim |
| `price` | DECIMAL(18,2) | NOT NULL | — | Current list price of the product | Must be present; must be >= 0 |
| `cost` | DECIMAL(18,2) | NOT NULL | — | Unit cost of the product | Must be present; must be >= 0 |
| `stock_quantity` | INT | NOT NULL | — | Current inventory on hand | Must be present; must be >= 0 |
| `reorder_level` | INT | NOT NULL | — | Inventory threshold triggering reorder | Must be present; must be >= 0 |

**Intentional DQ issues (assignment):** None specified. Standard validation still applies.

**Optional business rule:** `cost` <= `price` (flag if violated; not an assignment requirement).

---

## Bronze Layer Schemas

Bronze preserves raw CSV values. All source business columns are stored as **STRING** to prevent silent type coercion. Ingest metadata is appended.

### bronze_customers

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `customer_id` | STRING | NULL | Raw value from CSV |
| `customer_name` | STRING | NULL | Raw value from CSV |
| `email` | STRING | NULL | Raw value from CSV |
| `country` | STRING | NULL | Raw value from CSV |
| `signup_date` | STRING | NULL | Raw value from CSV |
| `customer_segment` | STRING | NULL | Raw value from CSV |
| `lifetime_value` | STRING | NULL | Raw value from CSV |
| `_ingest_ts` | TIMESTAMP | NOT NULL | UTC ingest timestamp |
| `_source_file` | STRING | NOT NULL | Source CSV file name |
| `_batch_id` | STRING | NOT NULL | Ingest batch / pipeline run ID |
| `_source_row_num` | BIGINT | NOT NULL | Row number from CSV (1-based, for lineage) |

**Validation:** None at Bronze layer.

---

### bronze_orders

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `order_id` | STRING | NULL | Raw value from CSV |
| `customer_id` | STRING | NULL | Raw value from CSV |
| `order_date` | STRING | NULL | Raw value from CSV |
| `product_id` | STRING | NULL | Raw value from CSV |
| `quantity` | STRING | NULL | Raw value from CSV |
| `unit_price` | STRING | NULL | Raw value from CSV |
| `total_amount` | STRING | NULL | Raw value from CSV |
| `order_status` | STRING | NULL | Raw value from CSV |
| `payment_date` | STRING | NULL | Raw value from CSV |
| `_ingest_ts` | TIMESTAMP | NOT NULL | UTC ingest timestamp |
| `_source_file` | STRING | NOT NULL | Source CSV file name |
| `_batch_id` | STRING | NOT NULL | Ingest batch / pipeline run ID |
| `_source_row_num` | BIGINT | NOT NULL | Row number from CSV (1-based) |

**Validation:** None at Bronze layer.

---

### bronze_products

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `product_id` | STRING | NULL | Raw value from CSV |
| `product_name` | STRING | NULL | Raw value from CSV |
| `category` | STRING | NULL | Raw value from CSV |
| `price` | STRING | NULL | Raw value from CSV |
| `cost` | STRING | NULL | Raw value from CSV |
| `stock_quantity` | STRING | NULL | Raw value from CSV |
| `reorder_level` | STRING | NULL | Raw value from CSV |
| `_ingest_ts` | TIMESTAMP | NOT NULL | UTC ingest timestamp |
| `_source_file` | STRING | NOT NULL | Source CSV file name |
| `_batch_id` | STRING | NOT NULL | Ingest batch / pipeline run ID |
| `_source_row_num` | BIGINT | NOT NULL | Row number from CSV (1-based) |

**Validation:** None at Bronze layer.

---

## Silver Layer Schemas

Silver contains typed business columns from source entities **plus** standardized data quality columns. Source lineage metadata is carried forward from Bronze.

### Common Silver DQ Columns (all entity tables)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `dq_is_valid` | BOOLEAN | NOT NULL | `true` if all quality checks pass for the row |
| `dq_failed_rules` | ARRAY&lt;STRING&gt; | NOT NULL | List of failed rule IDs (empty array if valid) |
| `dq_failure_count` | INT | NOT NULL | Count of failed rules |
| `dq_completeness_pass` | BOOLEAN | NOT NULL | Pass/fail for completeness category |
| `dq_uniqueness_pass` | BOOLEAN | NOT NULL | Pass/fail for uniqueness category |
| `dq_referential_pass` | BOOLEAN | NOT NULL | Pass/fail for referential integrity (orders only; `true` for customers/products) |
| `dq_type_business_pass` | BOOLEAN | NOT NULL | Pass/fail for type and business validation |
| `dq_checked_at` | TIMESTAMP | NOT NULL | Timestamp of last DQ evaluation |
| `dq_run_id` | STRING | NOT NULL | Pipeline run identifier |
| `_ingest_ts` | TIMESTAMP | NOT NULL | From Bronze |
| `_source_file` | STRING | NOT NULL | From Bronze |
| `_batch_id` | STRING | NOT NULL | From Bronze |
| `_source_row_num` | BIGINT | NOT NULL | From Bronze |

### silver_customers

Business columns match the **customers** source entity (typed). Plus common DQ columns above.

| Column | Data Type | Nullable | PK/FK | Validation Rule (Silver) |
|--------|-----------|----------|-------|--------------------------|
| `customer_id` | INT | NULL | PK | Parseable integer; uniqueness check |
| `customer_name` | STRING | NULL | — | Non-blank if present |
| `email` | STRING | NULL | — | Completeness check (`customers_email_not_null`) |
| `country` | STRING | NULL | — | Non-blank if present |
| `signup_date` | DATE | NULL | — | Parseable date; not future |
| `customer_segment` | STRING | NULL | — | Enum: Premium, Standard, Basic |
| `lifetime_value` | DECIMAL(18,2) | NULL | — | Parseable decimal; >= 0 |

**Key rule IDs:**

| Rule ID | Category |
|---------|----------|
| `customers_customer_id_not_null` | Completeness |
| `customers_email_not_null` | Completeness |
| `customers_customer_name_not_null` | Completeness |
| `customers_customer_id_unique` | Uniqueness |
| `customers_segment_valid` | Type/business |
| `customers_signup_date_valid` | Type/business |
| `customers_lifetime_value_valid` | Type/business |

---

### silver_orders

Business columns match the **orders** source entity (typed). Plus common DQ columns above.

| Column | Data Type | Nullable | PK/FK | Validation Rule (Silver) |
|--------|-----------|----------|-------|--------------------------|
| `order_id` | INT | NULL | PK | Parseable integer; uniqueness check |
| `customer_id` | INT | NULL | FK | Completeness + RI vs customers |
| `order_date` | DATE | NULL | — | Parseable date; not future |
| `product_id` | INT | NULL | FK | Completeness + RI vs products |
| `quantity` | INT | NULL | — | Parseable integer; > 0 |
| `unit_price` | DECIMAL(18,2) | NULL | — | Parseable decimal; >= 0 |
| `total_amount` | DECIMAL(18,2) | NULL | — | Parseable decimal; >= 0; equals qty × price |
| `order_status` | STRING | NULL | — | Enum: Pending, Completed, Cancelled |
| `payment_date` | DATE | NULL | — | Valid date; business rules vs status/date |

**Key rule IDs:**

| Rule ID | Category |
|---------|----------|
| `orders_order_id_not_null` | Completeness |
| `orders_customer_id_not_null` | Completeness |
| `orders_product_id_not_null` | Completeness |
| `orders_order_id_unique` | Uniqueness |
| `orders_customer_id_exists` | Referential integrity |
| `orders_product_id_exists` | Referential integrity |
| `orders_status_valid` | Type/business |
| `orders_quantity_positive` | Type/business |
| `orders_total_amount_matches` | Business logic |
| `orders_completed_has_payment_date` | Business logic |

---

### silver_products

Business columns match the **products** source entity (typed). Plus common DQ columns above.

| Column | Data Type | Nullable | PK/FK | Validation Rule (Silver) |
|--------|-----------|----------|-------|--------------------------|
| `product_id` | INT | NULL | PK | Parseable integer; uniqueness check |
| `product_name` | STRING | NULL | — | Non-blank if present |
| `category` | STRING | NULL | — | Non-blank if present |
| `price` | DECIMAL(18,2) | NULL | — | Parseable decimal; >= 0 |
| `cost` | DECIMAL(18,2) | NULL | — | Parseable decimal; >= 0 |
| `stock_quantity` | INT | NULL | — | Parseable integer; >= 0 |
| `reorder_level` | INT | NULL | — | Parseable integer; >= 0 |

**Key rule IDs:**

| Rule ID | Category |
|---------|----------|
| `products_product_id_not_null` | Completeness |
| `products_product_name_not_null` | Completeness |
| `products_product_id_unique` | Uniqueness |
| `products_price_non_negative` | Type/business |
| `products_cost_non_negative` | Type/business |

---

### silver_dq_metrics

Aggregated quality report per pipeline run.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `run_id` | STRING | NOT NULL | Pipeline run identifier |
| `entity` | STRING | NOT NULL | `customers`, `orders`, or `products` |
| `check_category` | STRING | NOT NULL | `completeness`, `uniqueness`, `referential_integrity`, `type_business` |
| `rule_id` | STRING | NOT NULL | Stable rule identifier |
| `rule_description` | STRING | NOT NULL | Human-readable rule description |
| `total_rows` | BIGINT | NOT NULL | Rows evaluated |
| `failed_rows` | BIGINT | NOT NULL | Rows that failed the rule |
| `passed_rows` | BIGINT | NOT NULL | `total_rows - failed_rows` |
| `pass_pct` | DECIMAL(5,2) | NOT NULL | Percentage passed |
| `fail_pct` | DECIMAL(5,2) | NOT NULL | Percentage failed |
| `evaluated_at` | TIMESTAMP | NOT NULL | When metrics were computed |

**Primary key (logical):** `(run_id, entity, rule_id)`

---

## Gold Layer Schemas

Gold tables are derived from validated Silver data. Filters applied unless noted:

- `dq_is_valid = true` on contributing Silver rows
- `order_status = 'Completed'` for order-based revenue metrics

---

### gold_sales_by_product

| Column | Data Type | Nullable | Description | Derivation |
|--------|-----------|----------|-------------|------------|
| `product_id` | INT | NOT NULL | Product identifier | From `silver_products` |
| `product_name` | STRING | NOT NULL | Product name | From `silver_products` |
| `category` | STRING | NOT NULL | Product category | From `silver_products` |
| `total_orders` | BIGINT | NOT NULL | Count of qualifying orders | `COUNT` of valid completed orders per product |
| `total_revenue` | DECIMAL(18,2) | NOT NULL | Sum of order revenue | `SUM(total_amount)` |
| `avg_order_value` | DECIMAL(18,2) | NULL | Average order value | `total_revenue / total_orders`; NULL if zero orders |

**Grain:** One row per product. Products with no qualifying orders appear with `total_orders = 0`, `total_revenue = 0`, `avg_order_value = NULL`.

---

### gold_revenue_by_customer

| Column | Data Type | Nullable | Description | Derivation |
|--------|-----------|----------|-------------|------------|
| `customer_id` | INT | NOT NULL | Customer identifier | From `silver_customers` |
| `customer_name` | STRING | NOT NULL | Customer name | From `silver_customers` |
| `customer_segment` | STRING | NOT NULL | Customer segment | From `silver_customers` |
| `total_orders` | BIGINT | NOT NULL | Count of qualifying orders | `COUNT` of valid completed orders per customer |
| `total_revenue` | DECIMAL(18,2) | NOT NULL | Sum of order revenue | `SUM(total_amount)` |
| `avg_order_value` | DECIMAL(18,2) | NULL | Average order value | `total_revenue / total_orders` |
| `lifetime_value_actual` | DECIMAL(18,2) | NOT NULL | Actual LTV from orders | `SUM(total_amount)` of qualifying orders |

**Grain:** One row per valid Silver customer. Customers with no qualifying orders have `total_orders = 0`, `total_revenue = 0`, `lifetime_value_actual = 0`.

---

### gold_customer_segmentation

| Column | Data Type | Nullable | Description | Derivation |
|--------|-----------|----------|-------------|------------|
| `segment_type` | STRING | NOT NULL | Customer segment | `customer_segment` from `silver_customers` |
| `customer_count` | BIGINT | NOT NULL | Customers in segment | `COUNT(DISTINCT customer_id)` of valid customers |
| `avg_revenue` | DECIMAL(18,2) | NULL | Average revenue per customer in segment | `total_revenue / customer_count` |
| `total_revenue` | DECIMAL(18,2) | NOT NULL | Total segment revenue | `SUM` of customer `total_revenue` from order joins |

**Grain:** One row per `segment_type` (`Premium`, `Standard`, `Basic`).

---

## Entity Relationship Diagram

```
┌─────────────────────┐         ┌─────────────────────┐
│     customers       │         │      products       │
│  PK: customer_id    │         │  PK: product_id     │
└──────────┬──────────┘         └──────────┬──────────┘
           │                               │
           │ 1                          1  │
           │                               │
           │ N                          N  │
┌──────────┴───────────────────────────────┴──────────┐
│                      orders                          │
│  PK: order_id                                        │
│  FK: customer_id → customers.customer_id             │
│  FK: product_id  → products.product_id              │
└──────────────────────────────────────────────────────┘
```

---

## Layer Comparison (customers example)

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| `customer_id` type | STRING | INT | INT (in `gold_revenue_by_customer`) |
| DQ columns | No | Yes | No |
| Bad rows | All retained | All retained, flagged | Invalid rows excluded from aggregates |
| Purpose | Raw archive | Validated SoR | Analytics |

---

## Related Documents

- `requirements-analysis.md` — Business requirements and acceptance criteria
- `design-notes.md` — Architecture, flow, and operational design
- `data-quality-strategy.md` — Detailed rule logic and metric validation (to be created)
- `database/schema.sql` — DDL reference (to be created)
