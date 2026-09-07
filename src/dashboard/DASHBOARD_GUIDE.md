# Databricks SQL Dashboard Guide

This guide walks through building the e-commerce analytics dashboard in **Databricks SQL Dashboard** using queries from `dashboard_queries.sql`. The dashboard is a read-only presentation layer on **Gold tables only** — no Bronze or Silver logic runs here.

---

## Prerequisites

1. **Pipeline Gold layer built** — run Silver first, then Gold:

   ```powershell
   python src/silver/create_silver_tables.py --schema ecommerce
   python src/gold/create_gold_tables.py --schema ecommerce
   ```

2. **SQL warehouse** — start or create a SQL warehouse in your Databricks workspace (Dashboards require an active warehouse).

3. **Schema** — queries default to `ecommerce`. If you use a different schema, change the `USE` statement in `dashboard_queries.sql` and update table references accordingly.

---

## Dashboard export PDFs (assignment proof)

Exported screenshots from the **published** Databricks dashboard (2026-09-07). Open these files in the repo to review the three required visuals without signing in to Databricks:

| # | Visualization | PDF file |
|---|---------------|----------|
| 1 | Top 10 Products by Revenue | [`exports/dashboard-01-top-10-products-by-revenue.pdf`](exports/dashboard-01-top-10-products-by-revenue.pdf) |
| 2 | Customer Revenue Distribution | [`exports/dashboard-02-customer-revenue-distribution.pdf`](exports/dashboard-02-customer-revenue-distribution.pdf) |
| 3 | Customer Segmentation by Behavior | [`exports/dashboard-03-customer-segmentation.pdf`](exports/dashboard-03-customer-segmentation.pdf) |

**Original export filenames (Downloads):**

- `Dashboards 2026-09-07 12_42.pdf` → Top 10 Products
- `Dashboards 2026-09-07 12_43.pdf` → Customer Revenue Distribution
- `Dashboards 2026-09-07 12_41.pdf` → Customer Segmentation

**Live dashboard:** https://dbc-6a498a86-07f2.cloud.databricks.com/dashboardsv3/01f1aab7db5713f8a43e09512edcaa70/published?o=7474655649363572

---

## 1. Required Gold Tables

All dashboard queries read from these four Gold tables (created by `create_gold_tables.py`):

| Gold table | Purpose | Used by |
|------------|---------|---------|
| `gold_sales_by_product` | Product-level revenue, order counts, AOV | Top 10 Products, Top Categories (optional) |
| `gold_revenue_by_customer` | Customer lifetime revenue and marketing tier | Customer Revenue Distribution, Revenue by Tier (optional) |
| `gold_customer_segmentation` | Behavioral segment aggregates | Customer Segmentation |
| `gold_daily_weekly_trends` | Daily/weekly revenue and order trends | Daily Revenue Trend (optional) |

### Shared business rules (affects all numbers)

- **Revenue** counts only `order_status = 'Completed'` orders (design D1).
- **Quality gate** — only `dq_is_valid = true` Silver rows feed Gold (design D2).
- **Duplicates** — duplicate `order_id` rows are excluded in Silver and deduplicated defensively in Gold.
- **Order status filter** — not available on the dashboard; Pending/Cancelled orders are excluded at the Gold layer by design.

---

## 2. Dashboard Setup Overview

### Step 1 — Confirm Gold data exists

In a **SQL query** editor, run:

```sql
USE ecommerce;

SELECT 'gold_sales_by_product' AS table_name, COUNT(*) AS row_count FROM gold_sales_by_product
UNION ALL
SELECT 'gold_revenue_by_customer', COUNT(*) FROM gold_revenue_by_customer
UNION ALL
SELECT 'gold_customer_segmentation', COUNT(*) FROM gold_customer_segmentation
UNION ALL
SELECT 'gold_daily_weekly_trends', COUNT(*) FROM gold_daily_weekly_trends;
```

Each table should return a non-zero row count (segmentation returns four segment rows).

### Step 2 — Choose how to filter (pick one)

**Option A — Run as-is (recommended to start)**  
Queries use a `dashboard_filters` CTE with literal defaults (`'ALL'`). Paste a query block and **Run** — no SQL parameters required.

To narrow a chart, edit the CTE in that query, for example:

```sql
WITH dashboard_filters AS (
    SELECT 'Electronics' AS product_category  -- was 'ALL'
)
```

**Option B — Interactive Lakeview filters**  
After adding a visualization, use **Add filter** on the dataset field (`category`, `customer_segment`, or `period_start_date`). Viewers change filters in the dashboard UI without editing SQL.

**Option C — SQL parameters (`:name`)**  
Only if you need query-level parameters: add each name under **Query → Parameters** with a default (e.g. `product_category` = `ALL`). The repo queries no longer use `:parameter` syntax by default because unset parameters cause `UNBOUND_SQL_PARAMETER` on serverless.

Discover valid category values:

```sql
SELECT DISTINCT category FROM gold_sales_by_product ORDER BY category;
```

### Step 3 — Create and save queries

For each visualization:

1. Open **SQL** → **Queries** → **Create query**.
2. Attach your SQL warehouse.
3. Copy the matching query block from `src/dashboard/dashboard_queries.sql`.
4. Ensure the query starts with `USE ecommerce;` (or your schema).
5. **Run** the query to confirm it returns rows.
6. **Save** the query with a clear name (e.g. `Dashboard — Top 10 Products by Revenue`).

### Step 4 — Add visualizations to the dashboard

1. Open your dashboard → **Add** → **Visualization**.
2. Select the saved query.
3. Configure chart type, axes, and fields per the sections below.
4. **Optional:** add **field filters** on `category`, `customer_segment`, or `period_start_date` for interactive filtering.
5. Arrange tiles and save the dashboard.

### Published dashboard (this project)

- **URL:** https://dbc-6a498a86-07f2.cloud.databricks.com/dashboardsv3/01f1aab7db5713f8a43e09512edcaa70/published?o=7474655649363572
- **Deliverable PDF:** `src/dashboard/E-Commerce-Gold-Analytics-Dashboard.pdf`
- **Deliverable notes:** `src/dashboard/DASHBOARD_DELIVERABLE.md`

---

## 3. Required Visualizations

---

### Visualization A — Top 10 Products by Revenue

#### Query setup

- **Source file:** `dashboard_queries.sql` — block labeled `VISUALIZATION 1: Top 10 Products by Revenue`
- **Gold table:** `gold_sales_by_product`
- **Saved query name (suggested):** `Dashboard — Top 10 Products by Revenue`

#### Visualization configuration

| Setting | Value |
|---------|-------|
| **Chart type** | Bar chart |
| **X-axis** | `product_name` |
| **Y-axis** | `total_revenue` |
| **Sort** | Descending by `total_revenue` (query already orders; confirm chart respects it) |
| **Optional tooltip / color** | `category`, `total_orders`, `avg_order_value` |

Use a **horizontal** bar chart if your Databricks UI offers orientation — long product names read better on the Y-axis with revenue on the X-axis.

#### Filters

| Filter | How to apply | Behavior |
|--------|----------------|----------|
| Product category | Edit `dashboard_filters.product_category` in the query, or add a field filter on `category` | `ALL` = all categories |

#### Suggested title

**Top 10 Products by Revenue**

#### Expected interpretation

- Shows which products drive the most **completed, valid-order** revenue.
- Useful for merchandising, inventory prioritization, and promo planning.
- When `product_category` is narrowed, the chart shows the top 10 within that category only.
- Products with no qualifying orders appear in Gold with zero revenue and typically rank below active sellers.

---

### Visualization B — Customer Revenue Distribution

#### Query setup

- **Source file:** `dashboard_queries.sql` — block labeled `VISUALIZATION 2: Customer Revenue Distribution`
- **Gold table:** `gold_revenue_by_customer`
- **Saved query name (suggested):** `Dashboard — Customer Revenue Distribution`

#### Visualization configuration

| Setting | Value |
|---------|-------|
| **Chart type** | Histogram (or **Bar chart** if Histogram is unavailable) |
| **X-axis** | `revenue_bucket` |
| **Y-axis** | `customer_count` |
| **Sort** | Ascending by bucket order (query uses `bucket_sort_order`; buckets must read left-to-right from low to high spend) |

**Fixed bucket boundaries (design D5):**

| Bucket | Customer lifetime revenue |
|--------|---------------------------|
| `$0` | Exactly zero |
| `$1 - $100` | > 0 and ≤ 100 |
| `$101 - $500` | > 100 and ≤ 500 |
| `$501 - $1,000` | > 500 and ≤ 1,000 |
| `$1,001 - $5,000` | > 1,000 and ≤ 5,000 |
| `$5,000+` | > 5,000 |

#### Filters

| Filter | Parameter | Behavior |
|--------|-----------|----------|
| Marketing tier | `customer_segment` | `ALL` = all customers; otherwise only `Premium`, `Standard`, or `Basic` |

This filter uses the **source customer segment** from the customer master, not the behavioral segments in the pie chart.

#### Suggested title

**Customer Revenue Distribution**

#### Expected interpretation

- Shows how customers are spread across **lifetime revenue** buckets (completed valid orders only).
- A tall bar at `$0` indicates many registered customers with no qualifying purchases.
- Shifting mass toward higher buckets suggests stronger repeat spend and order completion.
- Filtering by marketing tier compares spend patterns across Premium vs Standard vs Basic cohorts.

---

### Visualization C — Customer Segmentation

#### Query setup

- **Source file:** `dashboard_queries.sql` — block labeled `VISUALIZATION 3: Customer Segmentation`
- **Gold table:** `gold_customer_segmentation`
- **Saved query name (suggested):** `Dashboard — Customer Segmentation`

#### Visualization configuration

| Setting | Value |
|---------|-------|
| **Chart type** | Pie chart |
| **Slice dimension** | `segment_type` |
| **Slice size / value** | `customer_count` |
| **Optional labels** | Show percentage of total customers per slice |

**Segment definitions** (documented assumptions in `04_customer_segmentation.sql`):

| Segment | Rule (simplified) |
|---------|-------------------|
| **Inactive** | No qualifying orders, or last order older than 365 days |
| **High-Value** | Lifetime qualifying revenue ≥ $1,000 |
| **Repeat** | Two or more qualifying orders (and not Inactive / High-Value) |
| **One-Time** | Exactly one qualifying order (and not Inactive / High-Value) |

Precedence: Inactive → High-Value → Repeat → One-Time. Each customer appears in exactly one segment.

#### Filters

None — segments are mutually exclusive at Gold grain. Use the histogram or optional “Revenue by Tier” chart for `customer_segment` filtering.

#### Suggested title

**Customer Segmentation by Behavior**

#### Expected interpretation

- Shows **what share of the customer base** falls into each behavioral segment.
- Large **Inactive** slice → acquisition or re-engagement opportunity.
- Large **High-Value** slice → healthy core of spenders (threshold is $1,000 lifetime revenue).
- **One-Time** vs **Repeat** balance indicates retention vs single-purchase churn.
- For revenue-weighted view, add a second bar chart using `total_revenue` by `segment_type` from the same query.

---

## 4. Optional Supporting Visualizations

These queries are in `dashboard_queries.sql` but are not required for the assignment minimum.

| Query block | Chart | X-axis | Y-axis | Filters |
|-------------|-------|--------|--------|---------|
| Daily Revenue Trend | Line | `period_start_date` | `total_revenue` | `start_date`, `end_date` |
| Revenue by Source Customer Segment | Bar | `customer_segment` | `total_revenue` | `customer_segment` |
| Top Categories by Revenue | Bar | `category` | `total_revenue` | `product_category` |

---

## 5. Suggested Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  E-Commerce Gold Analytics                                  │
│  [product_category ▼]  [customer_segment ▼]                 │
├──────────────────────────────┬──────────────────────────────┤
│  Top 10 Products by Revenue  │  Customer Segmentation       │
│  (Bar)                       │  (Pie)                       │
├──────────────────────────────┴──────────────────────────────┤
│  Customer Revenue Distribution (Histogram)                    │
└─────────────────────────────────────────────────────────────┘
```

Place global filters at the top so they apply to all linked tiles.

---

## 6. Verifying Dashboard Numbers

Use the checks below in the SQL editor. Results should match what the dashboard displays (within rounding for displayed decimals).

### 6.1 Gold tables are populated

```sql
USE ecommerce;

SELECT COUNT(*) AS product_rows FROM gold_sales_by_product;
SELECT COUNT(*) AS customer_rows FROM gold_revenue_by_customer;
SELECT SUM(customer_count) AS segmented_customers FROM gold_customer_segmentation;
```

`segmented_customers` should equal `customer_rows` (every valid customer is assigned one behavioral segment).

### 6.2 Top 10 Products — spot-check ranking

Run the dashboard query without the UI and compare the top row:

```sql
SELECT product_name, total_revenue
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
```

The highest bar in the chart should match the first row. If `product_category` is filtered, add the same `WHERE` clause as in `dashboard_queries.sql`.

**Cross-check total product revenue:**

```sql
SELECT CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS gold_product_revenue_sum
FROM gold_sales_by_product;
```

This should reconcile with Silver completed valid orders (Gold build logs reconciliation in `create_gold_tables.py`).

### 6.3 Customer Revenue Distribution — bucket totals

```sql
-- Total customers in histogram should match filtered gold_revenue_by_customer count
SELECT COUNT(*) AS customer_count
FROM gold_revenue_by_customer
WHERE 'ALL' = 'ALL' OR customer_segment = 'ALL';  -- replace with your filter value

-- Sum of bucket counts from the dashboard query logic
SELECT SUM(customer_count) AS bucket_total
FROM (
  -- paste VISUALIZATION 2 query here
  SELECT revenue_bucket, COUNT(*) AS customer_count
  FROM (
    SELECT
      customer_id,
      CASE
        WHEN total_revenue = 0 THEN '$0'
        WHEN total_revenue <= 100 THEN '$1 - $100'
        WHEN total_revenue <= 500 THEN '$101 - $500'
        WHEN total_revenue <= 1000 THEN '$501 - $1,000'
        WHEN total_revenue <= 5000 THEN '$1,001 - $5,000'
        ELSE '$5,000+'
      END AS revenue_bucket,
      CASE
        WHEN total_revenue = 0 THEN 1
        WHEN total_revenue <= 100 THEN 2
        WHEN total_revenue <= 500 THEN 3
        WHEN total_revenue <= 1000 THEN 4
        WHEN total_revenue <= 5000 THEN 5
        ELSE 6
      END AS bucket_sort_order
    FROM gold_revenue_by_customer
    WHERE 'ALL' = 'ALL' OR customer_segment = 'ALL'
  ) bucketed
  GROUP BY revenue_bucket, bucket_sort_order
) t;
```

`customer_count` and `bucket_total` must be equal.

### 6.4 Customer Segmentation — pie slice math

```sql
SELECT
  segment_type,
  customer_count,
  ROUND(100.0 * customer_count / SUM(customer_count) OVER (), 1) AS pct_of_customers
FROM gold_customer_segmentation
ORDER BY segment_type;
```

Percentages in the pie chart should match `pct_of_customers` (±0.1% for rounding).

**Segment count integrity:**

```sql
SELECT SUM(customer_count) AS segments_total FROM gold_customer_segmentation;
SELECT COUNT(*) AS customers_total FROM gold_revenue_by_customer;
```

Both totals should match.

**Revenue consistency:**

```sql
SELECT CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS segmentation_revenue
FROM gold_customer_segmentation;

SELECT CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS customer_revenue
FROM gold_revenue_by_customer;
```

Both sums should match — segmentation rolls up the same per-customer revenue as `gold_revenue_by_customer`.

### 6.5 Reconcile to Silver (source of truth)

If dashboard numbers look wrong, validate the Gold layer against Silver:

```sql
SELECT
  CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS silver_completed_valid_revenue,
  COUNT(DISTINCT o.order_id) AS silver_completed_valid_orders
FROM silver_orders o
WHERE o.dq_is_valid = true
  AND o.order_status = 'Completed'
  AND o.customer_id IS NOT NULL
  AND o.total_amount IS NOT NULL
  AND o.product_id IS NOT NULL;
```

Compare `silver_completed_valid_revenue` to:

```sql
SELECT CAST(SUM(total_revenue) AS DECIMAL(18, 2)) FROM gold_revenue_by_customer;
```

They should agree. A mismatch usually means Gold tables were not refreshed after a Silver re-run — re-execute `create_gold_tables.py`.

### 6.6 Parameter sanity checks

| Symptom | Likely cause |
|---------|----------------|
| Empty chart after filtering | Parameter value typo (category name case-sensitive) |
| Histogram shows one bucket only | `customer_segment` set to a tier with few customers |
| Pie chart unchanged when changing filters | Segmentation query has no parameters — expected |
| All revenue is zero | Gold not built, or no Completed orders in source data |
| Top 10 looks like full catalog | `LIMIT 10` removed from query, or chart not using saved query |

---

## 7. Refresh Workflow

When source CSVs or Silver logic change:

1. Re-run `create_silver_tables.py`.
2. Re-run `create_gold_tables.py` (overwrites Gold tables per `gold_write_mode`).
3. Refresh the dashboard — Databricks re-executes linked queries against current Gold data.

No dashboard-side ETL is required.

---

## 8. Quick Reference

| Visual | Gold table | Chart | X-axis | Y-axis / Value | Filter |
|--------|------------|-------|--------|----------------|--------|
| Top 10 Products | `gold_sales_by_product` | Bar | `product_name` | `total_revenue` | `product_category` |
| Revenue Distribution | `gold_revenue_by_customer` | Histogram | `revenue_bucket` | `customer_count` | `customer_segment` |
| Segmentation | `gold_customer_segmentation` | Pie | `segment_type` | `customer_count` | — |

**Query source:** `src/dashboard/dashboard_queries.sql`
