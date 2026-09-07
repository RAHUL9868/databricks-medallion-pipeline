-- =============================================================================
-- E-Commerce Pipeline — Databricks SQL Dashboard Queries
-- File: src/dashboard/dashboard_queries.sql
-- Schema: ecommerce (adjust USE statement if PIPELINE_SCHEMA differs)
-- =============================================================================
--
-- FILTERING (no :parameter binding required)
-- ----------------------------------------
-- Each query uses a `dashboard_filters` CTE with literal defaults so the SQL runs
-- in the SQL editor and in dashboards without UNBOUND_SQL_PARAMETER errors.
--
-- To narrow results: edit the literals in `dashboard_filters` (e.g. product_category
-- = 'Electronics' instead of 'ALL').
--
-- For interactive Lakeview dashboards, prefer **field filters** on category,
-- customer_segment, and period_start_date instead of SQL parameters.
--
-- Optional SQL parameters (:name syntax) require defaults in Query → Parameters.
-- See DASHBOARD_GUIDE.md for dashboard filter-widget setup.
--
-- Order status: Gold tables include Completed orders only (design D1).
--
-- =============================================================================

USE ecommerce;


-- =============================================================================
-- VISUALIZATION 1: Top 10 Products by Revenue
-- =============================================================================
-- Purpose     : Rank best-selling products by realized revenue
-- Gold table    : gold_sales_by_product
-- Chart type    : Horizontal bar (product_name vs total_revenue)
-- Filter        : dashboard_filters.product_category — ALL or a category name
-- =============================================================================

WITH dashboard_filters AS (
    SELECT 'ALL' AS product_category  -- e.g. 'Electronics', 'Clothing', or 'ALL'
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.total_orders,
    p.total_revenue,
    p.avg_order_value
FROM gold_sales_by_product AS p
CROSS JOIN dashboard_filters AS f
WHERE f.product_category = 'ALL'
   OR p.category = f.product_category
ORDER BY p.total_revenue DESC
LIMIT 10
;


-- =============================================================================
-- VISUALIZATION 2: Customer Revenue Distribution (Histogram)
-- =============================================================================
-- Purpose     : Distribution of customer lifetime revenue in fixed buckets
-- Gold table    : gold_revenue_by_customer
-- Chart type    : Bar / histogram (revenue_bucket vs customer_count)
-- Filter        : dashboard_filters.customer_segment — ALL or Premium/Standard/Basic
-- =============================================================================

WITH dashboard_filters AS (
    SELECT 'ALL' AS customer_segment  -- e.g. 'Premium', 'Standard', 'Basic', or 'ALL'
)
SELECT
    revenue_bucket,
    COUNT(*) AS customer_count
FROM (
    SELECT
        c.customer_id,
        CASE
            WHEN c.total_revenue = 0 THEN '$0'
            WHEN c.total_revenue <= 100 THEN '$1 - $100'
            WHEN c.total_revenue <= 500 THEN '$101 - $500'
            WHEN c.total_revenue <= 1000 THEN '$501 - $1,000'
            WHEN c.total_revenue <= 5000 THEN '$1,001 - $5,000'
            ELSE '$5,000+'
        END AS revenue_bucket,
        CASE
            WHEN c.total_revenue = 0 THEN 1
            WHEN c.total_revenue <= 100 THEN 2
            WHEN c.total_revenue <= 500 THEN 3
            WHEN c.total_revenue <= 1000 THEN 4
            WHEN c.total_revenue <= 5000 THEN 5
            ELSE 6
        END AS bucket_sort_order
    FROM gold_revenue_by_customer AS c
    CROSS JOIN dashboard_filters AS f
    WHERE f.customer_segment = 'ALL'
       OR c.customer_segment = f.customer_segment
) AS bucketed
GROUP BY revenue_bucket, bucket_sort_order
ORDER BY bucket_sort_order
;


-- =============================================================================
-- VISUALIZATION 3: Customer Segmentation (Behavioral)
-- =============================================================================
-- Purpose     : Share of customers and revenue by behavioral segment
-- Gold table    : gold_customer_segmentation
-- Chart type    : Pie / donut (segment_type vs customer_count)
-- Filters       : none
-- =============================================================================

SELECT
    segment_type,
    customer_count,
    total_revenue,
    avg_revenue
FROM gold_customer_segmentation
ORDER BY
    CASE segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat' THEN 2
        WHEN 'One-Time' THEN 3
        WHEN 'Inactive' THEN 4
        ELSE 5
    END
;


-- =============================================================================
-- SUPPORTING VISUALIZATION: Daily Revenue Trend
-- =============================================================================
-- Purpose     : Revenue and order volume over time
-- Gold table    : gold_daily_weekly_trends
-- Chart type    : Line (period_start_date vs total_revenue)
-- Filter        : start_date / end_date — defaults to full DAY range in Gold table
-- =============================================================================

WITH trend_bounds AS (
    SELECT
        MIN(period_start_date) AS start_date,
        MAX(period_start_date) AS end_date
    FROM gold_daily_weekly_trends
    WHERE period_grain = 'DAY'
),
dashboard_filters AS (
    SELECT start_date, end_date FROM trend_bounds
)
SELECT
    t.period_start_date,
    t.order_count,
    t.total_revenue
FROM gold_daily_weekly_trends AS t
CROSS JOIN dashboard_filters AS f
WHERE t.period_grain = 'DAY'
  AND t.period_start_date >= f.start_date
  AND t.period_start_date <= f.end_date
ORDER BY t.period_start_date
;


-- =============================================================================
-- SUPPORTING VISUALIZATION: Revenue by Source Customer Segment
-- =============================================================================
-- Purpose     : Revenue by marketing tier (Premium / Standard / Basic)
-- Gold table    : gold_revenue_by_customer
-- Chart type    : Bar (customer_segment vs total_revenue)
-- Filter        : dashboard_filters.customer_segment
-- =============================================================================

WITH dashboard_filters AS (
    SELECT 'ALL' AS customer_segment
)
SELECT
    c.customer_segment,
    COUNT(*) AS customer_count,
    CAST(SUM(c.total_revenue) AS DECIMAL(18, 2)) AS total_revenue,
    CAST(AVG(c.total_revenue) AS DECIMAL(18, 2)) AS avg_revenue
FROM gold_revenue_by_customer AS c
CROSS JOIN dashboard_filters AS f
WHERE f.customer_segment = 'ALL'
   OR c.customer_segment = f.customer_segment
GROUP BY c.customer_segment
ORDER BY total_revenue DESC
;


-- =============================================================================
-- SUPPORTING VISUALIZATION: Top Categories by Revenue
-- =============================================================================
-- Purpose     : Category-level revenue rollup
-- Gold table    : gold_sales_by_product
-- Chart type    : Bar (category vs total_revenue)
-- Filter        : dashboard_filters.product_category — ALL or one category
-- =============================================================================

WITH dashboard_filters AS (
    SELECT 'ALL' AS product_category
)
SELECT
    p.category,
    COUNT(*) AS product_count,
    SUM(p.total_orders) AS total_orders,
    CAST(SUM(p.total_revenue) AS DECIMAL(18, 2)) AS total_revenue
FROM gold_sales_by_product AS p
CROSS JOIN dashboard_filters AS f
WHERE f.product_category = 'ALL'
   OR p.category = f.product_category
GROUP BY p.category
ORDER BY total_revenue DESC
;
