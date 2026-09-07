-- =============================================================================
-- E-Commerce Pipeline — Databricks SQL Dashboard Queries
-- File: src/dashboard/dashboard_queries.sql
-- Schema: ecommerce (adjust USE statement if PIPELINE_SCHEMA differs)
-- =============================================================================
--
-- DASHBOARD PARAMETERS (create in Databricks SQL Dashboard UI)
-- ------------------------------------------------------------
-- | Parameter          | Type   | Default | Used by                          |
-- |--------------------|--------|---------|----------------------------------|
-- | product_category   | STRING | ALL     | Top 10 products                  |
-- | customer_segment   | STRING | ALL     | Customer revenue distribution    |
-- | start_date         | DATE   | (min)   | Daily revenue trend              |
-- | end_date           | DATE   | (max)   | Daily revenue trend              |
--
-- Order status filter:
--   Gold revenue tables include Completed orders only (design D1).
--   Pending/Cancelled are excluded at the Gold layer, so order_status is
--   not a filter on these queries. Use Silver for operational status views.
--
-- =============================================================================

USE ecommerce;


-- =============================================================================
-- VISUALIZATION 1: Top 10 Products by Revenue
-- =============================================================================
-- Purpose     : Rank best-selling products by realized revenue for merchandising
--                 and inventory decisions.
-- Gold table    : gold_sales_by_product
-- Dimensions    : product_name, category, product_id (tooltip / drill)
-- Measures      : total_revenue (rank), total_orders, avg_order_value
-- Chart type    : Horizontal bar chart (product_name on Y, total_revenue on X)
-- Filters       : product_category — set to ALL or a specific category value
-- =============================================================================

SELECT
    product_id,
    product_name,
    category,
    total_orders,
    total_revenue,
    avg_order_value
FROM gold_sales_by_product
WHERE :product_category = 'ALL'
   OR category = :product_category
ORDER BY total_revenue DESC
LIMIT 10
;


-- =============================================================================
-- VISUALIZATION 2: Customer Revenue Distribution (Histogram)
-- =============================================================================
-- Purpose     : Show how customer lifetime revenue (from Completed valid orders)
--                 is distributed across fixed spend buckets.
-- Gold table    : gold_revenue_by_customer
-- Dimensions    : revenue_bucket (histogram bin label)
-- Measures      : customer_count (customers in each bucket)
-- Chart type    : Bar chart / histogram (revenue_bucket on X, customer_count on Y)
-- Filters       : customer_segment — source marketing tier (Premium/Standard/Basic)
--
-- Bucket boundaries (design decision D5 — fixed, documented):
--   $0          : zero revenue customers
--   $1–$100     : low spend
--   $101–$500   : moderate spend
--   $501–$1,000 : high spend
--   $1,001–$5,000: very high spend
--   $5,000+     : top spenders
-- =============================================================================

SELECT
    revenue_bucket,
    COUNT(*) AS customer_count
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
    WHERE :customer_segment = 'ALL'
       OR customer_segment = :customer_segment
) bucketed
GROUP BY revenue_bucket, bucket_sort_order
ORDER BY bucket_sort_order
;


-- =============================================================================
-- VISUALIZATION 3: Customer Segmentation (Behavioral)
-- =============================================================================
-- Purpose     : Show share of customers and revenue across behavioral segments
--                 (High-Value, Repeat, One-Time, Inactive). Threshold rules are
--                 documented in 04_customer_segmentation.sql.
-- Gold table    : gold_customer_segmentation
-- Dimensions    : segment_type
-- Measures      : customer_count, total_revenue, avg_revenue
-- Chart type    : Pie or donut chart (segment_type as slice, customer_count as size)
--                 Optional second chart: bar chart of total_revenue by segment_type
-- Filters       : none at Gold grain (segments are mutually exclusive)
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
-- Purpose     : Track revenue and order volume over time for executive monitoring.
-- Gold table    : gold_daily_weekly_trends
-- Dimensions    : period_start_date
-- Measures      : total_revenue, order_count
-- Chart type    : Line chart (period_start_date on X, total_revenue on Y);
--                 optional second axis for order_count
-- Filters       : start_date, end_date (inclusive) on period_start_date
-- Note          : Gold trends include Completed valid orders only (no status filter).
-- =============================================================================

SELECT
    period_start_date,
    order_count,
    total_revenue
FROM gold_daily_weekly_trends
WHERE period_grain = 'DAY'
  AND period_start_date >= :start_date
  AND period_start_date <= :end_date
ORDER BY period_start_date
;


-- =============================================================================
-- SUPPORTING VISUALIZATION: Revenue by Source Customer Segment
-- =============================================================================
-- Purpose     : Compare total revenue across marketing tiers (Premium/Standard/Basic)
--                 from the customer master — distinct from behavioral segmentation.
-- Gold table    : gold_revenue_by_customer
-- Dimensions    : customer_segment
-- Measures      : total_revenue (SUM), customer_count (COUNT), avg_revenue (AVG)
-- Chart type    : Bar chart (customer_segment on X, total_revenue on Y)
-- Filters       : customer_segment — set ALL or one tier
-- =============================================================================

SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue,
    CAST(AVG(total_revenue) AS DECIMAL(18, 2)) AS avg_revenue
FROM gold_revenue_by_customer
WHERE :customer_segment = 'ALL'
   OR customer_segment = :customer_segment
GROUP BY customer_segment
ORDER BY total_revenue DESC
;


-- =============================================================================
-- SUPPORTING VISUALIZATION: Top Categories by Revenue
-- =============================================================================
-- Purpose     : Summarize product revenue at category level for assortment planning.
-- Gold table    : gold_sales_by_product
-- Dimensions    : category
-- Measures      : total_revenue, total_orders, product_count
-- Chart type    : Bar chart (category on X, total_revenue on Y)
-- Filters       : product_category — when not ALL, returns single category row
-- =============================================================================

SELECT
    category,
    COUNT(*) AS product_count,
    SUM(total_orders) AS total_orders,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue
FROM gold_sales_by_product
WHERE :product_category = 'ALL'
   OR category = :product_category
GROUP BY category
ORDER BY total_revenue DESC
;
