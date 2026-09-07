-- =============================================================================
-- Gold: Sales by Product  (01_sales_by_product.sql)
-- Target table: gold_sales_by_product
-- Sources: silver_products, silver_orders
-- =============================================================================
--
-- AGGREGATION LOGIC (read before the query)
-- -----------------------------------------
-- Grain: one row per valid Silver product (catalog dimension).
--
-- Which orders count as business transactions?
--   INCLUDED  : order_status = 'Completed'
--   EXCLUDED  : order_status = 'Pending'   (not fulfilled; no revenue recognition)
--   EXCLUDED  : order_status = 'Cancelled' (reversed / non-sale)
--
-- Silver quality gate (design decision D2):
--   Only rows with dq_is_valid = true on silver_orders are eligible.
--   Invalid orders (NULL FKs, duplicates, type/business failures, etc.) are
--   retained in Silver for audit but must NOT inflate Gold KPIs.
--
-- Duplicate order_id protection (design decision D4):
--   Assignment duplicate order_id rows are flagged invalid in Silver and therefore
--   excluded by dq_is_valid. As a defensive measure, qualifying orders are
--   deduplicated to one row per order_id (earliest _source_row_num wins) before
--   aggregation so revenue cannot be double-counted if a duplicate ever slipped through.
--
-- NULL handling:
--   - Orders with NULL product_id cannot be attributed → excluded from aggregates.
--   - Orders with NULL total_amount → excluded (cannot sum revenue safely).
--   - Products with no qualifying orders → LEFT JOIN yields zeros / NULL AOV.
--
-- Metric definitions:
--   total_orders    : COUNT(DISTINCT order_id) of qualifying, deduplicated orders per product
--   total_revenue   : SUM(total_amount) of those orders, cast to DECIMAL(18,2)
--   avg_order_value : total_revenue / total_orders when total_orders > 0; otherwise NULL
--
-- =============================================================================

CREATE OR REPLACE TABLE gold_sales_by_product AS
WITH valid_products AS (
    -- Business-ready product catalog dimension (invalid catalog rows excluded per D2).
    SELECT
        product_id,
        product_name,
        category
    FROM silver_products
    WHERE dq_is_valid = true
      AND product_id IS NOT NULL
),

qualifying_orders AS (
    -- Completed, valid, attributable order lines only.
    SELECT
        order_id,
        product_id,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND product_id IS NOT NULL
      AND total_amount IS NOT NULL
),

deduplicated_orders AS (
    -- One economic event per order_id (prevents duplicate-key double counting).
    SELECT
        order_id,
        product_id,
        total_amount
    FROM (
        SELECT
            order_id,
            product_id,
            total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM qualifying_orders
    ) ranked
    WHERE row_rank = 1
),

product_order_metrics AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) AS total_orders,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(m.total_orders, CAST(0 AS BIGINT)) AS total_orders,
    COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))) AS total_revenue,
    CASE
        WHEN COALESCE(m.total_orders, CAST(0 AS BIGINT)) = 0 THEN CAST(NULL AS DECIMAL(18, 2))
        ELSE CAST(m.total_revenue / m.total_orders AS DECIMAL(18, 2))
    END AS avg_order_value
FROM valid_products AS p
LEFT JOIN product_order_metrics AS m
    ON p.product_id = m.product_id
;
