-- =============================================================================
-- Gold: Revenue by Customer  (02_revenue_by_customer.sql)
-- Target table: gold_revenue_by_customer
-- Sources: silver_customers, silver_orders
-- =============================================================================
--
-- BUSINESS RULES (read before the query)
-- --------------------------------------
--
-- 1. What qualifies as an order?
--    A Silver order row that simultaneously satisfies:
--      - dq_is_valid = true          (passed all Silver quality checks)
--      - order_status = 'Completed'  (fulfilled sale; see rule 2)
--      - customer_id IS NOT NULL     (attributable to a customer)
--      - total_amount IS NOT NULL    (revenue can be summed safely)
--    Pending and Cancelled rows are never counted as orders in this Gold table.
--
-- 2. Which statuses contribute revenue?
--      INCLUDED  : 'Completed'  (design decision D1 — revenue recognition)
--      EXCLUDED  : 'Pending'    (not fulfilled)
--      EXCLUDED  : 'Cancelled'  (not a completed sale)
--
-- 3. How are duplicate orders handled?
--    Assignment duplicate order_id rows are flagged invalid in Silver (dq_is_valid
--    = false) and are excluded by the quality gate. Defensively, qualifying orders
--    are deduplicated to one row per order_id (earliest _source_row_num wins)
--    before aggregation so total_orders and total_revenue cannot double-count.
--
-- 4. How are invalid orders handled?
--    Any silver_orders row with dq_is_valid = false is excluded entirely.
--    Invalid rows remain in Silver for audit and DQ metrics but do not appear in
--    Gold numerators (design decision D2).
--
-- 5. How are customers with no valid orders treated?
--    The customer dimension uses valid Silver customers (dq_is_valid = true).
--    A LEFT JOIN to order metrics keeps every valid customer in the output:
--      total_orders = 0, total_revenue = 0, lifetime_value_actual = 0,
--      avg_order_value = NULL.
--
-- 6. No duplicate customer rows
--    Grain is one row per customer_id. The valid_customers CTE deduplicates
--    silver_customers to a single row per customer_id (earliest _source_row_num)
--    before joining, so the Gold table cannot emit duplicate customer keys.
--
-- Metric definitions (DECIMAL(18,2) for money):
--   customer_id           : Primary key from deduplicated valid silver_customers
--   customer_name         : Dimension attribute from silver_customers
--   customer_segment      : Dimension attribute from silver_customers
--   total_orders          : COUNT(DISTINCT order_id) of qualifying deduplicated orders
--   total_revenue         : SUM(total_amount) of those orders
--   avg_order_value       : total_revenue / total_orders when total_orders > 0; else NULL
--   lifetime_value_actual : SUM(total_amount) of qualifying orders (= total_revenue at
--                           customer grain; represents realized LTV from completed sales)
--
-- =============================================================================

CREATE OR REPLACE TABLE gold_revenue_by_customer AS
WITH valid_customers AS (
    -- One dimension row per customer_id (invalid and duplicate-key rows excluded).
    SELECT
        customer_id,
        customer_name,
        customer_segment
    FROM (
        SELECT
            customer_id,
            customer_name,
            customer_segment,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM silver_customers
        WHERE dq_is_valid = true
          AND customer_id IS NOT NULL
    ) ranked
    WHERE row_rank = 1
),

qualifying_orders AS (
    -- Completed, valid, customer-attributable order lines only.
    SELECT
        order_id,
        customer_id,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND customer_id IS NOT NULL
      AND total_amount IS NOT NULL
),

deduplicated_orders AS (
    -- One economic event per order_id (prevents duplicate-key double counting).
    SELECT
        order_id,
        customer_id,
        total_amount
    FROM (
        SELECT
            order_id,
            customer_id,
            total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM qualifying_orders
    ) ranked
    WHERE row_rank = 1
),

customer_order_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    COALESCE(m.total_orders, CAST(0 AS BIGINT)) AS total_orders,
    COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))) AS total_revenue,
    CASE
        WHEN COALESCE(m.total_orders, CAST(0 AS BIGINT)) = 0 THEN CAST(NULL AS DECIMAL(18, 2))
        ELSE CAST(m.total_revenue / m.total_orders AS DECIMAL(18, 2))
    END AS avg_order_value,
    COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))) AS lifetime_value_actual
FROM valid_customers AS c
LEFT JOIN customer_order_metrics AS m
    ON c.customer_id = m.customer_id
;
