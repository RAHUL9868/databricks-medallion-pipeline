-- =============================================================================
-- Gold: Customer Segmentation  (04_customer_segmentation.sql)
-- Target table: gold_customer_segmentation
-- Sources: silver_customers, silver_orders
-- =============================================================================
--
-- =============================================================================
-- PROPOSED SEGMENTATION RULES (assignment labels without official thresholds)
-- =============================================================================
--
-- The assignment requires segment_type labels:
--   High-Value | Repeat | One-Time | Inactive
-- but does not define numeric thresholds. The rules below are **documented
-- engineering assumptions** (not assignment facts). Change constants in
-- `segmentation_params` if stakeholders adopt different cutoffs.
--
-- Qualifying order definition (consistent with Gold 01/02/03, design D1 + D2):
--   - silver_orders.dq_is_valid = true
--   - order_status = 'Completed'
--   - customer_id IS NOT NULL
--   - total_amount IS NOT NULL
--   - Deduplicated to one row per order_id (_source_row_num wins)
--
-- Customer universe:
--   - All silver_customers rows with dq_is_valid = true and non-null customer_id
--   - Deduplicated to one row per customer_id (no duplicate customer keys in output)
--
-- Per-customer metrics (from qualifying orders only):
--   - total_orders      : COUNT(DISTINCT order_id)
--   - total_revenue     : SUM(total_amount), DECIMAL(18,2)
--   - last_order_date   : MAX(order_date) of qualifying orders
--
-- ASSUMPTION A1 — High-Value revenue threshold
--   total_revenue >= 1,000.00 (USD) lifetime qualifying revenue.
--   Rationale: separates materially higher spenders from typical single/repeat buyers
--   on generated catalog price points without using source-system lifetime_value.
--
-- ASSUMPTION A2 — Repeat buyer
--   total_orders >= 2 among non-Inactive customers.
--
-- ASSUMPTION A3 — One-Time buyer
--   total_orders = 1 among non-Inactive, non-High-Value customers.
--
-- ASSUMPTION A4 — Inactive (revenue + activity)
--   - total_orders = 0 (no qualifying completed orders), OR
--   - last_order_date < CURRENT_DATE - 365 days (no qualifying activity in trailing
--     12 months; uses pipeline run date via CURRENT_DATE()).
--   Customers with no valid orders are always Inactive.
--
-- PRECEDENCE (first match wins — guarantees exactly one segment per customer):
--   1. Inactive   (no orders OR stale activity per A4)
--   2. High-Value (total_revenue >= A1 threshold)
--   3. Repeat     (total_orders >= 2)
--   4. One-Time   (total_orders = 1)
--   5. Inactive   (ELSE safety fallback; should not trigger)
--
-- Output metrics (aggregated by segment_type):
--   customer_count : COUNT(customers) in the segment
--   total_revenue  : SUM(customer total_revenue) in the segment, DECIMAL(18,2)
--   avg_revenue    : total_revenue / customer_count; NULL only if customer_count = 0
--
-- Validation guarantees (by construction):
--   - Segments do not overlap (mutually exclusive CASE precedence).
--   - Every valid customer appears in exactly one segment assignment.
--   - SUM(customer_count) over segments = COUNT(valid deduplicated customers).
--     (See reconciliation queries in comments at file end.)
--
-- Pending/Cancelled/invalid orders:
--   Excluded from metrics; they do not affect segment assignment except by their
--   absence (e.g., only Pending orders → total_orders = 0 → Inactive).
--
-- =============================================================================

CREATE OR REPLACE TABLE gold_customer_segmentation AS
WITH segmentation_params AS (
    SELECT
        CAST(1000.00 AS DECIMAL(18, 2)) AS high_value_revenue_threshold,
        CAST(365 AS INT) AS inactive_activity_days
),

valid_customers AS (
    SELECT
        customer_id
    FROM (
        SELECT
            customer_id,
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
    SELECT
        order_id,
        customer_id,
        order_date,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND customer_id IS NOT NULL
      AND total_amount IS NOT NULL
      AND order_date IS NOT NULL
),

deduplicated_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM (
        SELECT
            order_id,
            customer_id,
            order_date,
            total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _source_row_num
            ) AS row_rank
        FROM qualifying_orders
    ) ranked
    WHERE row_rank = 1
),

customer_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        CAST(COALESCE(SUM(total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
        MAX(order_date) AS last_order_date
    FROM deduplicated_orders
    GROUP BY customer_id
),

customer_segment_assignments AS (
    SELECT
        c.customer_id,
        COALESCE(m.total_orders, CAST(0 AS BIGINT)) AS total_orders,
        COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))) AS total_revenue,
        m.last_order_date,
        CASE
            WHEN COALESCE(m.total_orders, CAST(0 AS BIGINT)) = 0
                OR m.last_order_date IS NULL
                OR m.last_order_date < DATE_SUB(CURRENT_DATE(), p.inactive_activity_days)
                THEN 'Inactive'
            WHEN COALESCE(m.total_revenue, CAST(0 AS DECIMAL(18, 2))) >= p.high_value_revenue_threshold
                THEN 'High-Value'
            WHEN COALESCE(m.total_orders, CAST(0 AS BIGINT)) >= 2
                THEN 'Repeat'
            WHEN COALESCE(m.total_orders, CAST(0 AS BIGINT)) = 1
                THEN 'One-Time'
            ELSE 'Inactive'
        END AS segment_type
    FROM valid_customers AS c
    CROSS JOIN segmentation_params AS p
    LEFT JOIN customer_metrics AS m
        ON c.customer_id = m.customer_id
),

segment_aggregates AS (
    SELECT
        segment_type,
        COUNT(*) AS customer_count,
        CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue
    FROM customer_segment_assignments
    GROUP BY segment_type
),

segment_spine AS (
    SELECT 'High-Value' AS segment_type
    UNION ALL SELECT 'Repeat'
    UNION ALL SELECT 'One-Time'
    UNION ALL SELECT 'Inactive'
)

SELECT
    s.segment_type,
    COALESCE(a.customer_count, CAST(0 AS BIGINT)) AS customer_count,
    CASE
        WHEN COALESCE(a.customer_count, CAST(0 AS BIGINT)) = 0
            THEN CAST(NULL AS DECIMAL(18, 2))
        ELSE CAST(a.total_revenue / a.customer_count AS DECIMAL(18, 2))
    END AS avg_revenue,
    COALESCE(a.total_revenue, CAST(0 AS DECIMAL(18, 2))) AS total_revenue
FROM segment_spine AS s
LEFT JOIN segment_aggregates AS a
    ON s.segment_type = a.segment_type
ORDER BY
    CASE s.segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat' THEN 2
        WHEN 'One-Time' THEN 3
        WHEN 'Inactive' THEN 4
    END
;

-- =============================================================================
-- RECONCILIATION QUERIES (manual validation after build)
-- =============================================================================
--
-- 1) Customer counts reconcile (segment sums = valid customer dimension):
--    SELECT SUM(customer_count) AS segmented_customers FROM gold_customer_segmentation;
--    SELECT COUNT(*) AS valid_customers
--    FROM (
--      SELECT customer_id,
--             ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _source_row_num) AS rn
--      FROM silver_customers
--      WHERE dq_is_valid = true AND customer_id IS NOT NULL
--    ) WHERE rn = 1;
--
-- 2) No overlapping segments (each customer_id maps to exactly one segment_type):
--    -- Rebuild assignments in a scratch query from customer_segment_assignments logic;
--    -- expect COUNT(*) = COUNT(DISTINCT customer_id).
--
-- 3) All four labels present:
--    SELECT COUNT(DISTINCT segment_type) FROM gold_customer_segmentation;  -- expect 4
--
-- =============================================================================
