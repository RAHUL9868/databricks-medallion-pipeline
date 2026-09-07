-- =============================================================================
-- Gold: Daily & Weekly Trends  (03_daily_weekly_trends.sql)
-- Target table: gold_daily_weekly_trends
-- Source: silver_orders
-- =============================================================================
--
-- BUSINESS RULES (read before the query)
-- --------------------------------------
--
-- 1. Valid orders (eligible for time-series metrics)
--    A Silver order row must satisfy:
--      - dq_is_valid = true     (passed all Silver quality checks; design D2)
--      - order_date IS NOT NULL (required to place the order on a timeline)
--      - total_amount IS NOT NULL (revenue can be summed safely)
--    Rows failing any Silver check remain in silver_orders for audit only.
--
-- 2. Revenue-producing orders (included in order_count AND total_revenue)
--    Among valid orders, only fulfilled sales count toward trends:
--      INCLUDED  : order_status = 'Completed'  (design decision D1)
--      EXCLUDED  : order_status = 'Pending'      (not fulfilled; no revenue)
--      EXCLUDED  : order_status = 'Cancelled'    (not a completed sale)
--    Daily/weekly order_count and total_revenue use the same Completed filter
--    so volume and revenue trends are directly comparable.
--
-- 3. Duplicate order_id handling
--    Duplicate order_id rows are invalid in Silver and excluded by dq_is_valid.
--    Defensively, qualifying orders are deduplicated to one row per order_id
--    (earliest _source_row_num wins) before aggregation.
--
-- 4. Date handling
--    - order_date is a DATE column on silver_orders (cast during Silver build).
--    - Daily grain: period_start_date = order_date (calendar day).
--    - Weekly grain: period_start_date = start of ISO week (Monday) via
--      date_trunc('week', order_date), cast to DATE for readability.
--    - An order contributes to exactly one day and one week (its order_date).
--
-- 5. Sparse periods
--    Only days/weeks with at least one qualifying order appear in the output.
--    Calendar days or weeks with zero Completed valid orders are omitted
--    (downstream dashboards may outer-join to a date dimension if zero-fill is needed).
--
-- Output schema (long / unioned grain):
--   period_grain       : 'DAY' or 'WEEK'
--   period_start_date  : First calendar day of the period (DATE)
--   order_count        : COUNT(DISTINCT order_id) in the period
--   total_revenue      : SUM(total_amount), DECIMAL(18,2)
--
-- =============================================================================

CREATE OR REPLACE TABLE gold_daily_weekly_trends AS
WITH qualifying_orders AS (
    -- Valid, completed, revenue-eligible orders with a usable order_date.
    SELECT
        order_id,
        order_date,
        CAST(total_amount AS DECIMAL(18, 2)) AS total_amount,
        _source_row_num
    FROM silver_orders
    WHERE dq_is_valid = true
      AND order_status = 'Completed'
      AND order_date IS NOT NULL
      AND total_amount IS NOT NULL
),

deduplicated_orders AS (
    -- One economic event per order_id (prevents duplicate-key double counting).
    SELECT
        order_id,
        order_date,
        total_amount
    FROM (
        SELECT
            order_id,
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

daily_trends AS (
    SELECT
        'DAY' AS period_grain,
        order_date AS period_start_date,
        COUNT(DISTINCT order_id) AS order_count,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY order_date
),

weekly_trends AS (
    SELECT
        'WEEK' AS period_grain,
        CAST(DATE_TRUNC('week', order_date) AS DATE) AS period_start_date,
        COUNT(DISTINCT order_id) AS order_count,
        CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
    FROM deduplicated_orders
    GROUP BY DATE_TRUNC('week', order_date)
)

SELECT
    period_grain,
    period_start_date,
    order_count,
    total_revenue
FROM daily_trends

UNION ALL

SELECT
    period_grain,
    period_start_date,
    order_count,
    total_revenue
FROM weekly_trends

ORDER BY
    period_grain,
    period_start_date
;
