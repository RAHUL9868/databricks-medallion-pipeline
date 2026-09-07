-- =============================================================================
-- E-Commerce Medallion Pipeline — Reference Schema (Databricks / Delta Lake)
-- File: database/schema.sql
-- =============================================================================
--
-- PURPOSE
-- -------
-- Documents the logical schema for Bronze, Silver, and Gold objects used by this
-- project. All pipeline tables are Delta Lake tables unless noted as a view.
--
-- HOW TO USE THIS FILE
-- --------------------
-- 1. Primary table creation is performed by the Python pipeline (see
--    database/setup-notes.md). Do NOT rely on this file alone to populate data.
-- 2. Run the "Namespace bootstrap" section once per environment to create the
--    target schema/database.
-- 3. After Silver tables exist, you may run the "Silver valid-record views"
--    section (also created automatically by create_silver_tables.py).
-- 4. Gold tables are built by src/gold/*.sql via create_gold_tables.py — the
--    Gold DDL below is reference only.
--
-- NAMESPACE PARAMETERS (match src/config/pipeline_config.py)
-- ----------------------------------------------------------
--   PIPELINE_SCHEMA   default: ecommerce
--   PIPELINE_CATALOG  optional Unity Catalog name (unset = hive metastore)
--
-- Qualified table name resolution:
--   Without catalog : {schema}.{table}           e.g. ecommerce.bronze_customers
--   With catalog    : {catalog}.{schema}.{table} e.g. main.ecommerce.bronze_customers
--
-- Replace {schema} below with your PIPELINE_SCHEMA value before running DDL.
-- When using Unity Catalog, prefix with your catalog and use CREATE SCHEMA.
--
-- =============================================================================


-- =============================================================================
-- NAMESPACE BOOTSTRAP
-- =============================================================================

-- Unity Catalog (when PIPELINE_CATALOG is set):
-- CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

-- Hive metastore / legacy (Databricks Community Edition default):
CREATE SCHEMA IF NOT EXISTS ecommerce
COMMENT 'E-commerce medallion pipeline — Bronze, Silver, Gold Delta tables';


-- =============================================================================
-- BRONZE LAYER
-- Created by: src/bronze/bronze_ingest.py (ingest_all.py)
-- Format    : Delta Lake
-- Grain     : One row per CSV source row (+ ingest metadata)
-- Rule      : All business columns stored as STRING; no casting at Bronze
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze_customers
-- Source CSV: customers.csv
-- -----------------------------------------------------------------------------
-- Business columns (STRING, nullable):
--   customer_id, customer_name, email, country, signup_date,
--   customer_segment, lifetime_value
-- Metadata columns:
--   _ingest_ts        TIMESTAMP NOT NULL
--   _source_file      STRING    NOT NULL
--   _source_path      STRING    NOT NULL
--   _batch_id         STRING    NOT NULL
--   _source_row_num   BIGINT    NOT NULL  -- 1-based CSV row number
--   _corrupt_record   STRING    NULL      -- PERMISSIVE CSV parse failures

-- -----------------------------------------------------------------------------
-- bronze_orders
-- Source CSV: orders.csv
-- -----------------------------------------------------------------------------
-- Business columns (STRING, nullable):
--   order_id, customer_id, order_date, product_id, quantity, unit_price,
--   total_amount, order_status, payment_date
-- Metadata: same six columns as bronze_customers

-- -----------------------------------------------------------------------------
-- bronze_products
-- Source CSV: products.csv
-- -----------------------------------------------------------------------------
-- Business columns (STRING, nullable):
--   product_id, product_name, category, price, cost,
--   stock_quantity, reorder_level
-- Metadata: same six columns as bronze_customers

-- -----------------------------------------------------------------------------
-- bronze_ingest_audit
-- Append-only ingest run log (written by bronze_ingest.py)
-- -----------------------------------------------------------------------------
--   batch_id              STRING    NOT NULL
--   entity                STRING    NOT NULL  -- customers | orders | products
--   source_file           STRING    NOT NULL
--   source_path           STRING    NOT NULL
--   target_table          STRING    NOT NULL  -- fully qualified target
--   write_mode            STRING    NOT NULL  -- overwrite | append
--   status                STRING    NOT NULL  -- SUCCESS | FAILED
--   row_count             BIGINT    NULL
--   source_column_count   BIGINT    NULL
--   bronze_column_count   BIGINT    NULL
--   corrupt_record_count  BIGINT    NULL
--   ingest_started_at     TIMESTAMP NOT NULL
--   ingest_completed_at   TIMESTAMP NULL
--   error_message         STRING    NULL      -- truncated to 2000 chars


-- =============================================================================
-- SILVER LAYER
-- Created by: src/silver/create_silver_tables.py
-- Format    : Delta Lake
-- Grain     : Same row count as Bronze (invalid rows retained, not quarantined)
-- Rule      : Typed business columns + _source_* traceability + DQ flags
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver_customers
-- -----------------------------------------------------------------------------
-- Typed business columns:
--   customer_id        INT
--   customer_name      STRING
--   email              STRING
--   country            STRING
--   signup_date        DATE
--   customer_segment   STRING       -- Premium | Standard | Basic
--   lifetime_value     DECIMAL(18,2)
--
-- Traceability (_source_* columns, STRING — raw Bronze values):
--   _source_customer_id, _source_customer_name, _source_email, _source_country,
--   _source_signup_date, _source_customer_segment, _source_lifetime_value
--
-- Bronze lineage metadata:
--   _ingest_ts, _source_file, _source_path, _batch_id, _source_row_num,
--   _corrupt_record
--
-- Data quality columns (all entity Silver tables):
--   dq_is_valid              BOOLEAN
--   dq_record_status         STRING       -- valid | invalid | invalid_multiple
--   dq_failed_rules          ARRAY<STRING>
--   dq_failure_count         INT
--   dq_completeness_pass     BOOLEAN
--   dq_uniqueness_pass       BOOLEAN
--   dq_referential_pass      BOOLEAN      -- always true for customers/products
--   dq_type_business_pass    BOOLEAN
--   dq_business_logic_pass   BOOLEAN
--   dq_completeness_errors   ARRAY<STRING>
--   dq_uniqueness_errors     ARRAY<STRING>
--   dq_referential_errors    ARRAY<STRING>
--   dq_type_business_errors  ARRAY<STRING>
--   dq_business_logic_errors ARRAY<STRING>
--   dq_error_messages        ARRAY<STRING>
--   dq_checked_at            TIMESTAMP
--   dq_run_id                STRING

-- -----------------------------------------------------------------------------
-- silver_orders
-- -----------------------------------------------------------------------------
-- Typed business columns:
--   order_id        INT
--   customer_id     INT
--   order_date      DATE
--   product_id      INT
--   quantity        INT
--   unit_price      DECIMAL(18,2)
--   total_amount    DECIMAL(18,2)
--   order_status    STRING
--   payment_date    DATE
--
-- Traceability: _source_order_id, _source_customer_id, _source_order_date,
--   _source_product_id, _source_quantity, _source_unit_price, _source_total_amount,
--   _source_order_status, _source_payment_date
-- + Bronze metadata + DQ columns (same as silver_customers)

-- -----------------------------------------------------------------------------
-- silver_products
-- -----------------------------------------------------------------------------
-- Typed business columns:
--   product_id       INT
--   product_name     STRING
--   category         STRING
--   price            DECIMAL(18,2)
--   cost             DECIMAL(18,2)
--   stock_quantity   INT
--   reorder_level    INT
--
-- Traceability: _source_product_id, _source_product_name, _source_category,
--   _source_price, _source_cost, _source_stock_quantity, _source_reorder_level
-- + Bronze metadata + DQ columns (same as silver_customers)

-- -----------------------------------------------------------------------------
-- silver_dq_metrics
-- Append-only per-run rule metrics (one row per run_id + entity + rule_id)
-- -----------------------------------------------------------------------------
--   run_id            STRING         NOT NULL
--   entity            STRING         NOT NULL
--   check_category    STRING         NOT NULL
--                     -- completeness | uniqueness | referential_integrity | type_business
--   rule_id           STRING         NOT NULL
--   rule_description  STRING         NOT NULL
--   total_rows        BIGINT         NOT NULL
--   failed_rows       BIGINT         NOT NULL
--   passed_rows       BIGINT         NOT NULL
--   pass_pct          DECIMAL(5,2)   NOT NULL
--   fail_pct          DECIMAL(5,2)   NOT NULL
--   evaluated_at      TIMESTAMP      NOT NULL

-- -----------------------------------------------------------------------------
-- silver_dq_report
-- Reporting projection of silver_dq_metrics (overwritten each Silver run)
-- -----------------------------------------------------------------------------
--   dataset               STRING
--   check_name            STRING       -- rule_id
--   total_records         BIGINT
--   passed_records        BIGINT
--   failed_records        BIGINT
--   pass_percentage       DECIMAL(5,2)
--   execution_timestamp   TIMESTAMP

-- -----------------------------------------------------------------------------
-- Silver valid-record views (convenience; also created by create_silver_tables.py)
-- Run only after Silver entity tables exist.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW ecommerce.silver_customers_valid AS
SELECT *
FROM ecommerce.silver_customers
WHERE dq_is_valid = true;

CREATE OR REPLACE VIEW ecommerce.silver_orders_valid AS
SELECT *
FROM ecommerce.silver_orders
WHERE dq_is_valid = true;

CREATE OR REPLACE VIEW ecommerce.silver_products_valid AS
SELECT *
FROM ecommerce.silver_products
WHERE dq_is_valid = true;


-- =============================================================================
-- GOLD LAYER
-- Created by: src/gold/create_gold_tables.py executing src/gold/*.sql
-- Format    : Delta Lake (CREATE OR REPLACE TABLE)
-- Rule      : Completed valid orders only (design D1 + D2); defensive order dedup
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_sales_by_product  (src/gold/01_sales_by_product.sql)
-- Sources: silver_products, silver_orders
-- Grain  : one row per valid product_id
-- -----------------------------------------------------------------------------
--   product_id       INT            NOT NULL
--   product_name     STRING         NOT NULL
--   category         STRING         NOT NULL
--   total_orders     BIGINT         -- COUNT DISTINCT order_id; 0 if none
--   total_revenue    DECIMAL(18,2)  -- SUM(total_amount); 0 if none
--   avg_order_value  DECIMAL(18,2)  -- NULL when total_orders = 0

-- -----------------------------------------------------------------------------
-- gold_revenue_by_customer  (src/gold/02_revenue_by_customer.sql)
-- Sources: silver_customers, silver_orders
-- Grain  : one row per valid customer_id
-- -----------------------------------------------------------------------------
--   customer_id            INT            NOT NULL
--   customer_name          STRING         NOT NULL
--   customer_segment       STRING         NOT NULL  -- Premium | Standard | Basic
--   total_orders           BIGINT
--   total_revenue          DECIMAL(18,2)
--   avg_order_value        DECIMAL(18,2)  -- NULL when total_orders = 0
--   lifetime_value_actual  DECIMAL(18,2)  -- equals total_revenue at customer grain

-- -----------------------------------------------------------------------------
-- gold_daily_weekly_trends  (src/gold/03_daily_weekly_trends.sql)
-- Source: silver_orders
-- Grain  : one row per (period_grain, period_start_date)
-- -----------------------------------------------------------------------------
--   period_grain        STRING         -- 'DAY' | 'WEEK'
--   period_start_date   DATE           -- calendar day or ISO week start (Monday)
--   order_count         BIGINT
--   total_revenue       DECIMAL(18,2)

-- -----------------------------------------------------------------------------
-- gold_customer_segmentation  (src/gold/04_customer_segmentation.sql)
-- Sources: silver_customers, silver_orders
-- Grain  : one row per behavioral segment_type (always 4 rows)
-- -----------------------------------------------------------------------------
--   segment_type    STRING         -- High-Value | Repeat | One-Time | Inactive
--   customer_count  BIGINT
--   avg_revenue     DECIMAL(18,2)  -- NULL when customer_count = 0
--   total_revenue   DECIMAL(18,2)
--
-- Note: segment_type is behavioral (derived). customer_segment on
-- gold_revenue_by_customer is the source marketing tier — different concept.


-- =============================================================================
-- TABLE DEPENDENCY SUMMARY
-- =============================================================================
--
-- customers.csv  --> bronze_customers  --> silver_customers  --+
-- products.csv   --> bronze_products   --> silver_products   --+--> Gold tables
-- orders.csv     --> bronze_orders     --> silver_orders     --+
--                                    \-> silver_dq_metrics
--                                    \-> silver_dq_report
--                                    \-> silver_*_valid (views)
--
-- gold_sales_by_product        <-- silver_products, silver_orders
-- gold_revenue_by_customer     <-- silver_customers, silver_orders
-- gold_daily_weekly_trends     <-- silver_orders
-- gold_customer_segmentation   <-- silver_customers, silver_orders
--
-- Dashboard (src/dashboard/dashboard_queries.sql) reads Gold tables only.
--
-- =============================================================================
-- CREATION ORDER (pipeline scripts — see database/setup-notes.md)
-- =============================================================================
--
-- 1. CREATE SCHEMA (this file or ensure_schema_exists in Python)
-- 2. Bronze ingest: customers -> products -> orders (+ bronze_ingest_audit)
-- 3. Silver: create_silver_tables.py (+ views, metrics, report)
-- 4. Gold: create_gold_tables.py (01 -> 02 -> 03 -> 04)
-- 5. Dashboard: manual setup in Databricks SQL UI
--
-- =============================================================================
