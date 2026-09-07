# Design Notes

This document describes the technical architecture for the e-commerce Medallion pipeline. It aligns with `requirements-analysis.md` and is intended to be simple enough to run on **Databricks Community Edition**.

**Pipeline flow:** CSV → Bronze → Silver → Gold → Dashboard

---

## 1. Overall Architecture

The pipeline follows the Medallion Architecture pattern with four logical stages:

| Stage | Technology | Purpose |
|-------|------------|---------|
| Source | CSV files in `data/` | Simulated operational system extracts |
| Bronze | PySpark + Delta Lake | Raw, append-only landing zone |
| Silver | PySpark + Delta Lake | Typed, standardized, quality-checked data |
| Gold | SQL + Delta Lake | Business aggregations for analytics |
| Dashboard | Databricks SQL Dashboard | Visualization layer over Gold tables |

### High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE (CSV)                              │
│   customers.csv    orders.csv    products.csv                    │
└────────────┬──────────────┬──────────────┬──────────────────────┘
             │              │              │
             ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE (Raw Delta)                           │
│   bronze_customers   bronze_orders   bronze_products           │
└────────────┬──────────────┬──────────────┬──────────────────────┘
             │              │              │
             └──────────────┼──────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SILVER (Validated Delta + DQ)                       │
│   silver_customers   silver_orders   silver_products           │
│   silver_dq_metrics  (optional: silver_dq_row_results)          │
└────────────┬────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   GOLD (Analytics Delta)                         │
│   gold_sales_by_product                                          │
│   gold_revenue_by_customer                                       │
│   gold_customer_segmentation                                     │
└────────────┬────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│              DASHBOARD (Databricks SQL)                          │
│   Bar chart | Histogram | Pie chart                              │
└─────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Separation of concerns** — Ingestion (Bronze), validation (Silver), aggregation (Gold), and presentation (Dashboard) are isolated.
2. **Flag, don't delete** — Invalid records remain in Silver with quality status fields.
3. **Determinism** — Data generation and quality evaluation produce reproducible results for testing.
4. **Community Edition simplicity** — Single schema, DBFS-based paths, full-refresh Silver/Gold on rerun, no external orchestrator required.

### Platform Target: Databricks Community Edition

| Constraint | Design Response |
|------------|-----------------|
| Limited Unity Catalog features | Use default `hive_metastore` database (e.g., `ecommerce`) or a single UC schema if available |
| DBFS storage | Store CSVs under DBFS (e.g., `/FileStore/ecommerce/data/`) or repo-relative `data/` uploaded to DBFS |
| No complex infra | Run scripts as Databricks notebooks or `%run` orchestration; no Kafka/Airflow dependency |
| Modest compute | ~110K rows total — single-node cluster is sufficient |
| Delta Lake | Supported; used for all Bronze/Silver/Gold tables |

Configuration (catalog/schema, paths, run mode) is externalized so the same code works locally (pytest + Spark) and on Community Edition.

---

## 2. Data Flow

### Step-by-Step Flow

| Step | Component | Input | Output |
|------|-----------|-------|--------|
| 0 | `generate_sample_data.py` | Seed/config | `data/*.csv` |
| 1 | `01_ingest_customers.py` | `customers.csv` | `bronze_customers` |
| 2 | `02_ingest_orders.py` | `orders.csv` | `bronze_orders` |
| 3 | `03_ingest_products.py` | `products.csv` | `bronze_products` |
| 4 | `ingest_all.py` | All CSVs | All Bronze tables |
| 5 | Silver DQ modules (`01`–`05`) | Bronze tables | Intermediate DataFrames with rule flags |
| 6 | `create_silver_tables.py` | Bronze + DQ logic | `silver_*` + `silver_dq_metrics` |
| 7 | Gold SQL scripts (`01`, `02`, `04`) | Silver tables | Gold SELECT logic |
| 8 | `create_gold_tables.py` | Silver tables | `gold_*` tables |
| 9 | `dashboard_queries.sql` | Gold tables | Dashboard dataset queries |

### Execution Order

```
generate_sample_data
        ↓
ingest products ──┐
ingest customers ─┼→ ingest orders (after customers + products CSVs exist)
        ↓
create_silver_tables (reads all Bronze; RI checks need customer + product keys)
        ↓
create_gold_tables
        ↓
Dashboard (manual setup in Databricks SQL UI using dashboard_queries.sql)
```

Bronze entity ingests are independent, but **orders Silver referential integrity** logically depends on **customers** and **products** being available in Bronze (and ultimately Silver).

---

## 3. Bronze Responsibilities

Bronze is the **raw landing zone**. Its job is to copy source data into Delta with lineage metadata — nothing more.

### What Bronze Does

- Read CSV files from configured path (local `data/` or DBFS).
- Write one Delta table per source entity.
- Add ingest metadata columns only (see `data-model.md`).
- Record source file name, ingest timestamp, batch/run ID, and source row number.

### What Bronze Does Not Do

- No deduplication.
- No null filtering.
- No referential integrity checks.
- No type casting or business rule enforcement.
- No joins across entities.

### Raw Preservation Strategy

Source business columns are stored **as strings** in Bronze to avoid silent type coercion during ingest. Parsing and casting happen in Silver. This keeps Bronze aligned with the CSV source and supports debugging of malformed values.

---

## 4. Silver Responsibilities

Silver is the **validated system of record**. It standardizes types, applies quality rules, and exposes row-level quality status.

### What Silver Does

1. **Standardize** — Cast Bronze strings to typed columns (INT, DATE, DECIMAL, STRING).
2. **Completeness** — Flag required fields that are NULL or blank.
3. **Uniqueness** — Flag duplicate primary keys (`customer_id`, `order_id`, `product_id`).
4. **Referential integrity** — Flag orders with missing or orphan `customer_id` / `product_id`.
5. **Type/business validation** — Flag invalid enums, negative amounts, bad dates, amount mismatches.
6. **Consolidate quality** — Set `dq_is_valid`, `dq_failed_rules`, and per-category pass flags.
7. **Publish metrics** — Write aggregated pass/fail counts to `silver_dq_metrics`.

### What Silver Does Not Do

- Does not delete bad rows.
- Does not produce final business KPIs (that is Gold).
- Does not modify Bronze tables.

### Silver Module Mapping

| Module | Check Category |
|--------|----------------|
| `01_quality_completeness.py` | Completeness |
| `02_quality_uniqueness.py` | Uniqueness |
| `03_quality_type_validation.py` | Type validation (parsing, enums, ranges) |
| `04_quality_referential_integrity.py` | Referential integrity |
| `05_quality_business_logic.py` | Business validation (e.g., `total_amount` vs `quantity * unit_price`, status/date rules) |

### Valid Rows for Downstream (Design Decision D2)

Gold reads rows where `dq_is_valid = true` on the contributing entity tables. Invalid rows remain in Silver for audit and metrics.

---

## 5. Gold Responsibilities

Gold contains **business-ready analytical datasets** built from validated Silver data.

### What Gold Does

- Join Silver customers, orders, and products for analytics.
- Apply business filters for revenue metrics (design decision D1: **`Completed` orders only**).
- Exclude duplicate-key rows from aggregates (design decision D4: duplicate PK rows are invalid in Silver).
- Compute required metrics:
  - **Sales by product** — order counts and revenue per product.
  - **Revenue by customer** — order counts, revenue, AOV, and `lifetime_value_actual`.
  - **Customer segmentation** — segment-level counts and revenue.

### What Gold Does Not Do

- No row-level quality flagging (already done in Silver).
- No raw ingest or CSV parsing.
- No dashboard rendering logic (Dashboard reads Gold via SQL).

### Metric Definitions (Design Decisions D8, D9, D10)

| Metric | Formula |
|--------|---------|
| `total_orders` | `COUNT` of qualifying order rows at the stated grain |
| `total_revenue` | `SUM(total_amount)` for qualifying orders |
| `avg_order_value` | `total_revenue / total_orders` (NULL if `total_orders = 0`) |
| `lifetime_value_actual` | `SUM(total_amount)` of qualifying orders per customer |
| `segment_type` | `customer_segment` from Silver customers |

### Optional Gold Asset

`03_daily_weekly_trends.sql` is **not required** by the assignment Gold section. Implement only if stretch scope is needed.

---

## 6. Dashboard Responsibilities

The Dashboard is a **read-only presentation layer** on top of Gold tables.

### What the Dashboard Does

- Executes SQL queries from `dashboard_queries.sql` against Gold tables.
- Renders three required visualizations:
  1. **Top 10 products by revenue** — bar chart from `gold_sales_by_product`.
  2. **Customer revenue distribution** — histogram from `gold_revenue_by_customer` (fixed revenue buckets per design decision D5).
  3. **Customer segmentation** — pie chart from `gold_customer_segmentation`.

### What the Dashboard Does Not Do

- No ETL or data quality logic.
- No writes to pipeline tables.
- No dependency on Bronze or Silver (only Gold).

Setup instructions are documented in `src/dashboard/DASHBOARD_GUIDE.md`.

---

## 7. Error Handling

### Fail-Fast Scenarios

The pipeline should raise a clear error and stop when:

| Condition | Layer | Behavior |
|-----------|-------|----------|
| Source CSV file missing | Bronze | Fail with file path in error message |
| Required config (schema, path) missing | All | Fail at startup before processing |
| Bronze table missing when Silver runs | Silver | Fail with table name and remediation hint |
| Silver table missing when Gold runs | Gold | Fail with table name and remediation hint |
| Unparseable config values | All | Fail before data processing |

### Non-Fatal Scenarios (Flag, Continue)

| Condition | Layer | Behavior |
|-----------|-------|----------|
| NULL required field | Silver | Flag row; continue processing |
| Duplicate primary key | Silver | Flag all duplicates; continue |
| Orphan foreign key | Silver | Flag row; continue |
| Invalid enum or negative amount | Silver | Flag row; continue |
| Zero-order customer or product | Gold | Return zero metrics; no error |

### Silent Data Loss

**Never allowed** at any layer. Rows are only excluded from Gold aggregates via explicit, documented filters (`dq_is_valid`, `order_status`, etc.), not by silent deletion in Silver.

### Debugging Support

- `run_id` propagated through Silver metrics and optionally logged at each stage.
- Failed runs documented in `debugging-notes.md`.
- Delta time travel available for post-mortem on Community Edition clusters (short retention).

---

## 8. Data Quality Reporting

### Row-Level Quality (Silver Entity Tables)

Each Silver entity table carries quality status columns (see `data-model.md`):

- `dq_is_valid` — `true` only if all checks pass for the row.
- `dq_failed_rules` — array of failed rule IDs.
- `dq_failure_count` — number of failed rules.
- Optional per-category flags: `dq_completeness_pass`, `dq_uniqueness_pass`, `dq_referential_pass`, `dq_type_business_pass`.

### Aggregated Metrics (`silver_dq_metrics`)

One row per `(run_id, entity, check_category, rule_id)`:

| Metric | Description |
|--------|-------------|
| `total_rows` | Rows evaluated |
| `failed_rows` | Rows that failed the rule |
| `passed_rows` | `total_rows - failed_rows` |
| `pass_pct` | `passed_rows / total_rows * 100` |
| `fail_pct` | `failed_rows / total_rows * 100` |

### Traceability

- **Entity** — `customers`, `orders`, or `products`.
- **Category** — completeness, uniqueness, referential_integrity, type_business.
- **Rule ID** — stable identifier (e.g., `customers_email_not_null`, `orders_customer_id_exists`).
- **Run ID** — ties metrics to a pipeline execution.

### Validation Against Intentional Issues

After a full run on generated sample data, `silver_dq_metrics` fail counts should align with intentional issue volumes documented in `requirements-analysis.md` Section 6 (e.g., 50 NULL emails, 20 duplicate order rows).

Detailed rule definitions will be expanded in `data-quality-strategy.md`.

---

## 9. Logging / Observability

Community Edition does not require external monitoring tools. Observability is built into the pipeline itself.

### Pipeline Logging

| Event | What to Log |
|-------|-------------|
| Stage start/end | Layer name, `run_id`, timestamp |
| Row counts | Read count, written count per table |
| DQ summary | Total invalid rows per entity |
| Errors | Exception message, table/file context |

Implementation: Python `logging` module with INFO for milestones and ERROR for failures. In Databricks notebooks, logs appear in cell output.

### Data Observability (Primary)

| Artifact | Purpose |
|----------|---------|
| `silver_dq_metrics` | Quantitative DQ health per run |
| Silver `dq_*` columns | Row-level investigation |
| Bronze metadata (`_ingest_ts`, `_source_file`, `_batch_id`) | Source lineage |
| Gold row counts | Sanity check after aggregation |

### Optional Enhancements (Not Required)

- Email/Slack alerts on DQ threshold breaches.
- Databricks SQL alert on `fail_pct` > threshold.
- `silver_dq_row_results` normalized audit table for large-scale rule-level querying.

---

## 10. Idempotency / Re-run Strategy

The pipeline supports full reruns suitable for assessment and Community Edition development.

### Run Identifier

Each pipeline execution generates a `run_id` (e.g., `YYYYMMDD_HHMMSS` or UUID). Used in Silver DQ metrics and ingest metadata.

### Layer Strategies

| Layer | Default Mode (Community Edition) | Behavior |
|-------|----------------------------------|----------|
| **Bronze** | `overwrite` per entity table | Full reload from CSV; simplest for assessment reruns |
| **Silver** | `overwrite` per entity table | Rebuild from current Bronze snapshot |
| **Gold** | `CREATE OR REPLACE` / overwrite | Full rebuild from current Silver |
| **DQ metrics** | `overwrite` for current `run_id`, or append with `run_id` filter | Latest run queryable via `MAX(run_id)` |

### Alternative Bronze Mode (Optional)

`append` with `_batch_id` supports historical Bronze retention. For Community Edition assessment, **overwrite is recommended** to avoid duplicate source rows on rerun.

### Safe Rerun Checklist

1. Regenerate CSVs (optional) → `generate_sample_data.py`
2. Rerun Bronze ingest → replaces Bronze tables
3. Rerun Silver → replaces Silver tables and refreshes metrics
4. Rerun Gold → replaces Gold tables
5. Refresh Dashboard → re-run queries or refresh visuals in Databricks SQL

No manual table drops required when using overwrite mode.

---

## 11. Dependency Between Datasets

### Entity Relationship

```
customers (1) ──< orders (many) >── (1) products
```

- `orders.customer_id` → `customers.customer_id`
- `orders.product_id` → `products.product_id`

### Ingest Dependencies

| Dataset | Depends On | Reason |
|---------|------------|--------|
| `customers` | None | Independent source |
| `products` | None | Independent source |
| `orders` | None at Bronze | CSV ingest is independent; FK values stored as-is |

Bronze ingests can run in any order. `ingest_all.py` order: **customers → products → orders** (convention only).

### Silver Dependencies

| Check | Depends On |
|-------|------------|
| Customer completeness/uniqueness | `bronze_customers` only |
| Product completeness/uniqueness | `bronze_products` only |
| Order completeness/uniqueness | `bronze_orders` only |
| Order referential integrity | `silver_customers` or `bronze_customers` for valid `customer_id` set; same for products |

**Recommended:** Build `silver_customers` and `silver_products` first, then `silver_orders` with RI checks against valid customer/product ID sets from Bronze or Silver.

### Gold Dependencies

| Gold Table | Primary Silver Sources |
|------------|------------------------|
| `gold_sales_by_product` | `silver_orders`, `silver_products` |
| `gold_revenue_by_customer` | `silver_orders`, `silver_customers` |
| `gold_customer_segmentation` | `silver_customers`, `silver_orders` |

Gold requires all three Silver entity tables to be built first.

### Dashboard Dependencies

Dashboard depends only on Gold tables being populated.

---

## 12. Testing Strategy

### Test Layers

| Level | Scope | Tooling |
|-------|-------|---------|
| **Unit** | Individual DQ functions (e.g., completeness on a small DataFrame) | `pytest` + local PySpark |
| **Data generation** | Row counts and intentional issue counts | `pytest` on generator output |
| **Bronze integration** | CSV row count = Bronze row count; values preserved | `pytest` or notebook assertion |
| **Silver integration** | Full sample data; metrics match expected fail counts | `pytest` with fixture CSVs |
| **Gold integration** | Known subset produces expected aggregates | SQL assertions on small fixtures |
| **Dashboard smoke** | Queries return expected columns and non-empty results | SQL validation script |

### Test Data

- **Full dataset** — `data/*.csv` from generator for end-to-end validation.
- **Fixtures** — Small CSVs under `tests/fixtures/` for fast unit/integration tests (e.g., 5 customers, 10 orders, 3 products with one of each issue type).

### Independence

Each component exposes testable functions:

| Component | Testable Unit |
|-----------|---------------|
| `generate_sample_data.py` | `generate_customers()`, issue injection helpers |
| Bronze ingest | `ingest_customers(csv_path, config)` |
| Silver DQ modules | `apply_*_checks(df) -> df` |
| Gold SQL | Runnable against in-memory or test schema |
| Dashboard SQL | Runnable as SELECT-only validation |

### Determinism

- Fixed random seed in data generation.
- Fixed rule evaluation order in Silver.
- Stable rule IDs for assertion in tests.

### Community Edition Validation

Manual test path for assessment:

1. Upload repo CSVs to DBFS.
2. Run Bronze → Silver → Gold notebooks sequentially.
3. Query `silver_dq_metrics` and compare to expected fail counts.
4. Query Gold tables and build Dashboard from `dashboard_queries.sql`.

Automated `pytest` runs locally without a Databricks cluster for core logic validation.

---

## Design Decisions Summary

| ID | Decision | Choice |
|----|----------|--------|
| D1 | Revenue order filter | `Completed` orders only |
| D2 | Gold input rows | Silver rows where `dq_is_valid = true` |
| D3 | Bronze typing | All source columns stored as STRING |
| D4 | Duplicates in Gold | Excluded (flagged invalid in Silver) |
| D5 | Histogram buckets | Fixed SQL buckets (documented in dashboard guide) |
| D6 | Community Edition rerun | Overwrite Bronze/Silver/Gold on full pipeline rerun |
| D7 | Schema naming | Single database/schema: `ecommerce` (configurable) |

---

## Related Documents

- `requirements-analysis.md` — Requirements, assumptions, acceptance criteria
- `data-model.md` — Column-level schemas and table names
- `data-quality-strategy.md` — Detailed DQ rules (to be created)
- `database/setup-notes.md` — Databricks schema setup (to be created)
