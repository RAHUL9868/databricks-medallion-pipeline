# E-Commerce Medallion Pipeline

A Databricks-oriented **Medallion Architecture** pipeline that ingests e-commerce CSV extracts, validates data quality in Silver, builds analytical Gold tables, and feeds a Databricks SQL Dashboard.

---

# Project Overview

This repository implements a full **Bronze → Silver → Gold** analytics pipeline with explicit data quality (DQ) rules, measurable quality metrics, and reconciliation checks. Source data is simulated via a deterministic CSV generator that injects realistic defects for testing.

The pipeline is designed for **Databricks Community Edition** (DBFS paths, Delta Lake, SQL Dashboard) and can also run **locally** with PySpark for development and `pytest`.

**Primary entry point:** `src/run_pipeline.py` (end-to-end orchestration).

---

## Business Problem

An e-commerce company receives daily extracts from three operational systems:

- **Customers** — registration and marketing tier
- **Orders** — transactions with status (Pending, Completed, Cancelled)
- **Products** — catalog and pricing

The raw CSV files contain real-world imperfections: NULL foreign keys, duplicate primary keys, invalid references, and inconsistent amounts. Business stakeholders need **trustworthy KPIs** (product revenue, customer lifetime value, segment mix) without silently dropping bad records.

This project demonstrates how to:

1. Land raw data with full lineage (Bronze).
2. Type, validate, and **flag** bad rows while retaining them (Silver).
3. Publish analytics-ready aggregates from **valid, completed** transactions only (Gold).
4. Visualize results in a Databricks SQL Dashboard.

---

## Architecture

```
Source CSV
    │
    │  customers.csv, products.csv, orders.csv
    ▼
Bronze (Delta)
    │  bronze_customers, bronze_orders, bronze_products, bronze_ingest_audit
    │  All business columns as STRING + ingest metadata
    ▼
Silver (Delta)
    │  silver_customers, silver_orders, silver_products
    │  Typed columns + dq_* flags + _source_* traceability
    │  silver_*_valid (views: dq_is_valid = true)
    ▼
Quality Metrics (Delta)
    │  silver_dq_metrics (append per run), silver_dq_report (overwrite per run)
    ▼
Gold (Delta)
    │  gold_sales_by_product, gold_revenue_by_customer,
    │  gold_daily_weekly_trends, gold_customer_segmentation
    ▼
Dashboard (Databricks SQL)
       Top 10 Products | Revenue Histogram | Segmentation Pie
```

**Principles**

| Principle | Implementation |
|-----------|----------------|
| Flag, don't delete | Invalid rows stay in `silver_*` with `dq_is_valid = false` |
| Separation of concerns | Ingest (Bronze), validate (Silver), aggregate (Gold), present (Dashboard) |
| Determinism | Seed `42` generator + documented expected DQ failure counts |
| Config externalized | `src/config/pipeline_config.py` + environment variables |

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.9+ |
| Processing | Apache Spark (PySpark 3.4+) |
| Storage | Delta Lake tables |
| Gold transforms | Spark SQL (`CREATE OR REPLACE TABLE`) |
| Dashboard | Databricks SQL Dashboard (manual setup) |
| Testing | `pytest` + local Spark session |
| Sample data | Standard library only (`generate_sample_data.py`) |

**Target platform:** Databricks (Community Edition or workspace cluster). **Local:** PySpark + Java 8/11 for unit/integration tests.

---

## Repository Structure

```
ecommerce-medallion-pipeline/
├── README.md                          # This file
├── requirements-dev.txt               # pytest, pyspark (dev/test)
├── design-notes.md                    # Architecture and design decisions
├── data-model.md                      # Logical schemas (may lag code slightly)
├── data-quality-strategy.md           # DQ rule catalog
├── database/
│   ├── schema.sql                     # Reference DDL (not sole creation mechanism)
│   ├── setup-notes.md                 # Databricks setup and reset
│   └── seed-data-notes.md             # CSV generation and upload
├── src/
│   ├── run_pipeline.py                # ★ End-to-end orchestrator
│   ├── config/
│   │   └── pipeline_config.py         # Paths, schema, table names, env vars
│   ├── data_generation/
│   │   ├── generate_sample_data.py
│   │   └── DATA_GENERATION_NOTES.md
│   ├── bronze/
│   │   ├── bronze_ingest.py           # Shared ingest logic
│   │   ├── bronze_schemas.py
│   │   ├── ingest_all.py
│   │   └── 01_ingest_*.py … 03_ingest_*.py
│   ├── silver/
│   │   ├── create_silver_tables.py    # Silver orchestrator
│   │   ├── silver_common.py
│   │   ├── silver_dq_rules.py
│   │   └── 01_quality_*.py … 05_quality_*.py
│   ├── gold/
│   │   ├── create_gold_tables.py
│   │   └── 01_ … 04_*.sql
│   └── dashboard/
│       ├── dashboard_queries.sql
│       └── DASHBOARD_GUIDE.md
├── tests/                             # pytest suite
└── data/                              # Generated CSVs (not committed; create locally)
```

---

## Source Data

CSV files are **not committed**. Generate them with `generate_sample_data.py`.

| File | Rows (seed=42) | Key columns |
|------|----------------|-------------|
| `customers.csv` | 10,000 | `customer_id`, `email`, `customer_segment`, `signup_date`, … |
| `products.csv` | 500 | `product_id`, `product_name`, `category`, `price`, `cost`, … |
| `orders.csv` | 100,000 | `order_id`, `customer_id`, `product_id`, `order_status`, `total_amount`, … |

Default ingest location (configurable):

```
dbfs:/FileStore/ecommerce/data/
```

For local runs, use a filesystem path or `file:` URI, e.g. `./data`.

See `database/seed-data-notes.md` and `src/data_generation/DATA_GENERATION_NOTES.md` for column-level detail.

---

## Data Quality Issues

Intentional issues in generated data (seed **42**) exercise Silver rules:

| Issue | Count | Silver rule ID |
|-------|------:|----------------|
| NULL `email` (customers) | 50 | `customers_email_not_null` |
| Duplicate `customer_id` (20 rows in dup groups) | 10 keys | `customers_customer_id_unique` |
| NULL `customer_id` (orders) | 100 | `orders_customer_id_not_null` |
| NULL `product_id` (orders) | 200 | `orders_product_id_not_null` |
| Invalid `customer_id` (9000001–9000050) | 50 | `orders_customer_id_exists` |
| Invalid `product_id` (9000001–9000030) | 30 | `orders_product_id_exists` |
| Duplicate `order_id` (40 rows in dup groups) | 20 keys | `orders_order_id_unique` |
| Products | 0 intentional issues | — |

Natural data: ~70% of orders are `Completed`, 20% `Pending`, 10% `Cancelled`. Only **Completed** orders count toward Gold revenue.

---

## Setup

### Prerequisites

1. **Python 3.9+**
2. **Java 8 or 11** (required for local PySpark)
3. **Databricks workspace** (for production path) with:
   - Cluster or serverless Spark for pipeline scripts
   - SQL warehouse for Dashboard
4. **Git clone** of this repository

### Install dependencies (local dev / tests)

From the repository root:

```bash
pip install -r requirements-dev.txt
```

There is no separate `requirements.txt`; runtime on Databricks typically uses cluster libraries (PySpark preinstalled).

### Databricks: attach the repository

- Use **Repos** to clone this project, or upload `src/` to the workspace.
- Ensure the cluster can read your CSV path (DBFS or cloud storage).

### Create the target schema

The pipeline creates the schema automatically on first run. Optionally:

```sql
CREATE SCHEMA IF NOT EXISTS ecommerce;
```

See `database/setup-notes.md` for Unity Catalog vs hive metastore.

---

## Configuration

All settings live in `src/config/pipeline_config.py` and can be overridden via **environment variables** or CLI flags.

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_SCHEMA` | `ecommerce` | Database / schema name |
| `PIPELINE_CATALOG` | *(unset)* | Unity Catalog name (optional) |
| `PIPELINE_SOURCE_BASE_PATH` | `dbfs:/FileStore/ecommerce/data` | Directory containing CSV files |
| `PIPELINE_BATCH_ID` | UTC timestamp | Bronze ingest batch ID |
| `PIPELINE_RUN_ID` | Same as batch ID | Silver/Gold run correlation |
| `PIPELINE_BRONZE_WRITE_MODE` | `overwrite` | Bronze Delta write mode |
| `PIPELINE_SILVER_WRITE_MODE` | `overwrite` | Silver entity table write mode |
| `PIPELINE_GOLD_WRITE_MODE` | `overwrite` | Logged only; Gold SQL uses `CREATE OR REPLACE` |

Qualified table names:

- Without catalog: `ecommerce.bronze_customers`
- With catalog: `{catalog}.ecommerce.bronze_customers`

---

## Running Data Generation

Generates deterministic CSVs under `./data` by default (standard Python only; no Spark required).

```bash
python src/data_generation/generate_sample_data.py
```

Options:

```bash
python src/data_generation/generate_sample_data.py \
  --output-dir ./data \
  --seed 42 \
  --log-level INFO
```

**Upload to Databricks** (if using DBFS default path):

```bash
databricks fs mkdirs dbfs:/FileStore/ecommerce/data
databricks fs cp data/customers.csv dbfs:/FileStore/ecommerce/data/customers.csv
databricks fs cp data/products.csv  dbfs:/FileStore/ecommerce/data/products.csv
databricks fs cp data/orders.csv    dbfs:/FileStore/ecommerce/data/orders.csv
```

---

## Running Bronze

Ingest CSVs into Bronze Delta tables. Recommended entity order: **customers → products → orders**.

### All entities

```bash
python src/bronze/ingest_all.py --schema ecommerce
```

With local CSV path:

```bash
python src/bronze/ingest_all.py \
  --source-base-path ./data \
  --schema ecommerce \
  --write-mode overwrite
```

### Single entity

```bash
python src/bronze/01_ingest_customers.py --schema ecommerce --source-base-path ./data
python src/bronze/03_ingest_products.py  --schema ecommerce --source-base-path ./data
python src/bronze/02_ingest_orders.py    --schema ecommerce --source-base-path ./data
```

### Verify

```sql
SELECT COUNT(*) FROM ecommerce.bronze_customers;  -- 10000
SELECT COUNT(*) FROM ecommerce.bronze_products;   -- 500
SELECT COUNT(*) FROM ecommerce.bronze_orders;     -- 100000
```

---

## Running Silver

Runs completeness → uniqueness → type → referential integrity → business logic, then writes Silver tables, metrics, and valid-record views.

```bash
python src/silver/create_silver_tables.py --schema ecommerce
```

Options: `--catalog`, `--write-mode`, `--run-id`, `--log-level`.

### Verify row counts (unchanged from Bronze)

```sql
SELECT COUNT(*) FROM ecommerce.silver_orders;  -- 100000
```

### Verify intentional failures

```sql
SELECT check_name, failed_records
FROM ecommerce.silver_dq_report
WHERE failed_records > 0
ORDER BY failed_records DESC;
```

Expected top failures: `orders_product_id_not_null` (200), `orders_customer_id_not_null` (100), `orders_order_id_unique` (40), `customers_email_not_null` (50), etc.

---

## Running Gold

Executes four SQL scripts in order and runs reconciliation checks against Silver.

```bash
python src/gold/create_gold_tables.py --schema ecommerce
```

### Gold tables produced

| Table | Purpose |
|-------|---------|
| `gold_sales_by_product` | Revenue and orders per product |
| `gold_revenue_by_customer` | Lifetime revenue per customer |
| `gold_daily_weekly_trends` | Daily and weekly revenue trends |
| `gold_customer_segmentation` | Behavioral segments (4 rows) |

### Verify

```sql
SELECT COUNT(*) FROM ecommerce.gold_sales_by_product;          -- 500
SELECT COUNT(*) FROM ecommerce.gold_customer_segmentation;     -- 4
```

Reconciliation runs automatically inside `create_gold_tables.py` (revenue totals, row counts, duplicate keys).

---

## Running End-to-End Pipeline

**Recommended** for a full refresh:

```bash
python src/run_pipeline.py --help
```

### Local (generate + run)

```bash
python src/run_pipeline.py \
  --generate-sample-data \
  --source-base-path ./data \
  --schema ecommerce
```

### Databricks (CSVs already on DBFS)

```bash
python src/run_pipeline.py --schema ecommerce
```

### Generate locally, ingest from DBFS

```bash
python src/run_pipeline.py \
  --generate-sample-data \
  --sample-data-output-dir ./data \
  --source-base-path dbfs:/FileStore/ecommerce/data
```

Then upload `./data/*.csv` to DBFS before ingest completes (or run generation in a prior step).

### Pipeline stages (in order)

1. Validate configuration  
2. Generate / validate sample data (optional)  
3. Ingest customers → products → orders (Bronze)  
4. Completeness, uniqueness, type, referential, business checks (Silver)  
5. Build Silver tables + DQ metrics  
6. Build Gold tables  
7. Reconciliation checks  
8. Final validation  

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Pipeline execution failure |
| `2` | Configuration error |

---

## Testing

From the repository root (requires Java + PySpark):

```bash
python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42
python -m pytest tests/ -v
```

Test modules cover data generation, Bronze ingest, Silver DQ (including intentional issue counts at seed=42), Gold helpers, and pipeline configuration.

```bash
python -m pytest tests/test_silver_quality.py -v
python -m pytest tests/test_run_pipeline.py -v
```

---

## Data Quality Validation

### Row-level flags (Silver)

| Column | Meaning |
|--------|---------|
| `dq_is_valid` | `true` when no rules failed |
| `dq_record_status` | `valid`, `invalid`, or `invalid_multiple` |
| `dq_failed_rules` | Array of failed rule IDs |
| `dq_*_pass` | Per-category pass booleans |

Filter valid rows: `WHERE dq_is_valid = true` or use `silver_*_valid` views.

### Metrics tables

| Table | Behavior |
|-------|----------|
| `silver_dq_metrics` | One row per `run_id` + entity + rule; **appends** each run |
| `silver_dq_report` | Human-readable summary; **overwritten** each Silver run |

Query latest metrics:

```sql
SELECT * FROM ecommerce.silver_dq_report ORDER BY failed_records DESC;
```

Compare to `tests/quality_expectations.py` for seed=42 expected counts.

---

## Gold Metrics

| Metric | Definition |
|--------|------------|
| **Revenue** | `SUM(total_amount)` from `order_status = 'Completed'`, `dq_is_valid = true`, deduplicated by `order_id` |
| **Orders** | `COUNT(DISTINCT order_id)` under same filters |
| **AOV** | `total_revenue / total_orders` (NULL when zero orders) |
| **Segmentation** | Behavioral labels: `High-Value`, `Repeat`, `One-Time`, `Inactive` (see `src/gold/04_customer_segmentation.sql`) |

**Note:** `SUM(gold_sales_by_product.total_revenue)` and `SUM(gold_revenue_by_customer.total_revenue)` are **not** required to match — product Gold requires `product_id`; customer Gold requires `customer_id`. NULL-FK orders are excluded from different subsets.

`customer_segment` on `gold_revenue_by_customer` is the **marketing tier** (Premium/Standard/Basic). `segment_type` on `gold_customer_segmentation` is **behavioral** — different concepts.

---

## Dashboard

The Dashboard is **not** created by code. Set it up manually in Databricks SQL using:

- **Queries:** `src/dashboard/dashboard_queries.sql`
- **Guide:** `src/dashboard/DASHBOARD_GUIDE.md`

### Required visualizations

| Chart | Gold source | Type |
|-------|-------------|------|
| Top 10 Products by Revenue | `gold_sales_by_product` | Bar |
| Customer Revenue Distribution | `gold_revenue_by_customer` | Histogram |
| Customer Segmentation | `gold_customer_segmentation` | Pie |

Prerequisites: Gold tables built, SQL warehouse running, schema set to `ecommerce` (or edit `USE` in queries).

---

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `Missing source file` | CSVs not at `PIPELINE_SOURCE_BASE_PATH` | Generate + upload; pass `--source-base-path` |
| `Bronze table does not exist` | Skipped ingest | Run `ingest_all.py` or `run_pipeline.py` |
| Silver/Gold failure after Silver change | Stale Gold | Re-run `create_gold_tables.py` |
| `Configuration error` on `--generate-sample-data` with DBFS path | Cannot write CSVs to DBFS from local Python | Use `--sample-data-output-dir ./data` and upload |
| PySpark / Java errors locally | JDK not installed | Install Java 8/11; set `JAVA_HOME` |
| Dashboard empty | Gold not built or wrong schema | `SELECT COUNT(*) FROM ecommerce.gold_sales_by_product` |
| Duplicate metrics rows | Expected on rerun | Filter `silver_dq_metrics` by `run_id` |
| Reconciliation revenue mismatch | Silver not refreshed | Re-run Silver then Gold |

**Reset all tables:** see `database/setup-notes.md` § Cleanup.

---

## Assumptions

1. **Single schema** (`ecommerce` by default) is sufficient for Community Edition.
2. **Completed** orders represent recognized revenue; Pending/Cancelled are excluded from Gold (design D1).
3. **USD** monetary amounts; `DECIMAL(18,2)` throughout Gold.
4. **Sample data seed 42** is the reference for tests and documentation.
5. **One row per economic order** after deduplication by `order_id` (earliest `_source_row_num` wins).
6. **Segmentation thresholds** ($1,000 high-value, 365-day inactivity) are documented engineering assumptions in Gold SQL, not business-signed SLAs.
7. **Dashboard** is read-only over Gold; no operational write-back.

---

## Design Decisions

| ID | Decision | Choice |
|----|----------|--------|
| D1 | Revenue recognition | `order_status = 'Completed'` only |
| D2 | Gold input quality | `dq_is_valid = true` on Silver |
| D3 | Bronze typing | All source columns stored as STRING |
| D4 | Duplicate keys in Gold | Excluded via Silver DQ + defensive order dedup |
| D5 | Histogram buckets | Fixed SQL buckets in dashboard query |
| D6 | Full pipeline rerun | Overwrite Bronze/Silver/Gold entity tables |
| D7 | Schema naming | Configurable; default `ecommerce` |

Full rationale: `design-notes.md`.

---

## Known Limitations

1. **CSV files are not in git** — must be generated or supplied.
2. **Dashboard requires manual Databricks SQL setup** — no automated deploy.
3. **`silver_dq_metrics` appends** on each run; filter by `run_id` for latest-only views.
4. **Gold SQL hardcodes table names** — `PipelineConfig` table name overrides are not applied inside `.sql` files.
5. **`gold_write_mode=append` has no effect** — Gold scripts always `CREATE OR REPLACE TABLE`.
6. **Bronze `append` mode** can duplicate rows on rerun; default is `overwrite`.
7. **`_source_row_num`** uses Spark row ordering, not guaranteed physical CSV line order (dedup tie-breaker).
8. **Segmentation uses `CURRENT_DATE()`** — inactive customers can change segments when rerun on a later calendar date.
9. **Some rules in `data-quality-strategy.md`** are documented but not implemented (e.g. `products_product_id_unique`); see `src/silver/silver_dq_rules.py` for the authoritative rule list.
10. **Gold integration tests** are limited; reconciliation is exercised in `create_gold_tables.py` at runtime, not fully in `pytest`.
11. **Local pipeline** requires a Spark session with access to the configured source path (local `file:` or cluster DBFS).

---

## Future Improvements

- [ ] Header-based CSV validation before explicit-schema read (fail fast on missing columns)
- [ ] Deduplicate `product_id` in `gold_sales_by_product.sql` (parity with customer Gold)
- [ ] Segmentation revenue reconciliation in `create_gold_tables.py`
- [ ] Parameterize Gold SQL table names or remove unsupported config overrides
- [ ] Metrics overwrite / partition by `run_id` instead of append-only
- [ ] Full Gold SQL integration test in `pytest`
- [ ] Optional `PIPELINE_AS_OF_DATE` for deterministic segmentation
- [ ] CI workflow (generate data → pytest → optional Databricks deploy)
- [ ] Sync `data-model.md` with implemented schemas and behavioral segmentation

---

## Related Documentation

| Document | Contents |
|----------|----------|
| `database/setup-notes.md` | Databricks setup, execution order, reset |
| `database/databricks-git-setup.md` | Import GitHub repo into Databricks Repos |
| `notebooks/run_full_pipeline.ipynb` | Starter notebook for full pipeline on Databricks |
| `database/seed-data-notes.md` | CSV volumes, corruptions, upload |
| `database/schema.sql` | Reference table definitions |
| `src/dashboard/DASHBOARD_GUIDE.md` | Dashboard build and validation |
| `data-quality-strategy.md` | DQ rules and categories |
| `design-notes.md` | Architecture deep dive |

---

## Quick Start (Copy-Paste)

```bash
# 1. Install
pip install -r requirements-dev.txt

# 2. Generate sample CSVs
python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42

# 3. Run full pipeline locally (requires Java + PySpark)
python src/run_pipeline.py --generate-sample-data --source-base-path ./data --schema ecommerce

# 4. Run tests
python -m pytest tests/ -v
```

On **Databricks**, upload CSVs to DBFS, attach this repo to a cluster, then either:

**Option A — Notebook (recommended):** open `notebooks/run_full_pipeline.ipynb`, set widgets, run all cells.

**Option B — CLI on the cluster:**

```bash
python src/run_pipeline.py --schema ecommerce --source-base-path dbfs:/FileStore/ecommerce/data
```

Then follow `src/dashboard/DASHBOARD_GUIDE.md` to build the SQL Dashboard.
