# Database Setup Notes (Databricks)

Step-by-step instructions to reproduce the e-commerce Medallion pipeline from an empty Databricks workspace (or local Spark environment).

---

## 1. Architecture Summary

```
CSV files (seed data)
        │
        ▼
   BRONZE (Delta)     raw STRING + ingest metadata
        │
        ▼
   SILVER (Delta)     typed columns + DQ flags (invalid rows kept)
        │
        ▼
   GOLD (Delta)       analytics-ready aggregates
        │
        ▼
   DASHBOARD          Databricks SQL Dashboard (read-only)
```

All pipeline tables use **Delta Lake**. Table DDL is documented in `database/schema.sql`; **data is created by Python scripts**, not by running `schema.sql` end-to-end.

---

## 2. Prerequisites

| Requirement | Notes |
|-------------|-------|
| Databricks workspace | Community Edition or full workspace |
| Compute | SQL warehouse (dashboard) + cluster or serverless for PySpark jobs |
| Python 3.9+ | For local generation, tests, and CLI scripts |
| Repository clone | This project on local machine or Databricks Repos |

### Install Python dependencies (local / dev)

```bash
pip install -r requirements-dev.txt
```

Includes `pyspark>=3.4.0` and `pytest>=7.0.0`.

### Attach project to Databricks

- **Repos:** Connect this Git repository to a Databricks Repo.
- **Workspace files:** Upload `src/` and run notebooks/scripts from there.
- **Local + remote:** Generate CSVs locally, upload to DBFS, run ingest on a cluster.

---

## 3. Catalog and Schema Assumptions

Configuration lives in `src/config/pipeline_config.py` and is overridable via environment variables or CLI flags.

| Setting | Env variable | Default | Description |
|---------|--------------|---------|-------------|
| Schema / database | `PIPELINE_SCHEMA` | `ecommerce` | Hive database or Unity Catalog schema |
| Catalog (optional) | `PIPELINE_CATALOG` | *(unset)* | Unity Catalog name; omit for hive metastore |
| CSV base path | `PIPELINE_SOURCE_BASE_PATH` | `dbfs:/FileStore/ecommerce/data` | Directory containing seed CSVs |
| Bronze write mode | `PIPELINE_BRONZE_WRITE_MODE` | `overwrite` | Delta write mode for Bronze |
| Silver write mode | `PIPELINE_SILVER_WRITE_MODE` | `overwrite` | Delta write mode for Silver |
| Gold write mode | `PIPELINE_GOLD_WRITE_MODE` | `overwrite` | Label only; Gold SQL uses `CREATE OR REPLACE` |
| Batch / run ID | `PIPELINE_BATCH_ID`, `PIPELINE_RUN_ID` | UTC timestamp | Correlates ingest and DQ runs |

### Qualified name resolution

```
Without catalog : {schema}.{table}              → ecommerce.bronze_customers
With catalog    : {catalog}.{schema}.{table}    → main.ecommerce.bronze_customers
```

**No workspace-specific catalog is hard-coded.** Set `PIPELINE_CATALOG` only when using Unity Catalog.

### Bootstrap the namespace

**Option A — Python (recommended, used by all pipeline scripts):**

Schema is created automatically on first Bronze or Silver run via `ensure_schema_exists()`.

**Option B — SQL (manual):**

```sql
-- Hive metastore / Databricks CE (default):
CREATE SCHEMA IF NOT EXISTS ecommerce
COMMENT 'E-commerce medallion pipeline';

-- Unity Catalog (when PIPELINE_CATALOG is set):
-- CREATE SCHEMA IF NOT EXISTS {catalog}.ecommerce;
```

See `database/schema.sql` for the full reference schema.

---

## 4. Tables by Layer

### Bronze (4 tables)

| Table | Source | Created by |
|-------|--------|------------|
| `bronze_customers` | `customers.csv` | `src/bronze/01_ingest_customers.py` or `ingest_all.py` |
| `bronze_products` | `products.csv` | `src/bronze/03_ingest_products.py` or `ingest_all.py` |
| `bronze_orders` | `orders.csv` | `src/bronze/02_ingest_orders.py` or `ingest_all.py` |
| `bronze_ingest_audit` | ingest metadata | `bronze_ingest.py` (append per entity run) |

### Silver (5 tables + 3 views)

| Object | Created by |
|--------|------------|
| `silver_customers` | `create_silver_tables.py` |
| `silver_orders` | `create_silver_tables.py` |
| `silver_products` | `create_silver_tables.py` |
| `silver_dq_metrics` | `create_silver_tables.py` (append per run) |
| `silver_dq_report` | `create_silver_tables.py` (overwrite per run) |
| `silver_customers_valid` | view on `silver_customers WHERE dq_is_valid` |
| `silver_orders_valid` | view on `silver_orders WHERE dq_is_valid` |
| `silver_products_valid` | view on `silver_products WHERE dq_is_valid` |

### Gold (4 tables)

| Table | SQL asset | Sources |
|-------|-----------|---------|
| `gold_sales_by_product` | `src/gold/01_sales_by_product.sql` | `silver_products`, `silver_orders` |
| `gold_revenue_by_customer` | `src/gold/02_revenue_by_customer.sql` | `silver_customers`, `silver_orders` |
| `gold_daily_weekly_trends` | `src/gold/03_daily_weekly_trends.sql` | `silver_orders` |
| `gold_customer_segmentation` | `src/gold/04_customer_segmentation.sql` | `silver_customers`, `silver_orders` |

Column definitions: `database/schema.sql`.

---

## 5. Table Dependencies

```
customers.csv ──► bronze_customers ──┐
products.csv  ──► bronze_products  ──┼──► silver_* ──► gold_*
orders.csv    ──► bronze_orders    ──┘         │
                                                 ├── silver_dq_metrics
                                                 ├── silver_dq_report
                                                 └── silver_*_valid (views)

Dashboard queries ──► gold_* only
```

| Downstream | Requires |
|------------|----------|
| `bronze_*` | Seed CSVs at `PIPELINE_SOURCE_BASE_PATH` |
| `silver_*` | All three `bronze_*` entity tables |
| `silver_dq_metrics` / `silver_dq_report` | Silver quality pipeline (internal) |
| `gold_*` | `silver_customers`, `silver_orders`, `silver_products` |
| Dashboard | All four `gold_*` tables |

Referential integrity checks in Silver read `silver_customers` and `silver_products` (Bronze fallback only if Silver missing).

---

## 6. Creation Order

Execute in this sequence. Later steps fail fast if upstream tables are missing.

| Step | Action | Command / script |
|------|--------|------------------|
| 0 | Generate seed CSVs | `python src/data_generation/generate_sample_data.py` |
| 1 | Upload CSVs to DBFS | See `database/seed-data-notes.md` |
| 2 | Create schema | Automatic on step 3, or run bootstrap SQL |
| 3 | Bronze ingest | `customers` → `products` → `orders` |
| 4 | Silver build | `create_silver_tables.py` |
| 5 | Gold build | `create_gold_tables.py` (SQL 01 → 02 → 03 → 04) |
| 6 | Dashboard | Manual setup — `src/dashboard/DASHBOARD_GUIDE.md` |

---

## 7. Sample Data Loading

Full detail: **`database/seed-data-notes.md`**.

Quick path:

```bash
# 1. Generate locally
python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42

# 2. Upload to default Bronze path (Databricks CLI example)
databricks fs mkdirs dbfs:/FileStore/ecommerce/data
databricks fs cp data/customers.csv dbfs:/FileStore/ecommerce/data/customers.csv
databricks fs cp data/products.csv  dbfs:/FileStore/ecommerce/data/products.csv
databricks fs cp data/orders.csv    dbfs:/FileStore/ecommerce/data/orders.csv
```

---

## 8. Pipeline Execution

### Single entry point (recommended)

Run the full pipeline (Bronze → Silver → Gold) in one command:

```bash
python src/run_pipeline.py --help
```

**Local development** (generate CSVs and ingest to a local path):

```bash
python src/run_pipeline.py \
  --generate-sample-data \
  --source-base-path ./data \
  --schema ecommerce
```

**Databricks** (CSVs already on DBFS):

```bash
python src/run_pipeline.py --schema ecommerce
```

**Generate locally, ingest from DBFS** (upload CSVs after generation):

```bash
python src/run_pipeline.py \
  --generate-sample-data \
  --sample-data-output-dir ./data \
  --source-base-path dbfs:/FileStore/ecommerce/data
```

Exit codes: `0` success, `1` pipeline failure, `2` configuration error.

### Step-by-step (manual)

### Environment variables (optional)

```bash
export PIPELINE_SCHEMA=ecommerce
export PIPELINE_SOURCE_BASE_PATH=dbfs:/FileStore/ecommerce/data
# export PIPELINE_CATALOG=main          # Unity Catalog only
# export PIPELINE_BATCH_ID=20250907T120000Z
# export PIPELINE_RUN_ID=20250907T120000Z
```

### Bronze — ingest all entities

From repository root (cluster with Spark attached, or local Spark session):

```bash
python src/bronze/ingest_all.py --schema ecommerce
```

With overrides:

```bash
python src/bronze/ingest_all.py \
  --source-base-path dbfs:/FileStore/ecommerce/data \
  --schema ecommerce \
  --write-mode overwrite \
  --batch-id manual-run-001
```

**Databricks notebook alternative:**

```python
%run /Repos/<user>/ecommerce-medallion-pipeline/src/bronze/ingest_all
```

Or import and call `ingest_all_entities()` from `bronze.bronze_ingest`.

**Verify Bronze:**

```sql
SELECT 'bronze_customers' AS t, COUNT(*) AS n FROM ecommerce.bronze_customers
UNION ALL SELECT 'bronze_products', COUNT(*) FROM ecommerce.bronze_products
UNION ALL SELECT 'bronze_orders', COUNT(*) FROM ecommerce.bronze_orders;
```

Expected: 10000, 500, 100000.

### Silver — quality pipeline + typed tables

```bash
python src/silver/create_silver_tables.py --schema ecommerce
```

With run ID:

```bash
python src/silver/create_silver_tables.py \
  --schema ecommerce \
  --run-id manual-run-001 \
  --write-mode overwrite
```

**What it does:**

1. Reads all Bronze entity tables.
2. Runs DQ modules in order: completeness → uniqueness → type → referential → business logic.
3. Consolidates `dq_*` flags; casts typed columns; preserves `_source_*` traceability.
4. Asserts Bronze row count = Silver row count (no rows dropped).
5. Writes `silver_*` tables, appends `silver_dq_metrics`, overwrites `silver_dq_report`.
6. Creates `silver_*_valid` views.

**Verify Silver:**

```sql
-- Row counts match Bronze
SELECT COUNT(*) FROM ecommerce.silver_orders;

-- Intentional DQ failures (seed=42)
SELECT check_name, failed_records
FROM ecommerce.silver_dq_report
WHERE failed_records > 0
ORDER BY failed_records DESC;

-- Valid vs invalid
SELECT dq_is_valid, COUNT(*) FROM ecommerce.silver_orders GROUP BY dq_is_valid;
```

### Gold — analytical tables

```bash
python src/gold/create_gold_tables.py --schema ecommerce
```

With catalog:

```bash
python src/gold/create_gold_tables.py --catalog main --schema ecommerce
```

**What it does:**

1. Verifies Silver source tables exist.
2. Sets `USE CATALOG` / `USE SCHEMA` as configured.
3. Executes `01_sales_by_product.sql` through `04_customer_segmentation.sql`.
4. Logs row counts and runs reconciliation (revenue totals, no duplicate keys).

**Verify Gold:**

```sql
SELECT COUNT(*) FROM ecommerce.gold_sales_by_product;          -- expect 500
SELECT COUNT(*) FROM ecommerce.gold_revenue_by_customer;       -- expect ~9980 valid customers
SELECT COUNT(*) FROM ecommerce.gold_customer_segmentation;     -- expect 4 segments
SELECT period_grain, COUNT(*) FROM ecommerce.gold_daily_weekly_trends GROUP BY period_grain;
```

**Revenue reconciliation:**

```sql
SELECT CAST(SUM(total_revenue) AS DECIMAL(18,2)) FROM ecommerce.gold_revenue_by_customer;

SELECT CAST(SUM(total_amount) AS DECIMAL(18,2))
FROM ecommerce.silver_orders
WHERE dq_is_valid = true AND order_status = 'Completed'
  AND customer_id IS NOT NULL AND total_amount IS NOT NULL;
```

Totals should match within $0.01 (Gold deduplicates `order_id` defensively).

### Dashboard

Follow **`src/dashboard/DASHBOARD_GUIDE.md`**. Queries in `src/dashboard/dashboard_queries.sql` read Gold tables only.

---

## 9. Running on Databricks Community Edition

Typical workflow:

1. Clone or upload the repository.
2. Create an **all-purpose cluster** (single-node is sufficient for sample data).
3. Generate CSVs on your laptop; upload to `dbfs:/FileStore/ecommerce/data/`.
4. Open a notebook, attach the cluster, and run the three Python orchestrators:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<you>/ecommerce-medallion-pipeline/src")

from config.pipeline_config import load_config
from bronze.bronze_ingest import ingest_all_entities, get_spark
from silver.create_silver_tables import run_create_silver_tables
from gold.create_gold_tables import run_create_gold_tables

config = load_config(schema_name="ecommerce")
spark = get_spark()

ingest_all_entities(spark=spark, config=config)
run_create_silver_tables(spark=spark, config=config)
run_create_gold_tables(spark=spark, config=config)
```

5. Create a **SQL warehouse** and build the dashboard from `dashboard_queries.sql`.

Unity Catalog may be unavailable on CE — leave `PIPELINE_CATALOG` unset.

---

## 10. Local Development and Tests

Run the full test suite (uses generated fixtures, not Databricks):

```bash
python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42
python -m pytest tests/ -v
```

Tests cover data generation, Bronze ingest, Silver DQ, and Gold reconciliation logic.

---

## 11. Cleanup and Reset

### Soft reset — re-run pipeline (recommended)

Default write modes overwrite entity tables each run:

```bash
python src/bronze/ingest_all.py --schema ecommerce --write-mode overwrite
python src/silver/create_silver_tables.py --schema ecommerce --write-mode overwrite
python src/gold/create_gold_tables.py --schema ecommerce
```

Gold SQL always uses `CREATE OR REPLACE TABLE`.

### Drop all pipeline objects

Replace `ecommerce` with your `PIPELINE_SCHEMA`. Add catalog prefix if used.

```sql
USE ecommerce;

-- Gold
DROP TABLE IF EXISTS gold_customer_segmentation;
DROP TABLE IF EXISTS gold_daily_weekly_trends;
DROP TABLE IF EXISTS gold_revenue_by_customer;
DROP TABLE IF EXISTS gold_sales_by_product;

-- Silver views
DROP VIEW IF EXISTS silver_products_valid;
DROP VIEW IF EXISTS silver_orders_valid;
DROP VIEW IF EXISTS silver_customers_valid;

-- Silver tables
DROP TABLE IF EXISTS silver_dq_report;
DROP TABLE IF EXISTS silver_dq_metrics;
DROP TABLE IF EXISTS silver_products;
DROP TABLE IF EXISTS silver_orders;
DROP TABLE IF EXISTS silver_customers;

-- Bronze
DROP TABLE IF EXISTS bronze_ingest_audit;
DROP TABLE IF EXISTS bronze_orders;
DROP TABLE IF EXISTS bronze_products;
DROP TABLE IF EXISTS bronze_customers;
```

### Drop schema (full teardown)

```sql
DROP SCHEMA IF EXISTS ecommerce CASCADE;
```

On Unity Catalog: `DROP SCHEMA IF EXISTS {catalog}.ecommerce CASCADE;`

### Remove seed files from DBFS

```bash
databricks fs rm -r dbfs:/FileStore/ecommerce/data
```

Regenerate and re-upload CSVs before the next Bronze run.

### Audit / metrics tables

| Table | Reset behavior |
|-------|----------------|
| `bronze_ingest_audit` | Append-only; drop table or `DELETE` to clear history |
| `silver_dq_metrics` | Append-only per Silver run; drop to clear history |
| `silver_dq_report` | Overwritten each Silver run |

---

## 12. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Bronze table does not exist` | Skipped ingest | Run `ingest_all.py` |
| `Source file not found` | CSVs not at `PIPELINE_SOURCE_BASE_PATH` | Upload CSVs or pass `--source-base-path` |
| Silver row count mismatch | Unexpected Bronze filter | Re-ingest Bronze with `overwrite` |
| Gold revenue mismatch | Stale Silver or duplicate orders | Re-run Silver then Gold; check `create_gold_tables.py` logs |
| `CREATE SCHEMA` permission error | Catalog ACLs | Use hive metastore or grant `CREATE SCHEMA` on UC |
| Dashboard empty | Gold not built or wrong schema | `USE ecommerce; SELECT COUNT(*) FROM gold_sales_by_product` |

---

## 13. Related Documents

| Document | Contents |
|----------|----------|
| `database/schema.sql` | Column-level DDL reference and dependency diagram |
| `database/seed-data-notes.md` | CSV generation, corruption catalog, DBFS upload |
| `src/dashboard/DASHBOARD_GUIDE.md` | Dashboard build and validation |
| `design-notes.md` | Architecture and design decisions D1–D10 |
| `data-quality-strategy.md` | DQ rules and expected failure counts |
| `src/config/pipeline_config.py` | All configurable table names and paths |
