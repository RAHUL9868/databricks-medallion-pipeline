# Seed Data Notes

This document explains how to generate, validate, and load the sample CSV files used by the e-commerce Medallion pipeline on Databricks.

---

## Overview

| Item | Detail |
|------|--------|
| Generator | `src/data_generation/generate_sample_data.py` |
| Output files | `customers.csv`, `products.csv`, `orders.csv` |
| Default output directory | `<repo>/data/` |
| Default random seed | `42` (reproducible) |
| Runtime dependency | Standard Python only (no Spark/Databricks required to generate) |

CSV files are **not committed** to the repository. Every engineer must generate them locally (or in CI) before Bronze ingestion.

---

## File Volumes

| File | Rows | Unique keys (before intentional duplicates) |
|------|------|---------------------------------------------|
| `customers.csv` | 10,000 | 9,990 unique `customer_id` + 10 duplicate rows |
| `products.csv` | 500 | 500 unique `product_id` |
| `orders.csv` | 100,000 | 99,980 unique `order_id` + 20 duplicate rows |

---

## CSV Column Schemas

### `customers.csv`

| Column | Type in CSV | Notes |
|--------|-------------|-------|
| `customer_id` | integer string | `1` … `9,990` plus 10 duplicated rows |
| `customer_name` | string | Random first + last name |
| `email` | string or empty | 50 rows intentionally NULL/blank |
| `country` | string | One of 18 countries |
| `signup_date` | `YYYY-MM-DD` | `2018-01-01` – `2025-06-30` |
| `customer_segment` | string | `Premium` (20%), `Standard` (50%), `Basic` (30%) |
| `lifetime_value` | decimal string | Segment-based; 2 decimal places |

### `products.csv`

| Column | Type in CSV | Notes |
|--------|-------------|-------|
| `product_id` | integer string | `1` … `500` |
| `product_name` | string | Unique adjective + noun |
| `category` | string | One of 8 categories |
| `price` | decimal string | `5.00` – `500.00` |
| `cost` | decimal string | 55%–85% of price (`cost <= price` for natural rows) |
| `stock_quantity` | integer string | `0` – `2,000` |
| `reorder_level` | integer string | 10%–35% of stock (minimum 10) |

### `orders.csv`

| Column | Type in CSV | Notes |
|--------|-------------|-------|
| `order_id` | integer string | `1` … `99,980` plus 20 duplicated rows |
| `customer_id` | integer string or empty | 100 NULL rows; 50 invalid FK (`9000001`–`9000050`) |
| `order_date` | `YYYY-MM-DD` | `2019-01-01` – `2025-08-31`; not before customer signup |
| `product_id` | integer string or empty | 200 NULL rows; 30 invalid FK (`9000001`–`9000030`) |
| `quantity` | integer string | `1` – `5` |
| `unit_price` | decimal string | Product price ± 5% |
| `total_amount` | decimal string | Always `quantity × unit_price` (half-up to 2 decimals) |
| `order_status` | string | `Pending` (20%), `Completed` (70%), `Cancelled` (10%) |
| `payment_date` | `YYYY-MM-DD` or empty | Set for Completed orders; NULL for Pending/Cancelled |

---

## Intentional Data Quality Issues (seed = 42)

These corruptions exercise Silver DQ rules. Disjoint index sets prevent one row from receiving multiple corruption types (except duplicate rows, which copy an existing row verbatim).

### Customers

| Issue | Affected rows | Silver rule ID |
|-------|---------------|----------------|
| NULL `email` | 50 | `customers_email_not_null` |
| Duplicate `customer_id` | 10 keys → 20 rows in duplicate groups | `customers_customer_id_unique` |

### Orders

| Issue | Affected rows | Silver rule ID |
|-------|---------------|----------------|
| NULL `customer_id` | 100 | `orders_customer_id_not_null` |
| NULL `product_id` | 200 | `orders_product_id_not_null` |
| Invalid `customer_id` (`9000001`–`9000050`) | 50 | `orders_customer_id_exists` |
| Invalid `product_id` (`9000001`–`9000030`) | 30 | `orders_product_id_exists` |
| Duplicate `order_id` | 20 keys → 40 rows in duplicate groups | `orders_order_id_unique` |

### Products

No intentional corruptions — all 500 `product_id` values are unique with non-negative numeric fields.

### Expected Silver DQ failure counts (after full pipeline run)

| Rule ID | Expected `failed_rows` |
|---------|------------------------|
| `customers_email_not_null` | 50 |
| `customers_customer_id_unique` | 20 |
| `orders_customer_id_not_null` | 100 |
| `orders_product_id_not_null` | 200 |
| `orders_customer_id_exists` | 50 |
| `orders_product_id_exists` | 30 |
| `orders_order_id_unique` | 40 |

See `data-quality-strategy.md` and `src/data_generation/DATA_GENERATION_NOTES.md` for full rule definitions.

---

## Generate Seed Data

From the repository root:

```bash
python src/data_generation/generate_sample_data.py
```

### Options

```bash
python src/data_generation/generate_sample_data.py \
  --output-dir ./data \
  --seed 42 \
  --log-level INFO
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-dir` | `<repo>/data` | Directory for output CSV files |
| `--seed` | `42` | Random seed for reproducibility |
| `--log-level` | `INFO` | Logging verbosity |

The generator validates row counts and corruption counts **before writing** and exits with an error if any check fails.

---

## Validate Generated Files (Local)

### Row counts

```bash
python -c "
import csv
from pathlib import Path
for name in ('customers','products','orders'):
    path = Path('data') / f'{name}.csv'
    with path.open() as f:
        n = sum(1 for _ in csv.reader(f)) - 1
    print(f'{name}.csv: {n} rows')
"
```

**Expected:** `customers.csv: 10000`, `products.csv: 500`, `orders.csv: 100000`

### Customer issues

```bash
python -c "
import csv
from collections import Counter
from pathlib import Path

with (Path('data')/'customers.csv').open() as f:
    rows = list(csv.DictReader(f))
null_email = sum(1 for r in rows if not (r.get('email') or '').strip())
ids = [int(r['customer_id']) for r in rows if r['customer_id'].strip()]
dup_rows = sum(c for c in Counter(ids).values() if c > 1)
print('NULL email:', null_email)
print('Rows in duplicate groups:', dup_rows)
"
```

**Expected:** `NULL email: 50`, `Rows in duplicate groups: 20`

### Order issues

```bash
python -c "
import csv
from collections import Counter
from pathlib import Path

with (Path('data')/'customers.csv').open() as f:
    valid_customers = {int(r['customer_id']) for r in csv.DictReader(f) if r['customer_id'].strip()}
with (Path('data')/'products.csv').open() as f:
    valid_products = {int(r['product_id']) for r in csv.DictReader(f) if r['product_id'].strip()}
with (Path('data')/'orders.csv').open() as f:
    rows = list(csv.DictReader(f))

def blank(v): return not (v or '').strip()
print('NULL customer_id:', sum(1 for r in rows if blank(r['customer_id'])))
print('NULL product_id:', sum(1 for r in rows if blank(r['product_id'])))
print('Invalid customer_id:', sum(1 for r in rows if not blank(r['customer_id']) and int(r['customer_id']) not in valid_customers))
print('Invalid product_id:', sum(1 for r in rows if not blank(r['product_id']) and int(r['product_id']) not in valid_products))
print('Rows in duplicate order_id groups:', sum(c for c in Counter([int(r['order_id']) for r in rows if r['order_id'].strip()]).values() if c > 1))
"
```

### Reproducibility

Run twice with the same seed and compare files — they must be identical.

---

## Load Seed Data into Databricks

Bronze ingestion reads CSVs from a configurable base path (`PIPELINE_SOURCE_BASE_PATH`). Default:

```
dbfs:/FileStore/ecommerce/data
```

### Option A — Databricks UI (Community Edition friendly)

1. Generate CSVs locally (`data/` folder).
2. In the Databricks workspace: **Data** → **Upload Data** (or DBFS browser).
3. Create folder `FileStore/ecommerce/data/` if it does not exist.
4. Upload `customers.csv`, `products.csv`, `orders.csv`.
5. Confirm paths:
   - `dbfs:/FileStore/ecommerce/data/customers.csv`
   - `dbfs:/FileStore/ecommerce/data/products.csv`
   - `dbfs:/FileStore/ecommerce/data/orders.csv`

### Option B — Databricks CLI

```bash
databricks fs mkdirs dbfs:/FileStore/ecommerce/data
databricks fs cp data/customers.csv dbfs:/FileStore/ecommerce/data/customers.csv
databricks fs cp data/products.csv  dbfs:/FileStore/ecommerce/data/products.csv
databricks fs cp data/orders.csv    dbfs:/FileStore/ecommerce/data/orders.csv
```

### Option C — Notebook upload widget

In a Databricks notebook, use `dbutils.fs.cp` from a workspace path after uploading files to the notebook's file area, or generate CSVs directly in the notebook by running the generator if the repo is attached.

### Option D — Custom path (local Spark / alternate volume)

Override the source path when ingesting:

```bash
python src/bronze/ingest_all.py --source-base-path /path/to/data --schema ecommerce
```

Or set the environment variable:

```bash
export PIPELINE_SOURCE_BASE_PATH=/path/to/data
```

Use any URI Spark can read (`dbfs:`, `file:`, cloud storage paths).

---

## Bronze Ingest Order

Ingest parent dimensions before facts:

```
customers.csv  →  bronze_customers
products.csv   →  bronze_products
orders.csv     →  bronze_orders
```

`ingest_all.py` runs this order automatically. Bronze does not enforce referential integrity — order is a logical convention.

---

## What Happens After Load

| Layer | Effect of seed data |
|-------|----------------------|
| Bronze | All 10,000 + 500 + 100,000 rows land as STRING + metadata |
| Silver | Same row counts; ~430+ rows flagged `dq_is_valid = false` from intentional issues |
| Gold | Revenue KPIs use **Completed** orders with `dq_is_valid = true` only (~70% of natural orders minus invalid rows) |

Invalid and duplicate rows remain in Silver for audit — they are never silently deleted.

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `src/data_generation/DATA_GENERATION_NOTES.md` | Generator internals and validation detail |
| `database/setup-notes.md` | Full environment setup and pipeline execution |
| `database/schema.sql` | Table definitions and dependencies |
| `data-quality-strategy.md` | DQ rule catalog |
| `data-model.md` | Logical data model (may lag code in minor areas) |
