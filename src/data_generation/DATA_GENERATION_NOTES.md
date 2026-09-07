# Data Generation Notes

This document describes how `generate_sample_data.py` builds deterministic sample CSV files for the e-commerce Medallion pipeline assessment.

---

## Purpose

The generator creates three CSV files under a configurable output directory (default: `data/`):

| File | Approximate Rows |
|------|------------------|
| `customers.csv` | 10,000 |
| `products.csv` | 500 |
| `orders.csv` | 100,000 |

It runs with **standard Python only** (no Databricks dependency). A fixed random seed (`42` by default) ensures reproducible output.

---

## Volume Breakdown

### Customers (10,000 rows)

| Component | Count |
|-----------|-------|
| Naturally generated unique customers | 9,990 |
| Intentionally appended duplicate rows | 10 |
| **Total** | **10,000** |

### Products (500 rows)

| Component | Count |
|-----------|-------|
| Naturally generated unique products | 500 |
| Intentional corruptions | 0 |
| **Total** | **500** |

### Orders (100,000 rows)

| Component | Count |
|-----------|-------|
| Naturally generated unique orders | 99,980 |
| Intentionally appended duplicate rows | 20 |
| **Total** | **100,000** |

---

## Natural Data Generation

### Customers

- `customer_id`: sequential integers `1` … `9,990`, plus 10 duplicate rows (see corruptions).
- `customer_name`: random `first + last` from fixed name pools.
- `email`: `{first}.{last}{customer_id}@mail.example.com` (lowercase).
- `country`: random from 18 countries.
- `signup_date`: random date between `2018-01-01` and `2025-06-30`.
- `customer_segment`: weighted random — Premium (20%), Standard (50%), Basic (30%).
- `lifetime_value`: segment-based realistic decimal, formatted to 2 places.

### Products

- `product_id`: sequential `1` … `500`.
- `product_name`: unique combination of adjective + noun (deterministic fallback if collision).
- `category`: rotated across 8 categories.
- `price`: random `5.00` – `500.00`.
- `cost`: `55%`–`85%` of price (always `cost <= price` for natural rows).
- `stock_quantity`: `0` – `2,000`.
- `reorder_level`: `10%`–`35%` of stock (minimum 10).

### Orders

- `order_id`: sequential `1` … `99,980`, plus 20 duplicate rows.
- `customer_id` / `product_id`: random valid references from generated customers/products.
- `order_date`: random between `2019-01-01` and `2025-08-31`, not before customer signup.
- `quantity`: random `1` – `5`.
- `unit_price`: product list price ± 5%.
- `total_amount`: **`quantity × unit_price`**, rounded half-up to 2 decimals (always consistent).
- `order_status`: Pending (20%), Completed (70%), Cancelled (10%).
- `payment_date`: populated for **Completed** orders (`order_date` to `order_date + 14 days`); `NULL` for Pending/Cancelled.

---

## Intentional Corruptions

Corruptions are applied **after** natural generation on **disjoint index sets** so one row does not receive multiple intentional corruption types.

### Customers

| Issue | Target Count | Method |
|-------|--------------|--------|
| NULL `email` | **50 rows** | Set `email = NULL` on 50 randomly selected unique customer indices |
| Duplicate `customer_id` | **10 rows** | Copy 10 randomly selected customer rows verbatim and append (same `customer_id`) |

**Disjoint sets:** NULL-email indices and duplicate-source indices do not overlap.

**Effect on uniqueness metrics:**

- 10 `customer_id` values each appear exactly **twice**
- **20 rows** participate in duplicate groups (per Silver uniqueness rule)
- **10 rows** are the appended duplicate records

### Orders

| Issue | Target Count | Method |
|-------|--------------|--------|
| NULL `customer_id` | **100 rows** | Set `customer_id = NULL` |
| NULL `product_id` | **200 rows** | Set `product_id = NULL` |
| Invalid `customer_id` | **50 rows** | Set to `9000001` … `9000050` (not in customers) |
| Invalid `product_id` | **30 rows** | Set to `9000001` … `9000030` (not in products) |
| Duplicate `order_id` | **20 rows** | Copy 20 randomly selected order rows verbatim and append (same `order_id`) |

**Disjoint sets:** All five corruption pools are mutually exclusive on the base 99,980 unique orders.

**Invalid FK IDs** use the `9,000,001+` range to avoid collision with valid IDs (`1`–`9,990` customers, `1`–`500` products).

**Effect on uniqueness metrics:**

- 20 `order_id` values each appear exactly **twice**
- **40 rows** participate in duplicate groups
- **20 rows** are the appended duplicate records

### Products

No intentional corruptions. All 500 `product_id` values are unique.

---

## Corruption vs Natural Records

| Aspect | Natural Records | Corrupted Records |
|--------|-----------------|-------------------|
| Generation | Built by loop with valid defaults | Selected indices modified or copied |
| FK validity | Valid customer/product references | NULL or out-of-range IDs (orders only) |
| PK uniqueness | Unique before duplicate append | Duplicate rows share PK with source row |
| `total_amount` | Always `quantity × unit_price` | Unchanged (duplicates copy exact values) |
| Traceability | N/A | Disjoint index selection logged via internal audit (not in CSV) |

The CSV files contain **no corruption metadata columns** — only business fields. Issue counts are validated in code before write.

---

## Pre-Write Validation (Fail Fast)

`generate_sample_data.py` validates **before** writing CSVs and exits with an error if any check fails:

### Customers

- Row count = 10,000
- NULL `email` count = 50
- Duplicate extra rows = 10
- Duplicated `customer_id` key count = 10
- Rows in duplicate groups = 20
- No unintended PK duplication beyond the 10 intentional duplicates

### Products

- Row count = 500
- All `product_id` values unique
- No negative price, cost, stock, or reorder level

### Orders

- Row count = 100,000
- NULL `customer_id` = 100
- NULL `product_id` = 200
- Invalid `customer_id` (non-null, not in customers) = 50
- Invalid `product_id` (non-null, not in products) = 30
- Duplicate extra rows = 20
- Rows in duplicate `order_id` groups = 40
- Corruption index sets are disjoint (400 unique base rows corrupted + 20 duplicate sources)
- All non-corrupted base orders have valid FKs
- `total_amount = quantity × unit_price` for every row with numeric fields
- `payment_date >= order_date` when payment is present
- Completed natural orders have `payment_date`

---

## Usage

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
| `--output-dir` | `<repo>/data` | Output directory for CSV files |
| `--seed` | `42` | Random seed |
| `--log-level` | `INFO` | Logging verbosity |

---

## Validation: Verify Generated Files

After running the generator, use the checks below to confirm output matches assignment expectations.

### 1. Row counts

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

**Expected:**

```
customers.csv: 10000 rows
products.csv: 500 rows
orders.csv: 100000 rows
```

### 2. Customer intentional issues

```bash
python -c "
import csv
from collections import Counter
from pathlib import Path

with (Path('data')/'customers.csv').open() as f:
    rows = list(csv.DictReader(f))

null_email = sum(1 for r in rows if not (r.get('email') or '').strip())
ids = [int(r['customer_id']) for r in rows if r['customer_id'].strip()]
counts = Counter(ids)
dup_keys = sum(1 for c in counts.values() if c > 1)
dup_rows = sum(c for c in counts.values() if c > 1)

print('NULL email:', null_email)
print('Duplicated customer_id keys:', dup_keys)
print('Rows in duplicate groups:', dup_rows)
"
```

**Expected:**

```
NULL email: 50
Duplicated customer_id keys: 10
Rows in duplicate groups: 20
```

### 3. Order intentional issues

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

null_cust = sum(1 for r in rows if blank(r['customer_id']))
null_prod = sum(1 for r in rows if blank(r['product_id']))
invalid_cust = sum(1 for r in rows if not blank(r['customer_id']) and int(r['customer_id']) not in valid_customers)
invalid_prod = sum(1 for r in rows if not blank(r['product_id']) and int(r['product_id']) not in valid_products)
order_ids = [int(r['order_id']) for r in rows if r['order_id'].strip()]
oc = Counter(order_ids)
dup_rows = sum(c for c in oc.values() if c > 1)

print('NULL customer_id:', null_cust)
print('NULL product_id:', null_prod)
print('Invalid customer_id:', invalid_cust)
print('Invalid product_id:', invalid_prod)
print('Rows in duplicate order_id groups:', dup_rows)
"
```

**Expected:**

```
NULL customer_id: 100
NULL product_id: 200
Invalid customer_id: 50
Invalid product_id: 30
Rows in duplicate order_id groups: 40
```

### 4. total_amount consistency

```bash
python -c "
import csv
from decimal import Decimal
from pathlib import Path

with (Path('data')/'orders.csv').open() as f:
    bad = 0
    for r in csv.DictReader(f):
        q, u, t = r['quantity'], r['unit_price'], r['total_amount']
        if not q or not u or not t:
            continue
        expected = (Decimal(u) * int(q)).quantize(Decimal('0.01'))
        if Decimal(t) != expected:
            bad += 1
    print('total_amount mismatches:', bad)
"
```

**Expected:** `total_amount mismatches: 0`

### 5. Reproducibility

Run the generator twice with the same seed and compare file hashes:

```bash
python src/data_generation/generate_sample_data.py --seed 42 --output-dir data_run1
python src/data_generation/generate_sample_data.py --seed 42 --output-dir data_run2
# Linux/macOS:
diff data_run1/customers.csv data_run2/customers.csv
diff data_run1/orders.csv data_run2/orders.csv
diff data_run1/products.csv data_run2/products.csv
```

On Windows PowerShell:

```powershell
fc data_run1\customers.csv data_run2\customers.csv
```

Files should be identical.

---

## Alignment with Silver DQ Metrics

When the pipeline runs on this data, `silver_dq_metrics` should report approximately:

| Rule ID | Expected `failed_rows` |
|---------|------------------------|
| `customers_email_not_null` | 50 |
| `customers_customer_id_unique` | 20 |
| `orders_customer_id_not_null` | 100 |
| `orders_product_id_not_null` | 200 |
| `orders_customer_id_exists` | 50 |
| `orders_product_id_exists` | 30 |
| `orders_order_id_unique` | 40 |

See `data-quality-strategy.md` for full rule definitions.

---

## Related Documents

- `requirements-analysis.md` — Assignment requirements
- `data-quality-strategy.md` — DQ rules and expected failure counts
- `data-model.md` — Column definitions and table schemas
