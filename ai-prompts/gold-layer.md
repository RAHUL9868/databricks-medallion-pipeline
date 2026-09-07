# AI Prompts — Gold Layer

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

---

## Prompt 1 — Sales by Product SQL

**Timestamp:** 1:04 PM

### PROMPT SENT

```
Implement:

src/gold/01_sales_by_product.sql

Create the Sales by Product Gold aggregation.

Required columns:

- product_id
- product_name
- category
- total_orders
- total_revenue
- avg_order_value

Define clearly which order statuses are included in revenue calculations.

Do not count invalid Silver records as valid business transactions unless the design explicitly justifies it.

Use appropriate decimal precision.

Handle:
- products with no orders
- cancelled orders
- pending orders
- invalid orders
- NULL values

Avoid double counting caused by duplicate order IDs.

Document the business calculation for every metric.

Before writing SQL, explain the aggregation logic.
```

### AI RESPONSE SUMMARY

Agent explained aggregation logic in chat, then created `01_sales_by_product.sql`. **Business rules:** only `dq_is_valid = true` Silver orders; revenue from **`Completed`** status only; dedupe by `order_id` where applicable. SQL comments document each metric.

### MY EVALUATION

#### Accepted

- Completed-only revenue (design D1/D2).
- Valid Silver rows only.
- Pre-SQL business rule explanation.

#### Changed

- None recorded post-delivery.

#### Rejected

- None recorded.

#### Reasoning

- Matches `design-notes.md` Gold eligibility rules.

### VALIDATION

SQL review; runtime reconciliation in `create_gold_tables.py` (Prompt 5). Limited SQL integration in `tests/test_gold.py`.

---

## Prompt 2 — Revenue by Customer SQL

**Timestamp:** 1:05 PM

### PROMPT SENT

```
Implement:

src/gold/02_revenue_by_customer.sql

Create:

- customer_id
- customer_name
- customer_segment
- total_orders
- total_revenue
- avg_order_value
- lifetime_value_actual

Define:

- what qualifies as an order
- which statuses contribute revenue
- how duplicate orders are handled
- how invalid orders are handled
- how customers with no valid orders are treated

Ensure:
- no duplicate customer rows
- revenue calculations are correct
- average order value uses the correct denominator
- decimal calculations are accurate

Use Silver data as the source.

Explain every business rule before implementation.
```

### AI RESPONSE SUMMARY

Documented business rules in chat, implemented `02_revenue_by_customer.sql` with one row per customer, `lifetime_value_actual` from summed valid Completed order revenue, same eligibility filters as product Gold.

### MY EVALUATION

#### Accepted

- One row per `customer_id`.
- Consistent order qualification with `01_sales_by_product.sql`.

#### Changed

- None recorded.

#### Rejected

- None recorded.

### VALIDATION

Reconciliation checks in `create_gold_tables.py`; E2E review noted cross-Gold revenue totals may differ by design (product vs customer grain).

---

## Prompt 3 — Daily / Weekly Trends SQL

**Timestamp:** 1:13 PM

### PROMPT SENT

```
Implement:

src/gold/03_daily_weekly_trends.sql

Create useful analytical aggregations for:

- daily revenue
- daily order count
- weekly revenue
- weekly order count

Use order_date.

Clearly define:
- valid orders
- revenue-producing orders
- treatment of cancelled/pending orders
- handling of invalid records

Ensure dates are handled correctly.

Use SQL that is easy to understand and compatible with Databricks SQL.

Add comments explaining business logic.
```

### AI RESPONSE SUMMARY

Created `03_daily_weekly_trends.sql` with daily and weekly grains (union or separate CTEs), same valid/Completed order rules, `order_date` for bucketing.

### MY EVALUATION

#### Accepted

- Fourth Gold dataset beyond assignment minimum (useful for dashboard date filters).
- Documented date and status handling.

#### Changed

- None recorded.

#### Rejected

- None recorded.

### VALIDATION

Manual SQL review; orchestrator runs script as step in `create_gold_tables.py`.

---

## Prompt 4 — Customer Segmentation SQL

**Timestamp:** 1:14 PM

### PROMPT SENT

```
Implement:

src/gold/04_customer_segmentation.sql

Create customer segmentation with:

segment_type:
- High-Value
- Repeat
- One-Time
- Inactive

and:

- customer_count
- avg_revenue
- total_revenue

IMPORTANT:

The assignment specifies these segment labels but does not explicitly define the thresholds.

Do not silently invent thresholds.

First propose a segmentation rule using:
- revenue
- order count
- activity

Clearly document the proposed thresholds and assumptions.

Then implement the rule.

Ensure each customer maps to exactly one segment.

Define precedence if a customer could meet multiple segment criteria.

Include customers with no valid orders as Inactive where appropriate.

Validate that:
- total customer counts reconcile
- segments do not overlap
- all customers are assigned exactly once
```

### AI RESPONSE SUMMARY

Agent **proposed thresholds in chat** (revenue + order count + recency), documented precedence (e.g. High-Value before Repeat), implemented mutually exclusive segments in SQL. Uses `CURRENT_DATE()` for activity window (later flagged in E2E review).

### MY EVALUATION

#### Accepted

- Explicit threshold proposal before SQL (as required).
- Behavioral labels per assignment.
- Precedence rules for non-overlapping segments.

#### Changed

- Thresholds are **documented engineering assumptions** (assignment did not define them).

#### Rejected

- None recorded in transcript.

#### Reasoning

- User explicitly required proposed thresholds, not silent invention.

### VALIDATION

Customer count reconciliation in `create_gold_tables.py`; E2E review noted segmentation **revenue** not fully reconciled in orchestrator.

---

## Prompt 5 — Gold Orchestrator

**Timestamp:** 1:15 PM

### PROMPT SENT

```
Create:

src/gold/create_gold_tables.py

This should orchestrate creation of all required Gold datasets.

Required:
1. sales_by_product
2. revenue_by_customer
3. daily_weekly_trends
4. customer_segmentation

Requirements:

- Read from Silver.
- Create Delta Gold tables.
- Safe reruns.
- Consistent naming.
- Logging.
- Row-count validation.
- Basic reconciliation checks.

Add validations such as:

- Gold revenue should reconcile with valid Silver order revenue.
- Customer segmentation counts should reconcile with customer population.
- Product aggregation should not duplicate product IDs.
- Customer aggregation should not duplicate customer IDs.

If a reconciliation fails, fail the pipeline with a useful error message.
```

### AI RESPONSE SUMMARY

Implemented `create_gold_tables.py` executing four SQL files in order, extended `pipeline_config.py`, added `GoldPipelineError` and reconciliation helpers (Silver valid Completed revenue vs Gold totals, segmentation customer counts, duplicate key checks).

### MY EVALUATION

#### Accepted

- Fail-fast reconciliation with clear errors.
- Orchestrated SQL execution from Python.
- Logging and row-count validation.

#### Changed

- None recorded post-delivery.

#### Rejected

- None recorded.

#### Reasoning

- Runtime reconciliation compensates for thin Gold pytest coverage.

### VALIDATION

Run `python src/gold/create_gold_tables.py` after Silver build; `tests/test_gold.py` for orchestration helpers. E2E review: product dedup in SQL still a known gap (HIGH).
