# AI Prompts — Dashboard

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

---

## Prompt 1 — Dashboard SQL Queries (Interrupted)

**Timestamp:** 1:17 PM

### PROMPT SENT

```
Create:

src/dashboard/dashboard_queries.sql

The dashboard must contain at least:

1. Top 10 products by revenue
2. Customer revenue distribution
3. Customer segmentation

Create clean Databricks SQL queries for each visualization.

Also create useful filters where appropriate, such as:
- date
- category
- customer segment
- order status if applicable

For every query:
- explain the purpose
- identify the Gold table used
- identify dimensions
- identify measures
- identify recommended visualization type

Keep queries simple and dashboard-friendly.
```

### AI RESPONSE SUMMARY

**No assistant completion captured** (interrupted).

### MY EVALUATION

#### Accepted / Changed / Rejected

- N/A — no deliverable from this turn.

### VALIDATION

N/A

---

## Prompt 2 — Dashboard SQL Queries (Resend — Completed)

**Timestamp:** 1:17 PM

### PROMPT SENT

*(Identical to Prompt 1.)*

### AI RESPONSE SUMMARY

Created `src/dashboard/dashboard_queries.sql` with three required visualizations plus parameter/filter comments. All queries read **Gold tables only** (no Silver/Bronze). Each query block documents purpose, source table, dimensions, measures, and chart type (bar, histogram, pie).

### MY EVALUATION

#### Accepted

- Gold-only reads (design D1/D2).
- Comment metadata per visualization for manual Databricks setup.
- Filter parameters documented (date, category, segment).

#### Changed

- N/A (first successful completion on resend).

#### Rejected

- None recorded.

#### Reasoning

- Dashboard is presentation layer; SQL must not re-derive business rules from Silver.

### VALIDATION

Manual: run queries in Databricks SQL after Gold build; compare to Gold table aggregates. E2E review flagged `DASHBOARD_GUIDE.md` §6.5 Silver reconciliation query as incorrect (doc fix deferred).

---

## Prompt 3 — Dashboard Setup Guide

**Timestamp:** 1:18 PM

### PROMPT SENT

```
Create:

src/dashboard/DASHBOARD_GUIDE.md

Provide step-by-step instructions for creating the Databricks SQL dashboard.

Include:

1. Required Gold tables
2. Query setup
3. Visualization configuration
4. Chart type
5. X-axis
6. Y-axis
7. Filters
8. Suggested titles
9. Expected interpretation

Required visuals:

- Top 10 Products by Revenue → Bar Chart
- Customer Revenue Distribution → Histogram
- Customer Segmentation → Pie Chart

Also explain how to verify that the dashboard is showing correct numbers.
```

### AI RESPONSE SUMMARY

Created `DASHBOARD_GUIDE.md` aligned with `dashboard_queries.sql`: step-by-step Databricks SQL Dashboard UI instructions, chart bindings, filters, titles, and validation queries to compare dashboard numbers to Gold tables.

### MY EVALUATION

#### Accepted

- Manual setup documented honestly (no false claim of automated dashboard deploy).
- Verification section for number checking.
- Chart types match assignment.

#### Changed

- None recorded post-delivery.

#### Rejected

- None recorded.

#### Reasoning

- Community Edition has no API deploy in repo; guide is the correct deliverable.

### VALIDATION

README Dashboard section points to guide; human follows steps on Databricks cluster with Gold tables populated.

---

## Context Note

Dashboard prompts followed Gold orchestrator (Prompt 5 in `gold-layer.md`). The session did **not** include a prompt to auto-create the Databricks dashboard artifact — only SQL + guide.
