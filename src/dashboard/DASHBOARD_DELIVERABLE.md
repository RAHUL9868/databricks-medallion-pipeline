# E-Commerce Gold Analytics — Dashboard Deliverable

**Project:** E-commerce Medallion Pipeline  
**Schema:** `ecommerce`  
**Deliverable date:** 2026-09-07  
**Status:** Published

---

## Published dashboard

**Title:** E-Commerce Gold Analytics (Databricks Lakeview / SQL Dashboard)

**Published URL:**

https://dbc-6a498a86-07f2.cloud.databricks.com/dashboardsv3/01f1aab7db5713f8a43e09512edcaa70/published?o=7474655649363572

**Workspace:** `dbc-6a498a86-07f2.cloud.databricks.com`  
**Dashboard ID:** `01f1aab7db5713f8a43e09512edcaa70`

---

## Required visualizations

| # | Visualization | Chart type | Gold table | Query source |
|---|---------------|------------|------------|--------------|
| 1 | Top 10 Products by Revenue | Bar | `gold_sales_by_product` | `dashboard_queries.sql` — Visualization 1 |
| 2 | Customer Revenue Distribution | Histogram / Bar | `gold_revenue_by_customer` | `dashboard_queries.sql` — Visualization 2 |
| 3 | Customer Segmentation by Behavior | Pie | `gold_customer_segmentation` | `dashboard_queries.sql` — Visualization 3 |

---

## Data rules (Gold layer)

- Revenue includes **Completed** orders only (design D1).
- Only **valid** Silver rows (`dq_is_valid = true`) feed Gold (design D2).
- Duplicate `order_id` rows are excluded via Silver quality rules and defensive Gold deduplication.
- Dashboard queries read **Gold tables only** — no Bronze or Silver logic in the UI layer.

---

## Repository artifacts

| Artifact | Path |
|----------|------|
| Dashboard SQL queries | `src/dashboard/dashboard_queries.sql` |
| Setup and validation guide | `src/dashboard/DASHBOARD_GUIDE.md` |
| Notebook preview (optional) | `notebooks/databricks_dashboard_preview.ipynb` |
| PDF deliverable | `src/dashboard/E-Commerce-Gold-Analytics-Dashboard.pdf` |

---

## Refresh workflow

When pipeline data changes:

1. Re-run `notebooks/databricks_serverless_pipeline.ipynb` (or `run_pipeline.py`).
2. Refresh the published dashboard in Databricks — linked queries re-run against current Gold tables.

---

## Verification

Gold tables should be populated before viewing the dashboard:

```sql
USE ecommerce;

SELECT 'gold_sales_by_product' AS table_name, COUNT(*) AS rows FROM gold_sales_by_product
UNION ALL SELECT 'gold_revenue_by_customer', COUNT(*) FROM gold_revenue_by_customer
UNION ALL SELECT 'gold_customer_segmentation', COUNT(*) FROM gold_customer_segmentation;
```

Segmentation `SUM(customer_count)` should equal `COUNT(*)` on `gold_revenue_by_customer`.
