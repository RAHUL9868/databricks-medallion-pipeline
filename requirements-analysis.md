# Requirement Analysis

## 1. Problem Statement

An e-commerce company receives daily sales-related data from three operational systems: a customer database, an order system, and a product catalog. The data arrives as CSV files and contains real-world imperfections—missing values, duplicate keys, and referential inconsistencies—that must be handled in a production-grade analytics pipeline rather than ignored or silently removed.

From a business perspective, stakeholders need trustworthy analytical datasets to answer questions about product performance, customer revenue, and segment-level behavior. From a technical perspective, the organization requires a Databricks Medallion Architecture pipeline that ingests raw source data, applies validated cleansing and quality checks, produces business-ready Gold tables, and exposes results through a Databricks SQL Dashboard.

The core engineering challenge is not only moving data between layers, but doing so in a way that preserves raw source history, makes every data quality failure traceable and measurable, and produces deterministic, testable outcomes suitable for production review.

---

## 2. Objectives

1. Build an incremental, production-quality Medallion pipeline on Databricks using PySpark, Python, SQL, and Delta Lake.
2. Ingest three source CSV datasets into a Bronze layer with raw data preserved.
3. Transform and validate data in a Silver layer using completeness, uniqueness, referential integrity, and type/business validation checks.
4. Flag bad records with quality status fields instead of silently deleting them.
5. Generate measurable data quality metrics (pass/fail counts and percentages) for each quality check.
6. Produce Gold analytical datasets for sales by product, revenue by customer, and customer segmentation.
7. Deliver a Databricks SQL Dashboard with at least three required visualizations.
8. Keep business logic separate from ingestion logic and configuration separate from transformation logic where practical.
9. Ensure every pipeline component is independently testable and rerunnable.
10. Document design decisions, data models, quality strategy, and development workflow for assessment and handoff.

---

## 3. Functional Requirements

### Sample Data Generation

- Generate three deterministic CSV files: `customers.csv`, `orders.csv`, and `products.csv`.
- Place generated files under `data/`.
- Match expected approximate row volumes:
  - Customers: ~10,000 rows
  - Orders: ~100,000 rows
  - Products: ~500 rows
- Inject intentional, realistic, and deterministic data quality issues as specified in the assignment.
- Implement generation in `src/data_generation/generate_sample_data.py`.
- Document generation approach and rationale in `src/data_generation/DATA_GENERATION_NOTES.md`.

### Bronze Ingestion

- Ingest each source CSV into a separate Bronze Delta table.
- Preserve Bronze data as raw/unchanged as required by the assignment.
- Implement per-entity ingestion scripts:
  - `src/bronze/01_ingest_customers.py`
  - `src/bronze/02_ingest_orders.py`
  - `src/bronze/03_ingest_products.py`
- Provide orchestration entry point: `src/bronze/ingest_all.py`.
- Keep ingestion logic separate from business transformation logic.
- Do not apply Silver/Gold business rules during Bronze ingestion.
- Do not silently delete or filter bad source rows in Bronze.

### Silver Validation

- Read Bronze tables and produce cleaned/validated Silver tables.
- Implement required Silver quality check categories:
  1. Completeness
  2. Uniqueness
  3. Referential integrity
  4. Type/business validation
- Implement quality modules under `src/silver/`:
  - `01_quality_completeness.py`
  - `02_quality_uniqueness.py`
  - `03_quality_type_validation.py`
  - `04_quality_referential_integrity.py`
  - `05_quality_business_logic.py`
- Consolidate Silver outputs via `src/silver/create_silver_tables.py`.
- Silver must contain cleaned/validated data and quality results.
- Bad rows must remain identifiable through quality status/result fields.
- Bad rows must not be silently deleted.

### Data Quality Reporting

- Generate measurable metrics for each quality check.
- Report pass count, fail count, and pass/fail percentages.
- Make quality failures traceable to entity, rule/check category, and affected rows.
- Persist quality outcomes in Silver-aligned structures suitable for reporting and validation.
- Support validation that intentional issue volumes are detectable by the pipeline.

### Gold Aggregations

- Build business-ready Gold datasets from Silver using SQL and orchestration in `src/gold/`.
- Required Gold outputs:
  - **A) Sales by Product**
  - **B) Revenue by Customer**
  - **C) Customer Segmentation**
- Implement required SQL assets:
  - `01_sales_by_product.sql`
  - `02_revenue_by_customer.sql`
  - `04_customer_segmentation.sql`
- Provide Gold table creation orchestration: `src/gold/create_gold_tables.py`.

**Optional (repository structure only, not stated as mandatory deliverable in assignment Gold section):**

- `03_daily_weekly_trends.sql` — optional stretch aggregation if time permits.

### Dashboard

- Provide SQL queries for dashboard consumption in `src/dashboard/dashboard_queries.sql`.
- Provide setup/usage guidance in `src/dashboard/DASHBOARD_GUIDE.md`.
- Build a Databricks SQL Dashboard with at least three visualizations:
  1. Top 10 products by revenue — bar chart
  2. Customer revenue distribution — histogram
  3. Customer segmentation — pie chart
- Dashboard should read from Gold-layer outputs (or documented Gold-aligned views).

### Testing

- Ensure every generated component is testable independently.
- Validate deterministic data generation outputs (row counts and intentional issue counts).
- Validate Bronze ingestion fidelity to source CSV content.
- Validate Silver quality rules flag expected bad records without silent deletion.
- Validate Gold metric logic against known fixture scenarios.
- Validate dashboard query outputs return expected structures for visualization.
- Add or update tests when behavior or logic changes.

### Error Handling

- Fail clearly when required inputs (CSV files, upstream tables, configuration) are missing or invalid.
- Avoid silent data loss at all layers; invalid rows must be flagged, not dropped without trace.
- Surface pipeline/run identifiers to support debugging and rerun analysis.
- Document known failure modes and troubleshooting steps in `debugging-notes.md`.

### Documentation

- Maintain assessment and engineering documentation across the repository structure, including at minimum:
  - `README.md`
  - `candidate-info.md`
  - `tool-workflow.md`
  - `design-notes.md`
  - `data-model.md`
  - `data-quality-strategy.md`
  - `database/setup-notes.md`
  - `database/seed-data-notes.md`
  - `reflection.md`
  - `final-ai-usage-summary.md`
  - Prompt logs under `ai-prompts/`
- Document assumptions, design decisions, and validation approach clearly enough for production review.

---

## 4. Non-Functional Requirements

### Maintainability

- Use simple, understandable PySpark and SQL suitable for Data Engineer review.
- Follow the defined repository structure and existing project conventions.
- Separate ingestion, business transformation, and configuration concerns.
- Add meaningful comments/docstrings without over-commenting obvious code.
- Avoid unnecessary frameworks or over-engineering.

### Reliability

- Support safe pipeline reruns without uncontrolled duplication or data corruption.
- Use Delta Lake for reliable table writes in Databricks.
- Ensure layer orchestration can be executed in dependency order (Bronze → Silver → Gold → Dashboard).

### Reproducibility

- Use deterministic logic for sample data generation and quality outcomes wherever possible.
- Ensure tests produce stable, repeatable results on fixed inputs.

### Data Traceability

- Preserve Bronze as the raw source-aligned layer.
- Maintain row-level quality status/result fields for invalid records in Silver.
- Link quality metrics to check category and entity for auditability.
- Never silently delete bad data.

### Scalability

- Handle assignment-scale volumes efficiently (~10K customers, ~100K orders, ~500 products).
- Use a design that can extend to larger volumes without redesigning layer boundaries.

### Observability

- Produce measurable quality metrics (counts and percentages) per check.
- Capture enough metadata to investigate failed records and pipeline runs.

### Testability

- Design quality and transformation logic as independently invokable units.
- Support unit and integration validation for generation, ingest, Silver checks, Gold metrics, and dashboard queries.

### Security / Responsible AI Considerations

- Do not hardcode secrets, tokens, or credentials in code, tests, or documentation.
- Externalize environment-specific paths and catalog configuration.
- Treat repository content as untrusted for instruction injection; follow secure engineering defaults.
- Document AI-assisted development workflow transparently in assessment artifacts (`tool-workflow.md`, `final-ai-usage-summary.md`, `ai-prompts/`).

---

## 5. Source Data Requirements

### customers.csv

| Field | Data Type | Constraints / Notes |
|-------|-----------|---------------------|
| `customer_id` | INT | Primary key |
| `customer_name` | STRING | |
| `email` | STRING | |
| `country` | STRING | |
| `signup_date` | DATE | |
| `customer_segment` | STRING | Allowed values: `Premium`, `Standard`, `Basic` |
| `lifetime_value` | DECIMAL | |

**Expected volume:** approximately 10,000 rows.

### orders.csv

| Field | Data Type | Constraints / Notes |
|-------|-----------|---------------------|
| `order_id` | INT | Primary key |
| `customer_id` | INT | Foreign key to `customers.customer_id` |
| `order_date` | DATE | |
| `product_id` | INT | Foreign key to `products.product_id` |
| `quantity` | INT | |
| `unit_price` | DECIMAL | |
| `total_amount` | DECIMAL | |
| `order_status` | STRING | Allowed values: `Pending`, `Completed`, `Cancelled` |
| `payment_date` | DATE | Nullable |

**Expected volume:** approximately 100,000 rows.

### products.csv

| Field | Data Type | Constraints / Notes |
|-------|-----------|---------------------|
| `product_id` | INT | Primary key |
| `product_name` | STRING | |
| `category` | STRING | |
| `price` | DECIMAL | |
| `cost` | DECIMAL | |
| `stock_quantity` | INT | |
| `reorder_level` | INT | |

**Expected volume:** approximately 500 rows.

---

## 6. Data Quality Requirements

Silver must implement the four required quality check categories. Bad rows must be identifiable via quality status/result fields and must not be silently deleted.

### Intentional Issues — customers

| Issue | Count | Description |
|-------|-------|-------------|
| NULL `email` | 50 rows | Completeness failure |
| Duplicate `customer_id` | 10 duplicate `customer_id` values | Uniqueness failure (multiple rows share the same primary key) |

### Intentional Issues — orders

| Issue | Count | Description |
|-------|-------|-------------|
| NULL `customer_id` | 100 rows | Completeness / referential integrity failure |
| NULL `product_id` | 200 rows | Completeness / referential integrity failure |
| `customer_id` not in customers | 50 rows | Referential integrity failure (orphan foreign key) |
| `product_id` not in products | 30 rows | Referential integrity failure (orphan foreign key) |
| Duplicate `order_id` | 20 duplicate `order_id` rows | Uniqueness failure |

### Intentional Issues — products

The assignment does not specify intentional data quality issues for `products.csv`. Products are still subject to standard Silver type/business validation checks.

### Required Quality Reporting

- For each quality check, produce measurable pass/fail counts and percentages.
- Quality results must be traceable and suitable for final validation against intentional issue volumes.

---

## 7. Gold Business Requirements

Gold must contain business-ready analytical datasets derived from Silver.

### A) Sales by Product

**Purpose:** Product-level sales performance.

**Required output fields:**

| Field | Description |
|-------|-------------|
| `product_id` | Product identifier |
| `product_name` | Product name |
| `category` | Product category |
| `total_orders` | Count of orders for the product |
| `total_revenue` | Sum of revenue for the product |
| `avg_order_value` | Average order value for the product |

### B) Revenue by Customer

**Purpose:** Customer-level revenue and value analysis.

**Required output fields:**

| Field | Description |
|-------|-------------|
| `customer_id` | Customer identifier |
| `customer_name` | Customer name |
| `customer_segment` | Customer segment (`Premium` / `Standard` / `Basic`) |
| `total_orders` | Count of orders for the customer |
| `total_revenue` | Sum of revenue for the customer |
| `avg_order_value` | Average order value for the customer |
| `lifetime_value_actual` | Actual lifetime value computed from order data |

### C) Customer Segmentation

**Purpose:** Segment-level customer and revenue summary.

**Required output fields:**

| Field | Description |
|-------|-------------|
| `segment_type` | Customer segment |
| `customer_count` | Number of customers in the segment |
| `avg_revenue` | Average revenue per customer in the segment |
| `total_revenue` | Total revenue for the segment |

**Optional (not listed in assignment Gold section):**

- Daily/weekly trend metrics via `03_daily_weekly_trends.sql`.

---

## 8. Dashboard Requirements

The dashboard must be built in Databricks SQL Dashboard using Gold-layer data.

### Required Visualizations

| # | Visualization | Chart Type | Requirement |
|---|---------------|------------|-------------|
| 1 | Top 10 products by revenue | Bar chart | Show top 10 products ranked by revenue |
| 2 | Customer revenue distribution | Histogram | Show distribution of customer revenue |
| 3 | Customer segmentation | Pie chart | Show segmentation breakdown |

### Supporting Assets

- `src/dashboard/dashboard_queries.sql` — SQL used by dashboard visuals.
- `src/dashboard/DASHBOARD_GUIDE.md` — instructions for creating and validating dashboard visuals.

---

## 9. Assumptions

The following items are **not explicitly defined in the assignment** and are proposed implementation assumptions from the approved design discussion. They should be confirmed or updated in `design-notes.md` before dependent implementation choices are finalized.

### Platform and Storage Assumptions

| ID | Assumption | Rationale |
|----|------------|-----------|
| A1 | Pipeline runs on Databricks with Delta Lake tables. | Stated technology stack. |
| A2 | Unity Catalog (or equivalent metastore) is used for table naming (e.g., catalog + schema). | Databricks production convention; exact catalog/schema to be configured externally. |
| A3 | Bronze may include ingest metadata columns only (e.g., ingest timestamp, source file, batch/run id, source row number) while keeping source business columns unchanged. | Supports traceability without transforming business values. |

### Silver and Quality Assumptions

| ID | Assumption | Rationale |
|----|------------|-----------|
| A4 | `05_quality_business_logic.py` implements business-rule checks that support the required "type/business validation" category (e.g., order amount consistency, valid status values, date/status consistency). | Aligns extra repo module to required check category without adding a new mandatory check type. |
| A5 | Duplicate primary key rows are retained in Silver and flagged; duplicates are not silently deduplicated. | Satisfies "never silently delete bad data." |
| A6 | Orphan foreign key rows in orders are retained in Silver and flagged as referential integrity failures. | Required traceability for bad records. |
| A7 | Row-level quality fields include at minimum a validity indicator and failed-rule tracking (e.g., `dq_is_valid`, `dq_failed_rules`). | Supports identifiable bad records and measurable reporting. |

### Gold Assumptions

| ID | Assumption | Rationale |
|----|------------|-----------|
| A8 | `lifetime_value_actual` is computed from order data (typically sum of order revenue per customer). | Field is required in Gold output but computation formula is not specified in assignment. |
| A9 | `avg_order_value` is computed as `total_revenue / total_orders` at the stated Gold grain. | Standard interpretation of required metric names. |
| A10 | Gold "customer segmentation" uses the `customer_segment` field from customer data as `segment_type`. | Matches source schema and required output field name. |

### Assumptions Requiring Explicit Confirmation (Not Defined by Assignment)

| ID | Open Decision | Default Proposal |
|----|---------------|----------------|
| D1 | Which `order_status` values contribute to Gold revenue (`Completed` only vs. all statuses). | Use `Completed` orders for revenue metrics. |
| D2 | Whether Gold reads only Silver-valid rows (`dq_is_valid = true`) or all rows with SQL filters documented in Gold SQL. | Use Silver-valid rows for business-ready Gold outputs. |
| D3 | Bronze typing strategy: preserve all source columns as strings vs. inferred source types in Delta. | Preserve raw values; cast/standardize in Silver. |
| D4 | Duplicate handling for analytics: whether Gold excludes all duplicate-key rows or uses a deterministic survivor row. | Exclude duplicate-key rows from Gold aggregates unless all duplicates are flagged invalid. |
| D5 | Histogram bucket boundaries for customer revenue distribution. | Use fixed, documented SQL buckets in dashboard query. |

### Assignment Requirements (Not Assumptions)

The following are explicit assignment requirements and are not optional:

- Medallion flow: Source CSV → Bronze → Silver → Gold → Dashboard
- Required Silver check categories: completeness, uniqueness, referential integrity, type/business validation
- Required intentional issue counts in Section 6
- Required Gold outputs A, B, C with listed fields
- Required dashboard visuals (3)
- Repository structure as provided in the assignment brief
- Technology stack: Databricks, PySpark, Python, SQL, Delta Lake, Databricks SQL Dashboard

---

## 10. Edge Cases

The pipeline design must explicitly handle the following scenarios without silent data loss.

### Null Values

- NULL `email` in customers (intentional completeness issue).
- NULL `customer_id` and NULL `product_id` in orders (intentional completeness issues).
- NULL `payment_date` in orders (valid nullable field; may fail business-rule checks depending on `order_status`).

### Duplicate Primary Keys

- Duplicate `customer_id` values in customers (10 duplicate key values).
- Duplicate `order_id` rows in orders (20 duplicate rows).
- All duplicate members should be flagged by uniqueness checks.

### Orphan Foreign Keys

- Orders with `customer_id` not present in customers (50 rows).
- Orders with `product_id` not present in products (30 rows).
- Rows must be retained and flagged; not silently removed.

### Invalid Dates

- Malformed or unparseable dates in CSV inputs should fail type/business validation in Silver and be flagged.
- Logical date inconsistencies (e.g., `payment_date` before `order_date`) should be handled by business validation if implemented.

### Negative Quantities

- Negative `quantity` values should fail type/business validation and be flagged if present in source or derived test cases.

### Invalid Prices

- Negative `unit_price`, `price`, or `cost` values should fail type/business validation and be flagged if present.
- Inconsistent monetary values (e.g., `total_amount` not equal to `quantity * unit_price`) should be handled by business validation.

### Invalid Statuses

- `order_status` values outside `Pending` / `Completed` / `Cancelled` should fail type/business validation.
- `customer_segment` values outside `Premium` / `Standard` / `Basic` should fail type/business validation.

### Cancelled Orders

- Cancelled orders may exist in source and Silver.
- Gold revenue treatment depends on decision D1 (default: exclude from revenue unless otherwise confirmed).

### Pending Orders

- Pending orders may exist in source and Silver.
- May have NULL `payment_date`; business validation should define expected behavior.
- Gold revenue treatment depends on decision D1 (default: exclude from revenue unless otherwise confirmed).

### Missing Payment Dates

- Allowed for nullable field, but may be invalid for certain statuses (e.g., `Completed`) under business validation rules.

### Zero Revenue

- Orders with zero `total_amount` may exist; aggregation logic must not fail and should report zero revenue correctly.

### Customers Without Orders

- Customers may have no associated orders; Gold B and segmentation logic must handle zero-order customers without errors.

### Products Without Orders

- Products may have no associated orders; Gold A should still represent products with zero orders (zero counts/revenue), unless downstream SQL design filters them; behavior must be documented.

---

## 11. Acceptance Criteria

Use this checklist for final validation of the assessment deliverable.

### Repository and Documentation

- [ ] Repository follows the required structure defined in the assignment brief.
- [ ] `README.md` explains project purpose and how to run the pipeline.
- [ ] `requirements-analysis.md`, `design-notes.md`, `data-model.md`, and `data-quality-strategy.md` are present and consistent.
- [ ] AI workflow and reflection artifacts are present (`tool-workflow.md`, `final-ai-usage-summary.md`, `ai-prompts/`).

### Sample Data Generation

- [ ] `src/data_generation/generate_sample_data.py` generates all three CSV files deterministically.
- [ ] Output files are written to `data/customers.csv`, `data/orders.csv`, `data/products.csv`.
- [ ] Row volumes are approximately 10,000 customers, 100,000 orders, and 500 products.
- [ ] Intentional customer issues exist: 50 NULL emails, 10 duplicate `customer_id` values.
- [ ] Intentional order issues exist: 100 NULL `customer_id`, 200 NULL `product_id`, 50 orphan `customer_id`, 30 orphan `product_id`, 20 duplicate `order_id` rows.
- [ ] `src/data_generation/DATA_GENERATION_NOTES.md` documents generation logic and issue injection.

### Bronze Layer

- [ ] `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py` ingest CSVs to Bronze Delta tables.
- [ ] `ingest_all.py` orchestrates Bronze ingestion.
- [ ] Bronze preserves raw source data (no silent deletion/filtering of bad rows).
- [ ] Ingestion logic is separate from Silver/Gold business transformations.

### Silver Layer and Data Quality

- [ ] Silver implements completeness checks.
- [ ] Silver implements uniqueness checks.
- [ ] Silver implements referential integrity checks.
- [ ] Silver implements type/business validation checks.
- [ ] `05_quality_business_logic.py` supports business validation requirements (as part of type/business validation).
- [ ] `create_silver_tables.py` produces Silver tables and quality outcomes.
- [ ] Bad rows are not silently deleted.
- [ ] Bad rows are identifiable via quality status/result fields.
- [ ] Silver contains cleaned/validated data and quality results.

### Data Quality Reporting

- [ ] Quality report/metrics include pass count, fail count, and pass/fail percentages per check.
- [ ] Metrics are measurable and traceable by entity and check category.
- [ ] Detected failures align with intentional issue volumes from Section 6.

### Gold Layer

- [ ] `01_sales_by_product.sql` produces: `product_id`, `product_name`, `category`, `total_orders`, `total_revenue`, `avg_order_value`.
- [ ] `02_revenue_by_customer.sql` produces: `customer_id`, `customer_name`, `customer_segment`, `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_actual`.
- [ ] `04_customer_segmentation.sql` produces: `segment_type`, `customer_count`, `avg_revenue`, `total_revenue`.
- [ ] `create_gold_tables.py` orchestrates Gold table creation.
- [ ] Gold outputs are business-ready analytical datasets.

### Dashboard

- [ ] `dashboard_queries.sql` contains queries for required visuals.
- [ ] `DASHBOARD_GUIDE.md` documents dashboard setup.
- [ ] Dashboard includes bar chart: Top 10 products by revenue.
- [ ] Dashboard includes histogram: Customer revenue distribution.
- [ ] Dashboard includes pie chart: Customer segmentation.

### Engineering Quality

- [ ] Components are independently testable.
- [ ] Logic is deterministic where required for reproducible tests.
- [ ] Configuration/constants are separated from transformation logic where practical.
- [ ] Pipeline can be rerun safely without uncontrolled data loss or silent bad-record removal.
- [ ] Tests exist or test approach is documented for changed behavior.

### Optional (Not Required for Core Acceptance)

- [ ] `03_daily_weekly_trends.sql` implemented and wired to Gold orchestration.
- [ ] Advanced monitoring/alerting on quality thresholds beyond required metrics reporting.
- [ ] Additional dashboard visuals beyond the required three.
