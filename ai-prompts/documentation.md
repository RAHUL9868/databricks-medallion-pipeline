# AI Prompts — Documentation & Orchestration

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

Covers design-phase documentation, database/setup docs, E2E runner, README, and workflow meta-documentation.

---

## Prompt 1 — Session Charter & Design Proposal

**Timestamp:** 12:09 PM

### PROMPT SENT

```
You are my AI engineering assistant for a Data Engineering AI Capability Assessment.

We are building a production-quality Databricks Medallion Architecture pipeline for an e-commerce business.

IMPORTANT:
- Do not generate the entire project at once.
- We will build the project incrementally.
- Before generating code, analyze requirements and identify ambiguities.
- Do not make silent assumptions when an important requirement is unclear.
- Prefer simple, maintainable PySpark and SQL.
- Code must be understandable by a Data Engineer and production-review ready.
- Do not over-engineer the solution.
- Every generated component must be testable independently.
- Follow the repository structure defined below.
- Keep business logic separate from ingestion logic.
- Keep configuration/constants separate from transformation logic where practical.
- Add meaningful comments/docstrings, but don't comment obvious code.
- Use deterministic logic wherever possible so tests are reproducible.
- Never silently delete bad data.
- Data quality failures must be traceable and measurable.
- Preserve Bronze data as raw/unchanged as required.
- Silver should contain cleaned/validated data and quality results.
- Gold should contain business-ready analytical datasets.

TECHNOLOGY:
- Databricks
- PySpark
- Python
- SQL
- Delta Lake
- Databricks SQL Dashboard
- Cursor as the primary AI development tool

BUSINESS CONTEXT:
An e-commerce company ingests daily sales data from:
1. Customer database
2. Order system
3. Product catalog

The pipeline must implement:

Source CSV
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Dashboard

SOURCE DATASETS:

customers.csv:
- customer_id INT PRIMARY KEY
- customer_name STRING
- email STRING
- country STRING
- signup_date DATE
- customer_segment STRING: Premium/Standard/Basic
- lifetime_value DECIMAL

Expected approximately 10,000 rows.

orders.csv:
- order_id INT PRIMARY KEY
- customer_id INT FOREIGN KEY
- order_date DATE
- product_id INT FOREIGN KEY
- quantity INT
- unit_price DECIMAL
- total_amount DECIMAL
- order_status STRING: Pending/Completed/Cancelled
- payment_date DATE nullable

Expected approximately 100,000 rows.

products.csv:
- product_id INT PRIMARY KEY
- product_name STRING
- category STRING
- price DECIMAL
- cost DECIMAL
- stock_quantity INT
- reorder_level INT

Expected approximately 500 rows.

INTENTIONAL DATA QUALITY ISSUES:

customers:
- 50 rows with NULL email
- 10 duplicate customer_id values

orders:
- 100 rows with NULL customer_id
- 200 rows with NULL product_id
- 50 rows with customer_id not present in customers
- 30 rows with product_id not present in products
- 20 duplicate order_id rows

The intentional issues should be realistic and deterministic.

REQUIRED SILVER QUALITY CHECKS:
1. Completeness
2. Uniqueness
3. Referential Integrity
4. Type/business validation

Bad rows must NOT simply be deleted.
They must be identifiable using quality status/result fields.

QUALITY REPORT:
Generate measurable metrics showing pass/fail counts and percentages for each quality check.

GOLD REQUIREMENTS:

A) Sales by Product:
- product_id
- product_name
- category
- total_orders
- total_revenue
- avg_order_value

B) Revenue by Customer:
- customer_id
- customer_name
- customer_segment
- total_orders
- total_revenue
- avg_order_value
- lifetime_value_actual

C) Customer Segmentation:
- segment_type
- customer_count
- avg_revenue
- total_revenue

DASHBOARD:
At least 3 visualizations:
1. Top 10 products by revenue - bar chart
2. Customer revenue distribution - histogram
3. Customer segmentation - pie chart

REQUIRED REPOSITORY STRUCTURE:

databricks-medallion-pipeline/
├── README.md
├── candidate-info.md
├── tool-workflow.md
├── requirements-analysis.md
├── design-notes.md
├── data-model.md
├── data-quality-strategy.md
├── src/
│   ├── data_generation/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dashboard/
├── data/
├── database/
├── debugging-notes.md
├── reflection.md
├── final-ai-usage-summary.md
└── ai-prompts/
    ├── data-generation.md
    ├── bronze-layer.md
    ├── silver-layer.md
    ├── gold-layer.md
    ├── dashboard.md
    ├── debugging.md
    └── documentation.md

YOUR FIRST TASK:

Do NOT write implementation code yet.

Instead:

1. Analyze all requirements.
2. Identify functional requirements.
3. Identify non-functional requirements.
4. Identify assumptions that need to be made.
5. Identify edge cases.
6. Identify potential ambiguities/conflicts in the assignment.
7. Propose the pipeline architecture.
8. Propose the data flow.
9. Propose table names for Bronze/Silver/Gold.
10. Propose a testing strategy.
11. Propose how bad records should be flagged.
12. Propose how quality metrics should be calculated.
13. Explain how you will ensure Bronze remains raw.
14. Explain how the pipeline can be rerun safely.
15. Identify where configuration should be externalized.

Present the response as a design proposal.

Do not create source files yet.
```

### AI RESPONSE SUMMARY

Agent produced in-chat **Design Proposal** covering requirements summary, ambiguities, decisions **D1–D7**, proposed `bronze_*` / `silver_*` / `gold_*` table names, `dq_*` flag model, testing strategy, and Community Edition constraints. **No files written.**

### MY EVALUATION

#### Accepted

- Incremental charter (no big-bang code).
- Explicit ambiguity callouts (duplicate semantics, segmentation thresholds, revenue status rules).
- Design proposal before implementation.

**Evidence:** Next prompt references *"approved design discussion above."*

#### Changed

- N/A (chat-only deliverable).

#### Rejected

- None recorded.

#### Reasoning

- Context-setting prompt anchors all later layer prompts.

### VALIDATION

Subsequent prompts and repo artifacts align with D1–D7 and table naming from proposal.

---

## Prompt 2 — Requirements Analysis Document

**Timestamp:** 12:09 PM

### PROMPT SENT

```
Based on the approved design discussion above, create:

requirements-analysis.md

Use this structure:

# Requirement Analysis

## 1. Problem Statement
## 2. Objectives
## 3. Functional Requirements
## 4. Non-Functional Requirements
## 5. Source Data Requirements
## 6. Data Quality Requirements
## 7. Gold Business Requirements
## 8. Dashboard Requirements
## 9. Assumptions
## 10. Edge Cases
## 11. Acceptance Criteria

Do not invent additional mandatory business requirements.
Keep optional improvements clearly marked as optional.
```

*(Full section bullets from transcript included in charter; structure enforced as listed.)*

### AI RESPONSE SUMMARY

Created root `requirements-analysis.md` with all 11 sections, intentional DQ counts, acceptance checklist, assumptions vs assignment requirements.

### MY EVALUATION

#### Accepted

- Formal requirements anchor before code.
- Acceptance criteria for final validation.

#### Changed

- None recorded.

#### Rejected

- None recorded.

### VALIDATION

Cross-reference later modules against Section 6 DQ counts and Section 11 checklist.

---

## Prompt 3 — Design Notes & Data Model

**Timestamp:** 12:11 PM

### PROMPT SENT

```
Now create:

design-notes.md
data-model.md

The architecture must follow:

CSV → Bronze → Silver → Gold → Dashboard

Explain:

1. Overall architecture
2. Data flow
3. Bronze responsibilities
4. Silver responsibilities
5. Gold responsibilities
6. Dashboard responsibilities
7. Error handling
8. Data quality reporting
9. Logging/observability
10. Idempotency/re-run strategy
11. Dependency between datasets
12. Testing strategy

For the data model, document:

customers
orders
products

For every column specify:
- name
- data type
- nullable/not nullable
- business meaning
- primary key/foreign key where applicable
- validation rule

Also define proposed Bronze, Silver and Gold table names.

Keep the architecture simple enough to run in Databricks Community Edition.

Do not generate implementation code yet.
```

### AI RESPONSE SUMMARY

Created `design-notes.md` (12 sections) and `data-model.md` (column-level schemas, `silver_*` table names, Bronze STRING strategy).

### MY EVALUATION

#### Accepted

- Architecture docs before implementation.
- CE-friendly simplicity.

#### Changed

- None recorded.

#### Rejected

- None recorded.

### VALIDATION

Bronze/Silver/Gold implementations reference these docs; some drift noted in E2E review (`data-model.md` vs segmentation).

---

## Prompt 4 — Data Quality Strategy

**Timestamp:** 12:14 PM

### PROMPT SENT

```
Create:

data-quality-strategy.md

Design a practical data quality framework for this assignment.

The framework must cover:

1. Completeness
2. Uniqueness
3. Referential integrity
4. Type validation
5. Business-rule validation

For each check document:

- Check name
- Business purpose
- Source table
- Columns
- Rule
- Expected result
- Failure condition
- Row-level flag
- Dataset-level metric
- Pass percentage
- Failure percentage
- Example bad record
- How the test will prove the intentional issue was caught

IMPORTANT:

Do not delete invalid records merely to make quality metrics pass.

Define a consistent quality representation, for example:

quality_check_result
quality_status
quality_errors

but choose the final implementation based on maintainability.

Also define how multiple failures on the same row will be represented.

Explain how quality metrics will be persisted.

Create a final section called:

"Expected Intentional Data Quality Issues"

and list every issue specified in the assignment with expected approximate counts.
```

### AI RESPONSE SUMMARY

Created `data-quality-strategy.md` with `dq_*` column conventions, per-check tables, multi-failure representation, metrics persistence (`silver_dq_metrics`), and intentional issue inventory.

### MY EVALUATION

#### Accepted

- Flag-not-delete principle.
- Check catalog driving Silver module implementation.

#### Changed

- Implementation chose `dq_*` names over generic `quality_status` examples in prompt.

#### Rejected

- None recorded.

#### Reasoning

- `dq_*` prefix improves grep-ability and module consistency.

### VALIDATION

Silver modules and `silver_dq_rules.py` implement documented checks; E2E noted minor doc drift on duplicate order row counts.

---

## Prompt 5 — Database Setup Documentation

**Timestamp:** 1:19 PM

### PROMPT SENT

```
Create:

database/schema.sql
database/seed-data-notes.md
database/setup-notes.md

The setup must be compatible with Databricks.

Document:

- catalog/schema assumptions
- Bronze tables
- Silver tables
- Gold tables
- table dependencies
- creation order
- sample data loading
- pipeline execution order
- cleanup/reset approach

Avoid environment-specific hard-coded catalog names unless clearly parameterized.

The setup documentation should allow another engineer to reproduce the project.
```

### AI RESPONSE SUMMARY

Created three database files from implemented pipeline: reference `schema.sql`, CSV upload and corruption notes, Databricks setup and execution order.

### MY EVALUATION

#### Accepted

- Parameterized catalog/schema patterns.
- Reproducibility focus for another engineer.

#### Changed

- None recorded.

#### Rejected

- None recorded.

### VALIDATION

README Setup section references `database/setup-notes.md`.

---

## Prompt 6 — End-to-End Pipeline Entry Point

**Timestamp:** 1:22 PM

### PROMPT SENT

```
Now create a single end-to-end pipeline entry point.

Create or update:

src/run_pipeline.py

Pipeline sequence:

1. Validate configuration
2. Generate/validate sample data if requested
3. Ingest customers
4. Ingest products
5. Ingest orders
6. Run completeness checks
7. Run uniqueness checks
8. Run type validation
9. Run referential integrity checks
10. Run business validation
11. Build Silver tables
12. Generate quality metrics
13. Build Gold tables
14. Run reconciliation checks
15. Run final validation

Requirements:

- clear execution order
- logging
- error handling
- fail-fast behavior for critical failures
- configuration-driven paths/table names
- safe reruns
- meaningful exit status
- no driver-side processing of large datasets
- no hard-coded local machine paths

Add a --help style usage description if practical.

The pipeline should be understandable and executable by another engineer.
```

### AI RESPONSE SUMMARY

Created `src/run_pipeline.py` with 15 steps, CLI (`--help`, `--generate-sample-data`, path/schema args), fail-fast on critical errors. **Refactored** Silver/Gold orchestrators to support stepped execution.

### MY EVALUATION

#### Accepted

- Single entry point matching assignment flow.
- Config-driven, no hard-coded paths.

#### Changed

- Orchestrator refactors to enable stepped pipeline (see `debugging.md`).

#### Rejected

- None recorded.

### VALIDATION

`tests/test_run_pipeline.py`; README Quick Start command.

---

## Prompt 7 — Production README

**Timestamp:** 1:29 PM

### PROMPT SENT

```
Create a production-quality README.md.

It should include:

# Project Overview
## Business Problem
## Architecture
## Technology Stack
## Repository Structure
## Source Data
## Data Quality Issues
## Setup
## Configuration
## Running Data Generation
## Running Bronze
## Running Silver
## Running Gold
## Running End-to-End Pipeline
## Testing
## Data Quality Validation
## Gold Metrics
## Dashboard
## Troubleshooting
## Assumptions
## Design Decisions
## Known Limitations
## Future Improvements

The README must be executable as a guide.

A new engineer should be able to understand the project and run it without asking the author basic setup questions.

Do not claim something works unless the repository implementation supports it.
```

### AI RESPONSE SUMMARY

Created `README.md` with all sections, Quick Start, honest **Known Limitations** from E2E review (no false claims about dashboard automation or fixed defects).

### MY EVALUATION

#### Accepted

- Executable onboarding guide.
- Known Limitations reflect E2E findings (integrity over marketing).

#### Changed

- None recorded post-delivery.

#### Rejected

- None recorded.

#### Reasoning

- Prompt explicitly forbids unsupported claims; E2E review informed limitations section.

### VALIDATION

Human can follow Quick Start without author; limitations match `debugging.md` open findings.

---

## Prompt 8 — Workflow Documentation (Interrupted)

**Timestamp:** 1:31 PM

### PROMPT SENT

```
Create:

tool-workflow.md

and:

cursor-rules-or-instructions.md
task-breakdown.md
project-context.md

Document how Cursor was used across the lifecycle.

Include:

1. Requirement analysis
2. Architecture design
3. Data generation
4. Bronze implementation
5. Silver implementation
6. Gold implementation
7. Dashboard
8. Testing
9. Debugging
10. Documentation
11. Code review
12. Validation

Explain:

- how project context was provided
- how prompts were structured
- how generated code was reviewed
- how generated code was tested
- examples of accepted AI suggestions
- examples of rejected AI suggestions
- examples of modified AI suggestions
- how human engineering judgment was applied
- how sensitive information was avoided

Do not claim that I accepted or rejected something unless there is evidence from the actual development history.

The document should accurately represent an AI-assisted engineering workflow.
```

### AI RESPONSE SUMMARY

**Interrupted** — partial completion in session; user resent as Prompt 9.

### MY EVALUATION

#### Accepted / Changed / Rejected

- N/A for incomplete turn.

### VALIDATION

N/A

---

## Prompt 9 — Workflow Documentation (Completed)

**Timestamp:** 1:31 PM

### PROMPT SENT

*(Identical to Prompt 8.)*

### AI RESPONSE SUMMARY

Created `tool-workflow.md`, `cursor-rules-or-instructions.md`, `task-breakdown.md`, `project-context.md` with evidence-based accept/modify/defer notes and transcript references.

### MY EVALUATION

#### Accepted

- Evidence-based workflow narrative (no fabricated rejections).
- Cross-links between docs.

#### Changed

- N/A (completion of interrupted prompt).

#### Rejected

- None recorded.

### VALIDATION

Docs consistent with `prompt-extraction.md` and repo state.

---

## Related Files

| File | Role |
|------|------|
| `prompt-extraction.md` | Full verbatim prompt archive |
| `tool-workflow.md` | Lifecycle workflow summary |
| `ai-prompts/*.md` | Per-layer prompt journals (this set) |
