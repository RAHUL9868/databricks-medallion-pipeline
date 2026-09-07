# Project Context

This document describes what context was available to the AI assistant (Cursor Agent) while building the **e-commerce Medallion pipeline** for a Data Engineering AI Capability Assessment.

**Primary development record:** Cursor chat transcript `96f9a367-722e-481c-8fec-a8d5921f19dc` (same session as this repository).

---

## Assignment framing

The project is a **production-quality Databricks Medallion Architecture** pipeline:

```
Source CSV → Bronze → Silver → Quality Metrics → Gold → Dashboard
```

Core constraints communicated at project start:

- Build **incrementally** — do not generate the entire project at once
- Analyze requirements and surface ambiguities before coding
- Prefer simple, maintainable **PySpark** and **SQL**
- Keep business logic separate from ingestion; configuration separate from transforms
- **Flag bad data; never silently delete** invalid rows
- Every component should be **independently testable**
- Target **Databricks Community Edition** (DBFS, single schema, modest volume ~110k rows)

---

## Repository structure (specified in initial prompt)

The user provided a target layout including:

| Area | Purpose |
|------|---------|
| `src/data_generation/` | Deterministic CSV generator |
| `src/bronze/` | Per-entity ingest + `ingest_all.py` |
| `src/silver/` | DQ modules `01`–`05` + `create_silver_tables.py` |
| `src/gold/` | SQL aggregations + `create_gold_tables.py` |
| `src/dashboard/` | Dashboard SQL + guide |
| `src/config/` | `pipeline_config.py` |
| `tests/` | pytest suite |
| Root docs | `requirements-analysis.md`, `design-notes.md`, `data-model.md`, etc. |

---

## Domain context

### Source entities

| Dataset | Approx. volume | Role |
|---------|----------------|------|
| `customers.csv` | 10,000 | Customer master |
| `products.csv` | 500 | Product catalog |
| `orders.csv` | 100,000 | Transactions |

### Intentional data quality issues (seed 42)

Documented counts the pipeline must detect:

| Issue | Count |
|-------|------:|
| NULL customer emails | 50 |
| Duplicate `customer_id` rows | 10 keys (20 rows in duplicate groups) |
| NULL order `customer_id` | 100 |
| NULL order `product_id` | 200 |
| Invalid `customer_id` | 50 |
| Invalid `product_id` | 30 |
| Duplicate `order_id` rows | 20 keys (40 rows in duplicate groups) |

Canonical test expectations: `tests/quality_expectations.py`.

### Gold / dashboard expectations

- Revenue from **`Completed`** orders only
- Gold reads **`dq_is_valid = true`** Silver rows
- Three dashboard visuals: Top 10 products (bar), revenue distribution (histogram), segmentation (pie)
- Optional: `gold_daily_weekly_trends`

---

## Technical context provided to the agent

| Source | What it contributed |
|--------|---------------------|
| **Initial user message** | Standing engineering rules, repo structure, incremental workflow |
| **Prior assistant design proposal** | Ambiguity list, design decisions D1–D7, table naming |
| **`requirements-analysis.md`** | Formal requirements after user approved design discussion |
| **`design-notes.md` / `data-model.md`** | Architecture, column-level contracts, layer responsibilities |
| **`data-quality-strategy.md`** | Rule IDs, metrics shape, execution order |
| **Existing code files** | Each new prompt built on files created in earlier steps |
| **Cursor codebase tools** | Read, grep, write, shell (pytest often unavailable in agent environment) |

---

## Environment assumptions

| Topic | Assumption |
|-------|------------|
| **Catalog** | Optional `PIPELINE_CATALOG`; default hive schema `ecommerce` |
| **Paths** | Default `dbfs:/FileStore/ecommerce/data`; overridable via env/CLI |
| **Secrets** | No workspace tokens, passwords, or personal paths in repo |
| **CSV files** | Generated locally; not committed to git |
| **Dashboard** | Manual setup in Databricks SQL UI (not deployed by code) |

---

## Context evolution over the session

The agent did **not** receive a single monolithic spec. Context accumulated in layers:

1. Design proposal (no code)
2. Requirements + architecture docs (no code)
3. DQ strategy (no code)
4. One component per prompt (generator → Bronze → Silver modules → consolidate → Gold → dashboard → database docs → `run_pipeline.py` → README → E2E review)
5. E2E review findings fed into README **Known Limitations** (documented gaps, not all fixed in code)

---

## Documents the agent was asked to produce

| Document | When |
|----------|------|
| `requirements-analysis.md` | After approved design discussion |
| `design-notes.md`, `data-model.md` | Architecture phase |
| `data-quality-strategy.md` | Before Silver implementation |
| `DATA_GENERATION_NOTES.md` | With generator |
| `database/*` | Reproducibility / Databricks setup |
| `DASHBOARD_GUIDE.md` | After dashboard SQL |
| `README.md` | Onboarding guide |
| This workflow set | AI-assisted engineering record |

---

## What was *not* in context

- Live Databricks workspace credentials or cluster IDs
- Executed pytest results in the agent environment (Python/Spark often unavailable)
- Automated CI/CD configuration
- Pre-existing `.cursor/rules` files in the repository (none found in repo at documentation time)

See `tool-workflow.md` for how prompts and reviews used this context in practice.
