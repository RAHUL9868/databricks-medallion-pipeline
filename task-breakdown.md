# Task Breakdown

Chronological breakdown of work items as they appeared in the Cursor development session. Timestamps are from the chat transcript (`96f9a367-722e-481c-8fec-a8d5921f19dc`).

**Legend:** ✅ Delivered in repo | 📋 Review-only step (no code change requested) | 🔧 Orchestration / docs

---

## Phase 0 — Kickoff & design (no code)

| # | Time (UTC+5:30) | User request | Deliverable | Status |
|---|-----------------|--------------|-------------|--------|
| 0.1 | 12:09 | AI assistant rules + incremental build; analyze requirements first | Design proposal in chat (ambiguities, D1–D7, table names) | ✅ (chat) |
| 0.2 | 12:09 | Create `requirements-analysis.md` from approved design | `requirements-analysis.md` | ✅ |
| 0.3 | 12:11 | Create `design-notes.md` + `data-model.md` | Both root docs | ✅ |
| 0.4 | 12:14 | Create `data-quality-strategy.md` | `data-quality-strategy.md` | ✅ |

---

## Phase 1 — Data generation

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 1.1 | 12:15 | Implement `generate_sample_data.py` + notes | `src/data_generation/*` | ✅ |
| 1.2 | 12:18 | Senior DE review of generator; **do not rewrite immediately** | Critical review in chat | 📋 |
| 1.3 | — | *(No follow-up prompt in transcript asking to apply review fixes)* | Generator unchanged; tests codify seed=42 counts | ✅ |

---

## Phase 2 — Bronze layer

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 2.1 | 12:24 | Implement Bronze ingest (4 scripts + shared logic) | `src/bronze/*`, `src/config/pipeline_config.py` | ✅ |
| 2.2 | 12:26 | Bronze review checklist; **do not modify code initially** | Review findings in chat | 📋 |
| 2.3 | — | *(No follow-up in transcript to implement Bronze review fixes)* | Bronze code as delivered in 2.1 | ✅ |

---

## Phase 3 — Silver quality (module-by-module)

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 3.1 | 12:28 | `01_quality_completeness.py` + tests | Module + `tests/test_quality_completeness.py` | ✅ |
| 3.2 | 12:31 | `02_quality_uniqueness.py` + tests | Module + tests; duplicate-group semantics documented | ✅ |
| 3.3 | 12:36–12:38 | Repeat/implement uniqueness (transcript shows duplicate user message) | Same module refined | ✅ |
| 3.4 | 12:38 | `04_quality_referential_integrity.py` + tests | Module + tests | ✅ |
| 3.5 | 12:39 | `03_quality_type_validation.py` + tests | Module + tests; RI module updated for `dq_type_business_pass` | ✅ |
| 3.6 | 12:42 | `05_quality_business_logic.py` + tests | Module + tests; rule split vs module 03 | ✅ |
| 3.7 | 12:44 | `create_silver_tables.py` — consolidate DQ, metrics, reporting | Orchestrator + `silver_dq_rules.py` | ✅ |
| 3.8 | 12:47 | Silver test suite for intentional issues | `tests/test_silver_quality.py`, `quality_expectations.py`, supporting tests | ✅ |

---

## Phase 4 — Gold layer

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 4.1 | 13:04 | `01_sales_by_product.sql` | Gold SQL + metric comments | ✅ |
| 4.2 | — | `02_revenue_by_customer.sql` | *(Delivered in session; prompt in transcript continuation)* | ✅ |
| 4.3 | — | `03_daily_weekly_trends.sql` | Daily + weekly union | ✅ |
| 4.4 | — | `04_customer_segmentation.sql` | Behavioral segments + documented thresholds | ✅ |
| 4.5 | 13:15 | `create_gold_tables.py` with reconciliation | Orchestrator + Python reconciliation checks | ✅ |
| 4.6 | — | `tests/test_gold.py` | Orchestration + helper tests (limited SQL integration) | ✅ |

---

## Phase 5 — Dashboard

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 5.1 | 13:17 | `dashboard_queries.sql` | SQL + visualization comments | ✅ |
| 5.2 | 13:18 | `DASHBOARD_GUIDE.md` | Step-by-step Databricks SQL setup | ✅ |

---

## Phase 6 — Database & operations docs

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 6.1 | 13:19 | `database/schema.sql`, `seed-data-notes.md`, `setup-notes.md` | All three files | ✅ |

---

## Phase 7 — End-to-end orchestration

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 7.1 | 13:22 | `src/run_pipeline.py` (15-step sequence) | E2E entry point + `tests/test_run_pipeline.py` | ✅ |
| 7.2 | 13:26 | E2E pipeline review; **do not change code** | Classified findings (CRITICAL–LOW) in chat | 📋 |
| 7.3 | 13:29 | Production `README.md` | `README.md` with known limitations from review | ✅ |

---

## Phase 8 — Workflow documentation (this set)

| # | Time | User request | Deliverable | Status |
|---|------|--------------|-------------|--------|
| 8.1 | 13:31 | `tool-workflow.md`, cursor rules, task breakdown, project context | These four files | ✅ |

---

## Dependency graph (execution order)

```
Docs (0.x)
    ↓
generate_sample_data.py (1.1)
    ↓
Bronze ingest (2.1)
    ↓
Silver 01 → 02 → 03 → 04 → 05 (3.1–3.6)
    ↓
create_silver_tables.py (3.7)
    ↓
Gold SQL 01–04 → create_gold_tables.py (4.x)
    ↓
dashboard_queries.sql + guide (5.x)
    ↓
run_pipeline.py (7.1) — optional wrapper over above
    ↓
Dashboard manual setup (Databricks UI)
```

---

## Out-of-scope / not requested in transcript

- Fixing all E2E review defects in code
- Git commits or CI pipelines
- Automated dashboard deployment
- Unity Catalog governance beyond config hooks

See `tool-workflow.md` for how reviews related to implementation decisions.
