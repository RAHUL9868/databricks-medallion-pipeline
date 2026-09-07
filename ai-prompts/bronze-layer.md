# AI Prompts — Bronze Layer

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

---

## Prompt 1 — Implement Bronze Layer

**Timestamp:** 12:24 PM

### PROMPT SENT

```
Implement the Bronze layer.

Create:

src/bronze/01_ingest_customers.py
src/bronze/02_ingest_orders.py
src/bronze/03_ingest_products.py
src/bronze/ingest_all.py

Requirements:

1. Read CSV files from a configurable DBFS/Databricks path.
2. Create Bronze Delta tables.
3. Preserve source data as raw as practical.
4. Do not perform business transformations in Bronze.
5. Use explicit schemas where appropriate instead of relying blindly on inference.
6. Handle CSV parsing errors.
7. Handle missing files gracefully.
8. Log ingestion timestamp.
9. Log source file/path.
10. Log row counts.
11. Log column counts.
12. Make table names configurable.
13. Avoid hard-coded workspace-specific paths.
14. Make the process safe to rerun.
15. Ensure all three datasets are ingested.
16. Add clear error messages.

Design an appropriate ingestion metadata/audit mechanism.

Important:
Bronze is intended to preserve raw source information. Do not perform Silver-style cleaning here.

Before writing code, explain:
- schema strategy
- table naming
- overwrite vs append behavior
- rerun strategy
- ingestion logging strategy

Then implement the code.

Ensure the generated code can be executed in Databricks notebooks or as Python source files with minimal adaptation.
```

### AI RESPONSE SUMMARY

The agent published a **Bronze Layer Design** in chat (STRING schemas, explicit `StructType`, metadata columns, PERMISSIVE CSV, configurable `PipelineConfig`), then implemented:

- `src/config/pipeline_config.py` (env-driven paths and table names)
- `src/bronze/bronze_schemas.py`, `bronze_ingest.py`
- `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`, `ingest_all.py`
- `bronze_ingest_audit` table for ingest metadata

**Iteration during implementation:** pinned a shared `batch_id` across the three entity ingests in `ingest_all.py`; audit table uses append mode.

### MY EVALUATION

#### Accepted

- All business columns as STRING in Bronze (raw preservation).
- Explicit schemas in `bronze_schemas.py` (no blind inference).
- Config via `PipelineConfig` / `PIPELINE_*` env vars — no workspace IDs in code.
- Pre-implementation design explanation before code (as requested).
- Ingest audit table with row/column counts and timestamps.

**Evidence:** User proceeded to Bronze review (Prompt 2) without asking for a rewrite.

#### Changed

- **Shared `batch_id`:** Agent adjusted `ingest_all` so one batch ID spans customers/products/orders in a single run (transcript self-correction).
- **Extra shared module:** User listed four entry scripts; agent also added `bronze_ingest.py` and `bronze_schemas.py` for reuse (reasonable extension, not explicitly requested).

#### Rejected

- No explicit user rejection in transcript.

#### Reasoning

- STRING Bronze aligns with design docs (D3) — casting belongs in Silver.
- Shared ingest utilities reduce duplication across three entity scripts.
- Single `batch_id` improves traceability for a full ingest run.

### VALIDATION

| Check | How |
|-------|-----|
| Unit tests | `tests/test_bronze.py` (schema, metadata, ingest helpers) |
| Manual | `python src/bronze/ingest_all.py --source-base-path ./data` after generating CSVs |
| Review | Prompt 2 checklist (below) |

---

## Prompt 2 — Senior DE Bronze Review (Review-Only)

**Timestamp:** 12:26 PM

### PROMPT SENT

```
Act as a senior Data Engineer reviewing the Bronze implementation.

Do not modify the code initially.

Create a Bronze validation checklist covering:

- files exist
- schemas match expectations
- row counts
- column counts
- data types
- raw values preserved
- intentional NULLs preserved
- intentional duplicates preserved
- invalid foreign keys preserved
- ingestion metadata captured
- rerun behavior
- error handling

Then inspect the implementation against every item.

For every issue found:
- explain why it is an issue
- identify the affected file
- recommend a fix

Only make changes after presenting the review findings.
```

### AI RESPONSE SUMMARY

The agent produced a **Bronze Layer Review** with a full validation checklist (F1–F3, S1–S2, etc.) mapped to verification steps. It reported issues including:

- **`validate_csv_columns()` ineffective** when using explicit schema (missing CSV headers not detected) — later classified CRITICAL in E2E review
- **`_source_row_num`** not guaranteed physical CSV line order
- Header validation gaps vs `data-model.md`

**No code was modified** in this turn.

### MY EVALUATION

#### Accepted

- Review-first pattern (same as data generator review).
- Checklist usable for manual Databricks/local verification after ingest.

#### Changed

- Nothing changed after this prompt in the transcript.

#### Rejected

- **No explicit “reject” message** from the user.
- **Deferred:** Recommended fixes (e.g. header validation) were **not implemented** in this session — no follow-up fix prompt. E2E review later echoed the header-validation finding; `README.md` Known Limitations lists it as a future improvement.

#### Reasoning

- Review identified real risks without forcing a large refactor mid-build.
- Proceeding to Silver allowed DQ work to continue; Bronze header issue documented for later targeted fix.

### VALIDATION

| Check | How |
|-------|-----|
| Review artifact | Chat findings + checklist table |
| Code state | Bronze modules unchanged post-review in session |
| Later trace | E2E review (Prompt in `debugging.md`) → README Known Limitations #1 (future header validation) |
