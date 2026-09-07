# Reflection

**Project:** E-commerce Medallion pipeline (CSV → Bronze → Silver → Gold → Dashboard)  
**Development record:** Cursor session `96f9a367-722e-481c-8fec-a8d5921f19dc` (Sep 7, 2026)

---

## What I Built

A Databricks-oriented Medallion pipeline with deterministic sample data and explicit data quality:

| Layer | Deliverables |
|-------|----------------|
| **Source** | `generate_sample_data.py` — seed `42`, ~10k customers / 100k orders / 500 products, intentional NULLs, orphans, and duplicate keys |
| **Bronze** | Per-entity ingest scripts + `ingest_all.py`, STRING schemas, ingest audit table, env-driven `pipeline_config.py` |
| **Silver** | Five DQ modules (`01`–`05`), `create_silver_tables.py` with `consolidate_quality_model()`, `silver_dq_metrics` / `silver_dq_report` |
| **Gold** | Four SQL aggregations + `create_gold_tables.py` with runtime reconciliation and `GoldPipelineError` |
| **Dashboard** | `dashboard_queries.sql` (Gold-only) + manual `DASHBOARD_GUIDE.md` |
| **E2E** | `run_pipeline.py` — 15 ordered steps from config validation through Gold reconciliation |
| **Tests** | `quality_expectations.py` as single source of truth for intentional issue counts; pytest across generation, Bronze, Silver, Gold helpers, pipeline config |

The pipeline **flags** bad rows (`dq_is_valid`, `dq_failed_rules`) and does **not** delete them to improve metrics.

---

## How I Used AI Across the Lifecycle

I used Cursor Agent incrementally — not as a one-shot code generator.

1. **Charter (no code)** — Opening prompt set rules: incremental build, surface ambiguities, flag-not-delete, testable components. First output was an in-chat design proposal (decisions D1–D7, table names, `dq_*` model).

2. **Documentation anchors** — I prompted structured docs before implementation: `requirements-analysis.md` → `design-notes.md` + `data-model.md` → `data-quality-strategy.md`. The requirements prompt explicitly referenced the *"approved design discussion above."*

3. **Layer slices** — Each layer was a separate prompt with file paths and numbered requirements (e.g. 14 items for completeness, 16 for Bronze).

4. **Review gates** — Three times I blocked immediate edits:
   - Data generator: *"Do NOT immediately rewrite it"*
   - Bronze: *"Do not modify the code initially"*
   - Full pipeline: *"Do not immediately change code"*

5. **Validation prompts** — Silver test suite prompt listed exact intentional counts; Gold prompts required *"explain aggregation logic before SQL"*; segmentation required proposed thresholds before implementation.

6. **Honesty prompt** — README and workflow docs explicitly asked not to claim unsupported behavior and not to fabricate accept/reject history.

Prompt journals: `ai-prompts/*.md`. Full verbatim archive: `prompt-extraction.md`.

---

## What AI Helped With Most

**1. Repetitive PySpark module scaffolding**  
Silver modules `01`–`05` share a pattern: read Bronze/Silver → apply rules → append to `dq_failed_rules` → write metrics. AI produced consistent structure (`silver_common.py`, rule IDs, CLI entry points) faster than hand-writing five similar files.

**2. Design documents from outlines**  
I supplied section headings; AI filled `requirements-analysis.md` (11 sections), `design-notes.md` (12 architecture topics), and per-check tables in `data-quality-strategy.md`. That gave implementation prompts a stable contract.

**3. Cross-layer review**  
The E2E review prompt produced a classified defect list (e.g. Bronze `validate_csv_columns()` ineffective with explicit schema, Gold product dedup gap, `DASHBOARD_GUIDE.md` §6.5 wrong query) that I used directly in README **Known Limitations**.

**4. SQL with documented business rules**  
Gold SQL files include inline comments for Completed-only revenue, valid Silver filter, and dedupe intent — matching design D1/D2 without me writing four long scripts from scratch.

**5. Consolidation logic**  
`consolidate_quality_model()` in `create_silver_tables.py` — deriving authoritative `dq_*_pass` flags from `dq_failed_rules` — was a non-trivial piece AI drafted after five separate module prompts.

---

## Where AI Was Wrong

**1. Bronze header validation (CRITICAL)**  
`validate_csv_columns()` does not catch missing CSV headers when an explicit `StructType` is used — columns bind by position, not name. Surfaced in Bronze review and again in E2E review. Still open at session end.

**2. Incomplete first draft of `create_silver_tables.py`**  
AI’s first orchestrator version was incomplete; it required a full rewrite before consolidation, metrics, and `silver_dq_rules.py` worked.

**3. Documentation drift**  
`data-quality-strategy.md` wording on duplicate orders (20 rows) did not match Silver semantics (all members flagged → 40 failed uniqueness rows) or `quality_expectations.py`.

**4. Gold / dashboard gaps**  
E2E review found: `gold_sales_by_product` may not dedupe `product_id` like customer Gold does; `PipelineConfig` table name overrides are ignored inside `.sql` files; `DASHBOARD_GUIDE.md` validation query pointed at wrong Silver logic.

**5. Environment assumptions**  
The agent often could not run `python` or `pytest` in its shell. Early summaries could imply tests passed when they were only **written**, not executed in that environment.

**6. Order duplicate semantics during generation**  
AI initially interpreted *"20 duplicate order_id records"* ambiguously; it self-corrected to 20 appended rows (40 rows in duplicate groups) before my review prompt — but the ambiguity persisted in docs and assignment wording.

---

## Important Engineering Decisions I Made

| Decision | How it was made | Outcome |
|----------|-----------------|---------|
| **Incremental build** | Opening charter — no whole-repo generation | Reviewable diffs per layer |
| **Docs before code** | Sequential doc prompts after design approval | `dq_*` contract before Silver modules |
| **Review before rewrite** | Explicit on generator, Bronze, E2E | Findings without mandatory auto-fix |
| **Duplicate flagging** | Prompt asked AI to choose semantics | All rows in duplicate key group flagged (`02_quality_uniqueness.py`) |
| **Silver table names** | Prompt asked `customers_silver`; `data-model.md` had `silver_customers` | Shipped `silver_customers`, `silver_orders`, `silver_products` |
| **Rule ownership** | Business prompt listed rules already in type module | Qty/price/status/signup stay in `03`; `05` = cross-field only (`TYPE_RULES_OWNED_BY_MODULE_03`) |
| **Invalid rows in primary Silver tables** | `create_silver_tables` prompt asked quarantine vs in-table | Primary tables + flags, no quarantine |
| **Gold revenue scope** | Design D1/D2 + Gold prompts | `Completed` + `dq_is_valid` only |
| **Segmentation thresholds** | Prompt: propose before SQL, don’t invent silently | Documented precedence in `04_customer_segmentation.sql` |
| **Honest README** | Post–E2E prompt: don’t claim unsupported behavior | Known Limitations lists unfixed review items |
| **No secrets in repo** | Charter + config design | `PIPELINE_*` env vars, no workspace tokens |

---

## What I Accepted From AI

Evidence: subsequent prompts continued without rewrite requests; artifacts remain in repo.

- **`dq_*` quality column model** — `dq_failed_rules`, `dq_is_valid`, per-category pass flags, metrics to `silver_dq_metrics`.
- **Disjoint corruption pools** in `generate_sample_data.py` — one intentional defect per row where possible.
- **Pre-write count validation** in the generator — fail fast if seed `42` counts drift.
- **Bronze STRING preservation** + explicit schemas in `bronze_schemas.py`.
- **Shared `batch_id`** across three ingests in one `ingest_all` run (AI self-corrected during Bronze implementation).
- **All-members-flagged duplicate semantics** — 20 customer / 40 order uniqueness failures in tests.
- **`consolidate_quality_model()`** as authoritative quality state in Silver consolidation.
- **Gold Completed-only revenue** and valid-Silver filter across all four SQL files.
- **Fourth Gold table** `gold_daily_weekly_trends` for dashboard date filtering.
- **15-step `run_pipeline.py`** sequence matching the assignment flow.
- **pytest + local Spark** in `tests/conftest.py` rather than a custom test framework.

---

## What I Rejected From AI

The transcript does **not** contain explicit messages like *"don’t do X"* or *"revert Y."* What follows is evidence-based: choices I did **not** adopt, or review fixes I **deferred** (not the same as rejection).

**Not adopted as prompted**

| AI / prompt direction | What shipped instead | Why |
|----------------------|----------------------|-----|
| Table names `customers_silver`, `orders_silver`, `products_silver` | `silver_customers`, `silver_orders`, `silver_products` | Consistency with `data-model.md` |
| All business rules in `05_quality_business_logic.py` | Type/domain rules remain in `03` | Avoid double-flagging same row |

**Deferred (review findings not fixed in session)**

- Generator review recommendations — no follow-up fix prompt; tests codify current behavior.
- Bronze review fixes (e.g. header validation) — continued to Silver; listed in README future work.
- E2E review CRITICAL/HIGH items — documented in Known Limitations rather than patched before README.

**Process I explicitly blocked**

- **Immediate rewrite after review** — three prompts required findings first; I did not ask the agent to auto-apply every recommendation in the same turn.

I did **not** reject the duplicate *"flag all group members"* approach — I asked the agent to choose, and that choice stayed through tests and `quality_expectations.py`.

---

## How I Validated AI Output

| Method | Example |
|--------|---------|
| **Encoded expectations** | `tests/quality_expectations.py` mirrors `generate_sample_data.py` constants (50 NULL emails, 40 duplicate order rows under all-members-flagged semantics) |
| **Per-module pytest** | `test_quality_completeness.py`, `test_quality_uniqueness.py`, etc., with small Spark fixtures |
| **Integration-style Silver tests** | `test_silver_quality.py` asserts intentional issue rule counts |
| **Generator pre-write asserts** | `generate_sample_data.py` validates counts before CSV write |
| **Gold runtime reconciliation** | `create_gold_tables.py` compares Silver valid Completed revenue to Gold totals; fails with `GoldPipelineError` |
| **Review checklists** | Generator (13 questions), Bronze (12 categories), E2E (15 validation areas) |
| **README honesty check** | Limitations section matches E2E findings — no claim of automated dashboard deploy or fixed Bronze header validation |
| **Manual commands** | `python src/data_generation/generate_sample_data.py --seed 42`; `python -m pytest tests/ -v`; `python src/run_pipeline.py --help` (human-run when agent shell lacked Java/Python) |

**Gap:** Full Gold SQL integration is thin in pytest; reconciliation at Gold orchestration runtime is the main safety net.

---

## Debugging Experience

**Cross-module Silver bug** — After implementing `03_quality_type_validation.py`, referential integrity needed to respect `dq_type_business_pass` when module `04` ran after `03`. AI patched `04_quality_referential_integrity.py` so `dq_is_valid` stayed consistent.

**Test import friction** — Python cannot `import 01_quality_completeness` normally. AI added `importlib` loader in `tests/test_quality_completeness.py` and created `tests/conftest.py`.

**Orchestrator rewrite** — First `create_silver_tables.py` draft was incomplete; second pass added `silver_dq_rules.py`, consolidation, and metrics reporting.

**Pipeline stepping** — `run_pipeline.py` required refactoring Silver/Gold orchestrators to expose ordered steps without duplicating logic.

**Interrupted prompts** — Type validation, dashboard SQL, and workflow docs needed resend when the first message had no completion; duplicate uniqueness prompt confirmed existing code rather than duplicating work.

**Agent environment** — When `pytest` could not run in Cursor’s shell, validation depended on me running tests locally — a real workflow risk if I had treated “tests created” as “tests passed.”

---

## Data Quality Lessons

1. **Count semantics must be defined once** — Assignment said *"20 duplicate order_id rows"*; Silver flagged **all members** of duplicate groups → **40** failed uniqueness rows. Without `quality_expectations.py`, generator, strategy doc, and tests would disagree.

2. **Category boundaries matter** — NULL `customer_id` is completeness; orphan `customer_id` is referential integrity. Mixing them inflates RI metrics and confuses dashboards.

3. **Disjoint corruption pools** — Injecting defects on separate row sets prevents compound failures that break deterministic tests.

4. **Flag, don’t delete** — Invalid orders remain in `silver_orders` with `dq_is_valid = false`; Gold filters them out explicitly. Metrics stay honest.

5. **Metrics table behavior** — `silver_dq_metrics` **appends** per run; consumers must filter by `run_id`. Easy to misread “latest” pass rates if ignored.

6. **Consolidation is required** — Five modules each add `dq_failed_rules`; without `consolidate_quality_model()`, pass flags and `dq_is_valid` can contradict each other.

---

## Medallion Architecture Lessons

1. **Bronze stays raw** — All business columns as STRING; no casting or DQ in ingest. Type validation belongs in Silver. The cost: header/schema mismatches are harder to detect at Bronze without extra validation.

2. **Silver owns typing and DQ** — Bronze → cast + rules → `silver_*` with lineage columns. Gold should not re-implement DQ rules.

3. **Gold reads curated Silver** — Revenue SQL filters `dq_is_valid` and `order_status = 'Completed'`. Dashboard SQL reads Gold only — no Silver in `dashboard_queries.sql`.

4. **Orchestration vs SQL** — Python (`create_gold_tables.py`, `run_pipeline.py`) owns order, logging, and reconciliation; `.sql` files own business math. **Gap:** SQL hardcodes table names while `PipelineConfig` allows overrides — config and SQL can drift.

5. **Community Edition constraints** — Overwrite reruns, single schema, manual dashboard setup. Architecture choices (no automated dashboard deploy) reflect real CE limits, not oversights.

6. **Lineage** — `_ingest_ts`, `_batch_id`, `dq_run_id` support audit, but `_source_row_num` is Spark row order, not guaranteed physical CSV line number — weak for dedup tie-breaking.

---

## What I Would Improve

From E2E review and README Known Limitations — concrete next steps:

1. **Header-based CSV validation** before explicit-schema Bronze read (CRITICAL).
2. **Deduplicate `product_id`** in `gold_sales_by_product.sql` (parity with customer Gold).
3. **Segmentation revenue reconciliation** in `create_gold_tables.py`.
4. **Fix `DASHBOARD_GUIDE.md` §6.5** Silver validation query.
5. **Sync `data-model.md`** with behavioral segmentation and implemented `dq_*` columns.
6. **Align `data-quality-strategy.md`** duplicate-order counts with `quality_expectations.py` (40 rows).
7. **CI workflow** — `generate_sample_data.py --seed 42` → `pytest` on every push.
8. **`PIPELINE_AS_OF_DATE`** for deterministic segmentation instead of `CURRENT_DATE()`.
9. **Metrics write strategy** — partition or overwrite `silver_dq_metrics` by `run_id` instead of unbounded append.

---

## What I Learned About Prompting

1. **Numbered requirements work** — Prompts with 10–16 numbered items map cleanly to review checklists and test cases (e.g. completeness prompt → three NULL count tests).

2. **“Explain before implement” reduces rework** — Bronze design-before-code and Gold *"explain aggregation logic before SQL"* produced auditable decisions without premature files.

3. **Review-only prompts are high leverage** — *"Do not modify the code initially"* forced structured findings; cheaper than fixing the wrong implementation twice.

4. **Force ambiguity resolution** — Segmentation prompt: *"Do not silently invent thresholds"* → proposed rules in chat before SQL. Duplicate prompt: *"choose and document"* → all-members-flagged semantics.

5. **One component per prompt** — Charter explicitly forbade whole-project generation; Silver was six prompts plus consolidation plus tests — each diff stayed reviewable.

6. **Specify file paths** — `src/silver/02_quality_uniqueness.py` left no ambiguity about deliverable location.

7. **Honesty constraints belong in prompts** — README *"Do not claim something works unless the repository supports it"* produced Known Limitations instead of a marketing README.

8. **Resend when interrupted** — Type validation and dashboard SQL needed a second identical prompt when the first turn did not complete.

---

## Reusable Workflow

For a similar Medallion + DQ project with Cursor:

```
1. Charter prompt     → rules, stack, structure, NO code
2. Design in chat     → ambiguities, decisions, table names
3. Doc prompts        → requirements → design → DQ strategy (outlines provided)
4. Implement slice    → one module, numbered reqs, tests in same or next prompt
5. Review gate        → checklist, findings only, no auto-fix
6. Repeat 4–5         → per layer / per module
7. Consolidate        → orchestrator + single quality model
8. Test suite prompt  → central expectations file (e.g. quality_expectations.py)
9. Gold               → explain rules → SQL → orchestrator with reconciliation
10. Dashboard         → SQL + manual setup guide (don’t claim automation)
11. E2E review        → classify defects, no immediate rewrite
12. README            → executable + known limitations from review
13. Meta-docs         → ai-prompts/, workflow, reflection (evidence-based)
```

**Standing rules to paste into new sessions:** incremental only; read `data-quality-strategy.md` before Silver/Gold changes; flag not delete; `pipeline_config.py` for paths; findings before fixes on review prompts; do not claim tests passed unless `pytest` succeeded.

**Artifacts to keep:** `quality_expectations.py` (or equivalent), `prompt-extraction.md` / `ai-prompts/*.md`, and a Known Limitations section that tracks unfixed review items.

---

## Related Documentation

| Document | Purpose |
|----------|---------|
| `tool-workflow.md` | Lifecycle and evidence-based AI/human decisions |
| `ai-prompts/*.md` | Per-layer prompt journals with evaluation |
| `task-breakdown.md` | Chronological task list with timestamps |
| `cursor-rules-or-instructions.md` | Reusable prompt patterns |
| `README.md` | How to run the pipeline |
