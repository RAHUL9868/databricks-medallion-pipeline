# Tool Workflow — Cursor AI-Assisted Development

How **Cursor Agent** was used to build the e-commerce Medallion pipeline, mapped to the assessment lifecycle.

**Evidence source:** Chat transcript `96f9a367-722e-481c-8fec-a8d5921f19dc` and artifacts in this repository.  
**Honesty note:** Where the transcript does not show an explicit human accept/reject decision, this document says so rather than inventing one.

---

## Workflow at a glance

```
┌─────────────────────────────────────────────────────────────────┐
│  Human: charter + incremental prompts + review gates            │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Cursor Agent: read codebase → propose/design → implement       │
│                → (optional) review-only response                │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Human: next prompt, implicit continuation, or review request │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Validation: pytest (local), SQL checks (Databricks), E2E review│
└─────────────────────────────────────────────────────────────────┘
```

---

## How project context was provided

| Mechanism | Usage |
|-----------|--------|
| **Opening charter** | Incremental build, separation of concerns, testability |
| **Structured doc prompts** | User supplied markdown outlines for requirements, design, DQ strategy |
| **"Approved design discussion"** | Next doc prompt referenced prior assistant proposal as source of truth |
| **Cumulative repo state** | Each implementation prompt assumed earlier files exist (`pipeline_config.py`, prior Silver modules) |
| **Explicit acceptance criteria** | Counts (50 NULL emails, 40 duplicate order rows, etc.) in generator and Silver test prompts |
| **Review prompts** | Checklists and severity without immediate code changes |

Details: `project-context.md`.  
Task order: `task-breakdown.md`.  
Prompt patterns: `cursor-rules-or-instructions.md`.

---

## How prompts were structured

### Phase A — Design only (no files)

1. User set AI role + constraints  
2. Agent produced **design proposal** with ambiguities and decisions D1–D7  
3. User prompted: *"Based on the approved design discussion above, create requirements-analysis.md"*  

### Phase B — Documentation anchors

Sequential doc creation locked contracts before code:

`requirements-analysis.md` → `design-notes.md` + `data-model.md` → `data-quality-strategy.md`

### Phase C — Implementation slices

Typical prompt anatomy:

- **File paths** to create  
- **Numbered requirements** (8–16 items)  
- **Non-functional constraints** (Spark-native, configurable, logging)  
- **Tests** required in same or follow-up step  

### Phase D — Review gates

User explicitly blocked immediate edits:

- *"Do NOT immediately rewrite"* (data generator)  
- *"Do not modify the code initially"* (Bronze)  
- *"Do not immediately change code"* (E2E review)  

Agent returned findings; **transcript does not show follow-up prompts to apply every review fix.**

---

## Lifecycle by phase

### 1. Requirement analysis

| Item | Detail |
|------|--------|
| **Cursor role** | Summarize assignment; list ambiguities; propose decisions |
| **Human role** | Approve design discussion; request formal `requirements-analysis.md` |
| **Output** | `requirements-analysis.md` |
| **Validation** | Cross-check against later prompts (intentional DQ counts, three dashboard charts) |

---

### 2. Architecture design

| Item | Detail |
|------|--------|
| **Cursor role** | Author `design-notes.md` (12 sections) + `data-model.md` (column-level) |
| **Constraints** | Databricks CE, CSV→Bronze→Silver→Gold→Dashboard |
| **Validation** | Later code references same table names (`silver_*`, `gold_*`) |

---

### 3. Data generation

| Item | Detail |
|------|--------|
| **Cursor role** | Implement `generate_sample_data.py`, `DATA_GENERATION_NOTES.md` |
| **Design choices** | Seed 42; disjoint corruption pools; pre-write validation |
| **Review** | User requested senior DE review without immediate rewrite (12:18) |
| **Testing** | `tests/test_data_generation.py`; `quality_expectations.py` as single source of truth |

---

### 4. Bronze implementation

| Item | Detail |
|------|--------|
| **Cursor role** | `bronze_ingest.py`, schemas, per-entity scripts, `ingest_all.py`, extend `pipeline_config.py` |
| **Pre-implementation** | Agent published Bronze design (STRING schema, metadata, audit table) in chat before coding |
| **Review** | User requested checklist review; agent reported issues **without modifying code** (12:26) |
| **Testing** | `tests/test_bronze.py` |

---

### 5. Silver implementation

Delivered as **six code steps** plus consolidation:

| Module | Focus |
|--------|--------|
| `01_quality_completeness.py` | NULL email, NULL FKs |
| `02_quality_uniqueness.py` | Duplicate PK semantics |
| `03_quality_type_validation.py` | Types, domains, numeric constraints |
| `04_quality_referential_integrity.py` | Orphan FK detection |
| `05_quality_business_logic.py` | Cross-field rules |
| `create_silver_tables.py` | Consolidation, casting, metrics, views |

**Per-module tests:** `tests/test_quality_*.py`  
**Integration tests:** `tests/test_silver_quality.py`, `tests/test_create_silver_tables.py`

---

### 6. Gold implementation

| Item | Detail |
|------|--------|
| **Pattern** | User asked for aggregation logic explanation, then SQL file |
| **SQL assets** | `01`–`04` in `src/gold/` |
| **Orchestration** | `create_gold_tables.py` with reconciliation + `GoldPipelineError` |
| **Testing** | `tests/test_gold.py` (orchestration/helpers; not full four-table Spark integration) |

---

### 7. Dashboard

| Item | Detail |
|------|--------|
| **SQL** | `dashboard_queries.sql` with commented metadata per visual |
| **Guide** | `DASHBOARD_GUIDE.md` for manual Databricks SQL Dashboard setup |
| **Not automated** | No code deploys dashboard; engineer follows guide |

---

### 8. Testing

| Layer | Approach |
|-------|----------|
| **Unit / module** | pytest + local Spark session (`tests/conftest.py`) |
| **Intentional DQ** | `quality_expectations.py` + `test_silver_quality.py` |
| **Fixtures** | `tests/test_support.py` — small DataFrames, `ValidationCheck` helper |
| **Agent environment** | Transcript shows pytest often **could not run** (no Python/Java); agent stated this explicitly |

**Human validation:** Run `python -m pytest tests/ -v` locally with Java installed.

---

### 9. Debugging

Observed debugging style in transcript:

| Technique | Example |
|-----------|---------|
| **Read + grep codebase** | Before implementing type module, grep overlap with business rules |
| **Cross-module fixes** | After adding module 03, patch module 04 so `dq_is_valid` includes `dq_type_business_pass` |
| **Test adjustment** | Simplify tolerance test when decimal parsing behavior clarified |
| **Environment diagnosis** | Report `python`/`py` not found; suggest user-run pytest |

No separate bug ticket system — debugging occurred inline in the chat session.

---

### 10. Documentation

| When | Documents |
|------|-----------|
| Early | Requirements, design, data model, DQ strategy |
| Per component | `DATA_GENERATION_NOTES.md`, SQL header comments |
| Ops | `database/*`, `DASHBOARD_GUIDE.md` |
| Late | `README.md`, this workflow set |

**E2E review (13:26)** informed README **Known Limitations** without requiring all defects to be fixed first.

---

### 11. Code review

Three review modes used:

| Mode | Trigger | Agent output |
|------|---------|--------------|
| **Self-review after implement** | Generator complete | Critical review prose (user asked) |
| **Checklist review** | Bronze complete | Pass/fail per checklist item |
| **E2E audit** | Pipeline complete | CRITICAL–LOW findings with fixes |

**No Bugbot or external PR bot** appears in transcript — reviews were prompt-driven.

---

### 12. Validation

| Validation type | Where |
|-----------------|--------|
| **Pre-write asserts** | Generator validates counts before CSV write |
| **Row-count invariants** | Silver modules + `create_silver_tables.py` |
| **pytest** | `tests/` |
| **Gold reconciliation** | `create_gold_tables.py` at runtime |
| **E2E review** | Chat-only defect register (13:26) |
| **Manual SQL** | Documented in `DASHBOARD_GUIDE.md`, `setup-notes.md` |

---

## How generated code was reviewed

1. **Human prompt as review spec** — Checklists defined what to inspect  
2. **Agent self-review** — Senior DE narrative before edits  
3. **Tests as executable review** — Expected failure counts encoded in `quality_expectations.py`  
4. **E2E review** — Cross-layer consistency (schema, reconciliation, dashboard)  
5. **README honesty** — Limitations section reflects review findings not yet fixed  

---

## How generated code was tested

| Step | Action |
|------|--------|
| 1 | Agent writes `tests/test_*.py` alongside module |
| 2 | Agent attempts `pytest` in shell |
| 3 | If environment fails, agent **documents** manual test command for human |
| 4 | Human runs full suite on machine with Java + PySpark |
| 5 | Databricks validation via SQL in setup/dashboard guides |

---

## AI suggestions: accepted, modified, rejected

### Evidence-based: accepted or implemented

These align with user prompts and exist in the repo (no explicit "I reject" in transcript):

| Suggestion | Evidence |
|------------|----------|
| Incremental layer-by-layer build | Opening user charter |
| `dq_*` quality model over separate quarantine table | `create_silver_tables.py` docstring + user requirement to keep bad rows |
| Flag **all rows** in duplicate PK groups | User asked agent to choose; agent documented "all members flagged" (`02_quality_uniqueness.py`) |
| `consolidate_quality_model()` authoritative flags | `create_silver_tables.py` |
| Gold revenue = Completed + valid Silver only | User Gold SQL prompts + design D1/D2 |
| `gold_daily_weekly_trends` as fourth Gold table | `create_gold_tables.py` step list |
| Behavioral segmentation (`High-Value`, etc.) | `04_customer_segmentation.sql` comments + SQL |
| `run_pipeline.py` 15-step orchestration | User prompt 13:22; file exists |
| Disjoint corruption pools in generator | Implemented in `generate_sample_data.py` |

### Evidence-based: modified before merge

Human requirement met, but **agent changed the proposal** during implementation:

| Topic | Prompt / doc | What shipped | Evidence |
|-------|--------------|--------------|----------|
| Silver table names | User asked for `customers_silver`, etc. | `silver_customers`, `silver_orders`, `silver_products` | `data-model.md` + `create_silver_tables.py` |
| Business rules 1–4 vs module 05 | User listed quantity/price/status/signup in business module | Those rules stay in **module 03**; module **05** only cross-field rules | `05_quality_business_logic.py` `TYPE_RULES_OWNED_BY_MODULE_03` |
| Tolerance unit test | Initial test used `10.005` / `10.01` | Test simplified to exact decimal match | Transcript line ~80: `test_quality_business_logic.py` edit |
| RI + type interaction | Module 04 initially ignored type pass | `04_quality_referential_integrity.py` updated to include `dq_type_business_pass` in `dq_is_valid` | Transcript line 74 |

### No explicit rejection recorded

The transcript **does not contain** user messages such as "don't do X" or "revert Y" after a concrete AI proposal.

What *is* documented:

| Situation | Interpretation (conservative) |
|-----------|-------------------------------|
| Review findings for Bronze/generator | User asked for review **without** immediate fixes; **no follow-up fix prompt** appears in transcript |
| E2E review defects | User asked for report **without** code changes; README lists limitations; **not all defects fixed** |
| Duplicate order metric "20 vs 40" | Review noted doc ambiguity; **tests use 40** (`quality_expectations.py`); user continued pipeline without generator change prompt |

**Do not cite these as "rejected AI suggestions"** — cite them as **documented gaps or unresolved review items**.

---

## Human engineering judgment applied

| Decision | Human input | Outcome |
|----------|-------------|---------|
| Build order | Incremental prompts | Docs → generator → Bronze → Silver modules → Gold → dashboard |
| Review before rewrite | Explicit on generator, Bronze, E2E | Findings without mandatory auto-fix |
| Duplicate semantics | "Choose clearest semantics" | All rows in duplicate group flagged |
| Quarantine vs single table | Assignment: don't delete bad rows | Invalid rows in primary `silver_*` tables |
| When to add E2E runner | Dedicated prompt at 13:22 | `run_pipeline.py` |
| Honest README | After E2E review | Known limitations section |
| Sensitive data | Assignment + config design | No secrets; env-based paths |

---

## How sensitive information was avoided

| Risk | Mitigation |
|------|------------|
| Workspace tokens in repo | None committed; config uses env vars |
| Personal file paths | CLI/`PIPELINE_*` overrides; README uses `./data` or DBFS examples |
| Customer PII in samples | Synthetic `@mail.example.com` emails in generator |
| Credentials in chat | Transcript shows engineering discussion only |

---

## Tooling used inside Cursor

| Tool | Role |
|------|------|
| **Read / Grep / Glob** | Explore existing modules before edits |
| **Write / StrReplace** | Create and patch files |
| **Shell** | pytest (often unavailable) |
| **Task / explore subagents** | Broad codebase exploration (E2E review, database docs) |
| **Linter diagnostics** | Spot-check edited Python files |

---

## Gaps in the AI-assisted workflow

Transparent limitations for assessors:

1. **pytest not reliably run by agent** — tests written but human should execute  
2. **Databricks not exercised in agent environment** — SQL/logic validated by inspection + reconciliation code  
3. **Review findings not all remediated** — see README Known Limitations  
4. **No `.cursor/rules` in repo** — instructions lived in chat + this document  
5. **Dashboard manual** — AI produced SQL + guide, not a deployed artifact  

---

## Quick reference — key files by phase

| Phase | Primary artifacts |
|-------|-------------------|
| Analysis | `requirements-analysis.md` |
| Design | `design-notes.md`, `data-model.md`, `data-quality-strategy.md` |
| Generate | `src/data_generation/generate_sample_data.py` |
| Bronze | `src/bronze/bronze_ingest.py`, `ingest_all.py` |
| Silver | `src/silver/01`–`05`, `create_silver_tables.py` |
| Gold | `src/gold/*.sql`, `create_gold_tables.py` |
| Dashboard | `src/dashboard/dashboard_queries.sql`, `DASHBOARD_GUIDE.md` |
| E2E | `src/run_pipeline.py` |
| Onboard | `README.md`, `database/setup-notes.md` |

---

## Related documents

- `cursor-rules-or-instructions.md` — Reusable prompt rules  
- `project-context.md` — Domain and technical context  
- `task-breakdown.md` — Chronological tasks with timestamps  
- `README.md` — How to run the pipeline  
