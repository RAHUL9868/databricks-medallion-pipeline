# Cursor Rules & Instructions

This document captures the **standing instructions** and **prompt patterns** used during AI-assisted development of this repository. It is derived from the opening user message and recurring constraints in chat transcript `96f9a367-722e-481c-8fec-a8d5921f19dc`.

These are **not** automatically enforced Cursor Rules (no `.cursor/rules` files were committed at documentation time). They describe how the human directed the agent.

---

## 1. Standing engineering rules (session charter)

From the first user message (2026-09-07 12:09):

| Rule | Intent |
|------|--------|
| **Do not generate the entire project at once** | Incremental delivery, reviewable diffs |
| **Build incrementally** | One layer or module per prompt |
| **Analyze requirements; identify ambiguities** | Design before code |
| **Do not make silent assumptions** on unclear requirements | Call out decisions explicitly |
| **Prefer simple, maintainable PySpark and SQL** | Community Edition friendly |
| **Production-review ready code** | Readable by a Data Engineer |
| **Do not over-engineer** | Minimal abstractions |
| **Every component independently testable** | Module-level tests |
| **Follow defined repository structure** | Paths under `src/`, `tests/` |
| **Separate concerns** | Ingest vs business logic; config vs transforms |

---

## 2. Recommended prompt template

Effective prompts in this project followed a consistent shape:

```
1. ROLE (optional)     — "Act as a senior Data Engineer…"
2. SCOPE               — Exact files to create or review
3. REQUIREMENTS        — Numbered, testable acceptance criteria
4. CONSTRAINTS         — Spark-native, no driver collect, flag-not-delete, etc.
5. DELIVERABLES        — Code + tests + docs
6. PROCESS (optional)— "Do not modify code initially" / "explain before SQL"
```

### Example (implementation)

> Implement `src/silver/02_quality_uniqueness.py` …  
> Requirements: 1. Detect duplicate keys … 10. Support reruns …  
> **Important:** If a key occurs twice, decide whether both rows or only subsequent rows are flagged. Document the decision.  
> Also create tests.

### Example (review-only)

> Act as a senior Data Engineer reviewing the Bronze implementation.  
> **Do not modify the code initially.**  
> Create a validation checklist … inspect against every item …  
> **Only make changes after presenting the review findings.**

### Example (documentation)

> Based on the **approved design discussion above**, create `requirements-analysis.md` with this structure: …

---

## 3. Review-first workflow (explicit user pattern)

The user invoked **review-before-change** at least three times:

| Step | Topic | User instruction |
|------|-------|------------------|
| Data generation | Generator correctness | "Do NOT immediately rewrite it" |
| Bronze | Ingest validation | "Do not modify the code initially" … "Only make changes after presenting the review findings" |
| Full pipeline | E2E defects | "Do not immediately change code" |

**Agent behavior when this pattern is used:**

1. Produce checklist or structured findings
2. Map each issue to file + recommended fix
3. **Stop** unless a follow-up prompt requests fixes

---

## 4. Layer-specific instructions

### Bronze

- Raw STRING preservation; explicit schemas; PERMISSIVE CSV
- Configurable paths (`PIPELINE_SOURCE_BASE_PATH`); no hard-coded workspace IDs
- Log ingest metadata; safe rerun (`overwrite` default)

### Silver

- Never delete failed rows
- Row-level `dq_*` flags + dataset-level metrics
- Modules `01`–`05` runnable standalone; `create_silver_tables.py` consolidates
- Avoid contradictory quality columns → `consolidate_quality_model()` from `dq_failed_rules`

### Gold

- Explain aggregation logic **before** SQL when requested
- `Completed` orders only; valid Silver only; dedupe `order_id`
- `create_gold_tables.py` must reconcile and fail fast

### Dashboard

- Gold-only queries; document chart type, axes, filters per query
- Manual Databricks SQL Dashboard setup (guide separate from SQL)

---

## 5. Testing instructions

Recurring requirements in prompts:

- Prove **intentional issue counts** (seed 42)
- Use **small isolated fixtures** plus full generated data where appropriate
- Document what each test validates (expected vs actual pattern in `tests/test_support.py`)
- Run `pytest` when possible; state clearly when environment lacks Java/Spark

---

## 6. Security & sensitive information

Instructions implied and followed in repo:

| Practice | Implementation |
|----------|----------------|
| No secrets in code | Config via env vars only |
| No personal paths | Defaults use `dbfs:/FileStore/...` or `./data` via CLI |
| No credentials in docs | Setup uses generic Databricks patterns |
| CSV data not committed | Generated locally; documented in README |

**Do not** paste workspace tokens, PATs, or cluster HTTP paths into chat or commits.

---

## 7. Suggested Cursor configuration (optional)

To reproduce this workflow in a new session, paste or save rules equivalent to:

```markdown
- Incremental builds only; one component per task.
- Read design-notes.md and data-quality-strategy.md before changing Silver/Gold.
- Flag invalid data; never drop rows in Silver.
- Use pipeline_config.py for paths and table names.
- After generating code, list files changed and how to test.
- For reviews: findings first, no code changes unless asked.
- Do not claim tests passed unless pytest was run successfully.
```

---

## 8. Anti-patterns to avoid

| Anti-pattern | Why |
|--------------|-----|
| Monolithic "build everything" prompt | Violates session charter; hard to review |
| Skipping design docs | Later modules depend on `dq_*` contracts |
| Driver `collect()` on large DataFrames | User required Spark-native operations |
| Silent renames (`customers_silver` vs `silver_customers`) | Breaks `data-model.md` consistency |
| Claiming Databricks dashboard is automated | Only SQL + manual UI setup exists |

---

## Related documents

| File | Purpose |
|------|---------|
| `tool-workflow.md` | Full lifecycle + evidence-based AI/human decisions |
| `project-context.md` | Domain and technical context |
| `task-breakdown.md` | Chronological task list |
| `README.md` | Engineer onboarding |
