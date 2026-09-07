# AI Prompts — Debugging & Review

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

This file covers the formal **end-to-end review** prompt and **implementation iterations** where the agent corrected code after inspection or test friction (not separate user prompts).

---

## Prompt 1 — End-to-End Pipeline Review

**Timestamp:** 1:26 PM

### PROMPT SENT

```
Now perform an end-to-end review of the entire pipeline.

Do not immediately change code.

Trace:

CSV
→ Bronze
→ Silver
→ Quality Metrics
→ Gold
→ Dashboard

Validate:

1. Schema consistency
2. Data type consistency
3. Row counts
4. Primary-key behavior
5. Foreign-key behavior
6. Quality issue detection
7. Quality percentages
8. Gold calculations
9. Revenue reconciliation
10. Customer reconciliation
11. Product reconciliation
12. Segmentation reconciliation
13. Rerun behavior
14. Error handling
15. Dashboard query compatibility

Identify every defect or risk.

Classify each as:

CRITICAL
HIGH
MEDIUM
LOW

For every finding provide:
- problem
- impact
- root cause
- recommended fix

Do not rewrite everything.
Prefer targeted corrections.
```

### AI RESPONSE SUMMARY

Agent traced CSV → Dashboard across code, tests, and docs. Produced classified findings, including:

| Severity | Examples |
|----------|----------|
| **CRITICAL** | Bronze `validate_csv_columns()` ineffective with explicit schema |
| **HIGH** | `_source_row_num` not true CSV order; Gold product dedup; config table overrides ignored in Gold SQL; `DASHBOARD_GUIDE` wrong Silver query; segmentation revenue reconciliation gap; `silver_dq_metrics` append on rerun |
| **MEDIUM** | Doc drift (duplicate order count 20 vs 40); segmentation `CURRENT_DATE()`; thin Gold integration tests |
| **LOW** | Histogram empty buckets; `ORDER BY` in Gold CTAS |

**No code changes** in the same turn.

### MY EVALUATION

#### Accepted

- Review-only gate (consistent with generator and Bronze reviews).
- Severity classification and targeted fix recommendations.
- Cross-layer trace (not module-isolated).

#### Changed

- Nothing in code immediately after this prompt.

#### Rejected

- **No explicit user “reject”** of individual findings.
- **Deferred fixes:** Session continued with README (next prompt) documenting **Known Limitations** rather than implementing all fixes — not the same as rejecting findings.

#### Reasoning

- E2E review validates the whole system before claiming production-ready.
- Honest README limitations align with review without silent omission.

### VALIDATION

- Findings cross-checked against `README.md` Known Limitations section (created in next prompt).
- Human can re-verify each finding by running pipeline + pytest locally.

---

## Iteration — Environment: pytest Not Runnable in Agent Shell

**When:** Throughout session (notably Prompts 9, 17, 27)

### What happened

Agent attempted `python` / `pytest` in Cursor shell; environment often lacked Java or Python on PATH. Agent **documented** manual commands instead of claiming tests passed.

### MY EVALUATION

#### Accepted

- Tests written in repo regardless of agent shell limits.

#### Changed

- Validation responsibility shifted to human: `python -m pytest tests/ -v`.

#### Rejected

- Claiming green CI without execution evidence.

#### Reasoning

- Accurate test status matters for assessment integrity.

### VALIDATION

Human runs pytest with Java + PySpark installed (`README.md` Quick Start).

---

## Iteration — Order Duplicate Count (Data Generation)

**Parent prompt:** Data generation Prompt 1  
**Files:** `src/data_generation/generate_sample_data.py`

| Issue | Fix |
|-------|-----|
| Ambiguous “20 duplicate order_id records” | `ORDER_DUPLICATE_ROW_COUNT = 20`; 20 keys × 2 appearances |

**Validation:** `tests/quality_expectations.py` → `DUPLICATE_ORDER_ROW_COUNT = 40` under all-members-flagged Silver semantics.

---

## Iteration — Bronze Shared batch_id

**Parent prompt:** Bronze Prompt 1  
**Files:** `src/bronze/ingest_all.py`

| Issue | Fix |
|-------|-----|
| Inconsistent batch IDs across entities in one run | Pin single `batch_id` for customers/products/orders ingest |

**Validation:** Audit table rows share `batch_id` when using `ingest_all.py`.

---

## Iteration — Silver Test Imports (Completeness)

**Parent prompt:** Silver Prompt 1  
**Files:** `tests/test_quality_completeness.py`, `tests/conftest.py`

| Issue | Fix |
|-------|-----|
| Cannot import `01_quality_completeness.py` as normal module | `importlib` loader pattern |

**Validation:** `pytest tests/test_quality_completeness.py` (local).

---

## Iteration — RI After Type Validation

**Parent prompt:** Silver Prompt 6 (type validation resend)  
**Files:** `src/silver/04_quality_referential_integrity.py`

| Issue | Fix |
|-------|-----|
| `dq_is_valid` ignored `dq_type_business_pass` when module 04 ran after 03 | Initialize and combine `dq_type_business_*` in RI path |

**Validation:** Sequential module run order in `create_silver_tables.py` / `run_pipeline.py`; RI tests pass.

---

## Iteration — Business Logic Rule Overlap

**Parent prompt:** Silver Prompt 7  
**Files:** `src/silver/05_quality_business_logic.py`

| Issue | Fix |
|-------|-----|
| Prompt listed qty/price/status rules in business module; already in module 03 | `TYPE_RULES_OWNED_BY_MODULE_03`; module 05 = cross-field only |
| Tolerance unit test edge case | Simplified to exact decimal match test |

**Validation:** `test_type_rules_not_duplicated_in_business_logic` in `tests/test_quality_business_logic.py`.

---

## Iteration — Silver Consolidation Rewrite

**Parent prompt:** Silver Prompt 8  
**Files:** `src/silver/create_silver_tables.py`

| Issue | Fix |
|-------|-----|
| First orchestrator draft incomplete | Full rewrite with `consolidate_quality_model()`, `silver_dq_rules.py` |

**Validation:** `tests/test_create_silver_tables.py`, `test_silver_quality.py`.

---

## Iteration — Pipeline Stepped Execution

**Parent prompt:** Documentation Prompt 6 (`run_pipeline.py`) — see `documentation.md`  
**Files:** `src/run_pipeline.py`, Silver/Gold orchestrators

| Issue | Fix |
|-------|-----|
| Need 15-step sequence with fail-fast | Refactored orchestrators to support stepped invocation from single entry point |

**Validation:** `tests/test_run_pipeline.py` (config and step ordering).

---

## Review Findings Still Open (Session End)

These were **identified** in Prompt 1 above but **not fixed** in the transcript (documented in `README.md` Known Limitations):

1. Bronze header validation with explicit schema (CRITICAL)
2. Gold `valid_products` / product_id dedup in SQL (HIGH)
3. Gold SQL ignores `PipelineConfig` table name overrides (HIGH)
4. `DASHBOARD_GUIDE.md` §6.5 Silver reconciliation query (HIGH)
5. Segmentation revenue reconciliation in orchestrator (HIGH)
6. `silver_dq_metrics` append behavior (HIGH)
7. `data-quality-strategy.md` duplicate order wording vs 40-row test expectation (MEDIUM)

**Evidence:** No follow-up “fix finding X” prompts after E2E review; README lists limitations instead.
