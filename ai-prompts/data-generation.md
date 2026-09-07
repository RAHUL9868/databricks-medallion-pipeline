# AI Prompts — Data Generation

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

This file documents the two user prompts for sample data generation. Verbatim prompts are taken from the chat transcript; evaluation is evidence-based (no fabricated accept/reject claims).

---

## Prompt 1 — Implement Sample Data Generator

**Timestamp:** 12:15 PM

### PROMPT SENT

```
Implement the sample data generator described in the requirements.

Create:

src/data_generation/generate_sample_data.py

and:

src/data_generation/DATA_GENERATION_NOTES.md

Requirements:

CUSTOMERS:
- approximately 10,000 rows
- realistic names
- realistic email addresses
- countries
- signup dates
- customer segments
- lifetime values

PRODUCTS:
- approximately 500 rows
- realistic product names
- categories
- price
- cost
- stock quantity
- reorder level

ORDERS:
- approximately 100,000 rows
- realistic customers/products
- realistic order dates
- quantities
- unit prices
- total amount
- order status
- payment dates

INTENTIONAL ISSUES:

Customers:
- exactly 50 NULL email records
- exactly 10 duplicate customer_id records

Orders:
- exactly 100 NULL customer_id records
- exactly 200 NULL product_id records
- exactly 50 invalid customer_id records
- exactly 30 invalid product_id records
- exactly 20 duplicate order_id records

Important:

1. Use a fixed random seed.
2. Make the generator reproducible.
3. Do not accidentally introduce additional intentional primary-key duplicates.
4. Clearly distinguish naturally generated records from intentionally corrupted records.
5. Document the corruption process.
6. Validate generated issue counts before writing CSVs.
7. Fail fast if the expected issue counts are incorrect.
8. Keep data generation independent from Databricks.
9. Prefer standard Python libraries and commonly available packages.
10. Make output directory configurable.
11. Avoid hard-coded absolute local paths.
12. Add a main() function.
13. Add useful logging.
14. Add input/output validation.

Before finalizing the code, inspect it for:
- duplicate issue counting problems
- accidental extra NULLs
- accidental invalid foreign keys
- inconsistent total_amount calculations
- invalid date ranges

Also create a small validation section in DATA_GENERATION_NOTES.md showing how the generated files can be verified.
```

### AI RESPONSE SUMMARY

The agent reviewed the repo, then implemented `generate_sample_data.py` with disjoint corruption pools, pre-write validation, and seed `42`. During implementation it **self-corrected** order duplicate semantics: `20` duplicate keys produce **20 appended rows** (40 rows in duplicate groups when all members are counted). It also created `DATA_GENERATION_NOTES.md` with corruption steps and verification guidance.

### MY EVALUATION

#### Accepted

- Fixed random seed and fail-fast count validation before CSV write.
- Disjoint corruption pools (no row receives multiple intentional defects).
- Configurable `--output-dir` with no hard-coded workspace paths.
- `DATA_GENERATION_NOTES.md` documenting corruption order and verification.

**Evidence:** No follow-up prompt asked to rewrite the generator; later Silver tests (Prompt 17) assumed seed=42 counts from this module.

#### Changed

- **Order duplicate row count:** Agent initially aligned to “20 duplicate order_id records” as 10 keys × 2 rows; corrected to **20 appended duplicate rows** (`ORDER_DUPLICATE_ROW_COUNT = 20`, 20 keys appearing twice → 40 rows in duplicate groups under “flag all members” Silver semantics).

**Evidence:** Constants in `src/data_generation/generate_sample_data.py`; assistant self-correction noted in transcript before Prompt 2 (review).

#### Rejected

- No explicit user rejection recorded in the transcript for this deliverable.

#### Reasoning

- Pre-write validation enforces assignment counts at generation time — critical because downstream Silver tests depend on exact intentional issue volumes.
- Disjoint pools prevent accidental compound defects that would break deterministic test expectations.
- Order duplicate semantics remain **ambiguous** in assignment wording (“20 duplicate order_id rows” vs 20 keys × 2 rows); the agent chose an interpretation and Silver later adopted “flag all members in duplicate group” (40 failed uniqueness rows).

### VALIDATION

| Check | How |
|-------|-----|
| Issue counts | `generate_sample_data.py` pre-write assertions; `tests/test_data_generation.py` |
| Reproducibility | `--seed 42` (default); rerun produces same counts |
| Agent environment | Generator is pure Python; agent could not always run `python` locally — human run: `python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42` |
| Downstream | `tests/quality_expectations.py` mirrors generator constants |

---

## Prompt 2 — Senior DE Review of Generator (Review-Only)

**Timestamp:** 12:18 PM

### PROMPT SENT

```
Review the data generation implementation you just created as a senior Data Engineer.

Do NOT immediately rewrite it.

First perform a critical review.

Check:

1. Are all required issue counts exactly correct?
2. Could duplicate customer IDs cause more than the intended number of duplicate rows?
3. Could duplicate order IDs cause more than the intended number of duplicate rows?
4. Are invalid foreign keys guaranteed to actually be invalid?
5. Are NULL counts deterministic?
6. Are all required columns present?
7. Are data types appropriate?
8. Are order totals mathematically consistent?
9. Are payment dates logically consistent with order status?
10. Could rerunning the generator produce different results?
11. Is the code maintainable?
12. Is the code unnecessarily complex?
13. Are there hidden assumptions?

Provide:
- findings
- severity
- recommended changes

Only after the review, propose a corrected version if necessary.
```

### AI RESPONSE SUMMARY

The agent produced a structured **Critical Review** of `generate_sample_data.py` and `DATA_GENERATION_NOTES.md` without modifying code. It rated the generator structurally sound (disjoint pools, pre-write validation) and flagged **spec ambiguity on order duplicates** (assignment wording vs `data-quality-strategy.md` vs Silver “all members flagged” = 40 rows). It also noted minor documentation gaps and unused audit fields.

### MY EVALUATION

#### Accepted

- Review-only workflow: findings first, no immediate rewrite (as requested).
- Checklist coverage across counts, FK validity, determinism, and maintainability.

#### Changed

- Nothing changed in code after this prompt — **by design** (user instruction).

#### Rejected

- **No transcript evidence** that recommended review fixes were explicitly rejected.
- **Deferred, not rejected:** No follow-up prompt in the session asked the agent to apply review recommendations; generator left as delivered in Prompt 1.

#### Reasoning

- Review gate before refactor avoids blind acceptance of first-pass AI output.
- Order duplicate ambiguity was surfaced early; later Silver tests codified **40** duplicate-order uniqueness failures (`DUPLICATE_ORDER_ROW_COUNT = 40` in `quality_expectations.py`) without regenerating CSVs.

### VALIDATION

| Check | How |
|-------|-----|
| Review completeness | 13 numbered review questions addressed in chat |
| Code unchanged | Generator files unchanged after Prompt 2 (no fix prompt) |
| Cross-doc consistency | E2E review (see `debugging.md`) later noted `data-quality-strategy.md` duplicate-order wording drift |

---

## Iteration Note — Order Duplicate Semantics (During Prompt 1)

This was an **agent self-correction during implementation**, not a separate user prompt.

| Before | After |
|--------|-------|
| Ambiguous 10 vs 20 duplicate order rows | `ORDER_DUPLICATE_ROW_COUNT = 20`; comment documents 20 keys × 2 appearances |

**Validation:** `tests/quality_expectations.py` → `DUPLICATE_ORDER_ROW_COUNT = 40` for Silver uniqueness (all group members flagged).
