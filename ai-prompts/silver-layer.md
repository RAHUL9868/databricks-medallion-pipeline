# AI Prompts — Silver Layer

**Session:** `96f9a367-722e-481c-8fec-a8d5921f19dc`  
**Timestamps:** Monday, Sep 7, 2026 (UTC+5:30)

Nine substantive prompts (one duplicate resend, one interrupted/resend pair). Prompt numbers match canonical numbering in `prompt-extraction.md`.

---

## Prompt 1 — Silver Completeness Validation

**Timestamp:** 12:28 PM

### PROMPT SENT

```
Implement Silver completeness validation.

Create:

src/silver/01_quality_completeness.py

The implementation must:

1. Read Bronze customers and orders.
2. Validate critical fields:
   - customers.email
   - orders.customer_id
   - orders.product_id
3. Identify row-level failures.
4. Do not delete failed rows.
5. Add a quality result/status.
6. Capture meaningful error details.
7. Produce pass/fail counts.
8. Produce pass/fail percentages.
9. Persist results as Silver Delta tables.
10. Make the implementation reusable/configurable.
11. Avoid collecting large datasets to the driver.
12. Use Spark-native operations.
13. Handle NULLs correctly.
14. Include logging.

Design the quality columns before implementation.

Also create tests that prove:
- 50 NULL customer emails are detected
- 100 NULL order customer IDs are detected
- 200 NULL order product IDs are detected

The tests should fail if these intentional issues are not detected.
```

### AI RESPONSE SUMMARY

Agent designed `dq_*` columns (`dq_completeness_pass`, `dq_failed_rules`, `dq_is_valid`, etc.), added `src/silver/silver_common.py`, extended `pipeline_config.py`, and implemented `01_quality_completeness.py` with CLI. Created `tests/conftest.py` and `tests/test_quality_completeness.py`.

**Iteration:** Fixed test imports for the numbered module filename (`01_quality_completeness.py`) via `importlib`; removed unused import.

### MY EVALUATION

#### Accepted

- Quality column design documented before coding.
- Flag-not-delete semantics.
- Spark-native NULL/blank detection (no driver collect).
- Metrics to `silver_dq_metrics` with `check_category = 'completeness'`.

#### Changed

- Test loading pattern for `01_*` module name (agent self-fix after test structure issues).

#### Rejected

- None recorded.

#### Reasoning

- Established the `dq_*` contract used by all later Silver modules.

### VALIDATION

`tests/test_quality_completeness.py`; small fixtures + rule ID assertions.

---

## Prompt 2 — Silver Uniqueness Validation

**Timestamp:** 12:31 PM

### PROMPT SENT

```
Implement:

src/silver/02_quality_uniqueness.py

Validate:

CUSTOMERS:
- customer_id uniqueness

ORDERS:
- order_id uniqueness

Requirements:

1. Detect duplicate business keys.
2. Flag duplicate rows rather than silently deleting them.
3. Preserve duplicate records for traceability.
4. Produce row-level quality information.
5. Produce aggregate metrics.
6. Produce duplicate counts.
7. Produce pass percentages.
8. Make duplicate identification deterministic.
9. Avoid driver-side collection.
10. Support reruns safely.

Important:
Think carefully about how duplicate rows are flagged.

If a key occurs twice, decide whether:
- both rows are marked as duplicate, or
- only subsequent occurrences are marked.

Choose the approach that provides the clearest data quality semantics and document the decision.

Also create tests proving:
- customer duplicate IDs are detected
- order duplicate IDs are detected
```

### AI RESPONSE SUMMARY

Implemented `02_quality_uniqueness.py` with window `count(*) OVER (PARTITION BY key)`. **Decision: flag all rows in a duplicate key group** (10 customer keys → 20 failed rows; 20 order keys → 40 failed rows). Added `tests/test_quality_uniqueness.py`.

### MY EVALUATION

#### Accepted

- “All members flagged” semantics (documented in module docstring).
- NULL/blank keys excluded from uniqueness (completeness owns NULLs).

#### Changed

- None after delivery.

#### Rejected

- None recorded.

#### Reasoning

- Avoids arbitrary “winner” row in duplicate groups; aligns with auditable DQ metrics.

### VALIDATION

`tests/test_quality_uniqueness.py`; counts align with `quality_expectations.py`.

---

## Prompt 3 — Uniqueness Prompt Resend (Duplicate)

**Timestamp:** 12:36 PM

### PROMPT SENT

*(Identical to Prompt 2.)*

### AI RESPONSE SUMMARY

Agent confirmed `02_quality_uniqueness.py` already existed and summarized the duplicate-flagging decision — **no new code**.

### MY EVALUATION

#### Accepted

- Existing implementation as sufficient.

#### Changed

- N/A (resend only).

#### Rejected

- N/A — not a rejection; likely interrupted/resend in UI.

#### Reasoning

- Duplicate prompt did not indicate dissatisfaction with prior output.

### VALIDATION

No additional validation; prior tests unchanged.

---

## Prompt 4 — Referential Integrity

**Timestamp:** 12:38 PM

### PROMPT SENT

```
Implement:

src/silver/04_quality_referential_integrity.py

Validate:

orders.customer_id → customers.customer_id

orders.product_id → products.product_id

Requirements:

1. Detect NULL foreign keys.
2. Detect non-existent customer IDs.
3. Detect non-existent product IDs.
4. Distinguish NULL failures from orphan-key failures.
5. Preserve invalid records.
6. Add quality failure information.
7. Generate metrics.
8. Use Spark joins efficiently.
9. Avoid driver-side collections of entire reference tables.
10. Handle duplicate customer IDs in the source appropriately.
11. Document how referential integrity is evaluated when the customer source itself contains duplicate IDs.
12. Make table names configurable.
13. Include tests.

Expected intentional issues:
- 50 invalid customer IDs
- 30 invalid product IDs

The implementation must prove these issues are caught.
```

### AI RESPONSE SUMMARY

Implemented RI module with left-anti join pattern; NULL FKs attributed to completeness, not RI rule IDs. Parent keys use `distinct()` so duplicate customer IDs do not break valid references. **Iteration:** entity key extraction fix in RI module.

### MY EVALUATION

#### Accepted

- NULL vs orphan separation.
- `distinct()` on parent keys for duplicate customer_id scenario.

#### Changed

- Entity key extraction fix during implementation.

#### Rejected

- None recorded.

#### Reasoning

- Prevents double-counting NULL FKs in RI metrics.

### VALIDATION

`tests/test_quality_referential_integrity.py`; expects 50 / 30 orphan counts.

---

## Prompt 5 — Type Validation (Interrupted)

**Timestamp:** 12:39 PM

### PROMPT SENT

```
Implement:

src/silver/03_quality_type_validation.py

Create validation for expected data types and valid domain values.

Validate at minimum:

customers:
- customer_id
- signup_date
- customer_segment
- lifetime_value

orders:
- order_id
- customer_id
- order_date
- product_id
- quantity
- unit_price
- total_amount
- order_status
- payment_date

products:
- product_id
- price
- cost
- stock_quantity
- reorder_level

Validate domain values such as:

customer_segment:
Premium / Standard / Basic

order_status:
Pending / Completed / Cancelled

Also consider logical numeric constraints:
- quantity > 0
- price >= 0
- cost >= 0
- stock_quantity >= 0
- reorder_level >= 0

Do not invent arbitrary business rules unless documented as assumptions.

Flag failures instead of deleting records.

Create row-level error information and aggregate quality metrics.

Create tests for invalid type/domain/business values.

Explain any assumption you make.
```

### AI RESPONSE SUMMARY

**No assistant completion captured** (interrupted). User resent as Prompt 6.

### MY EVALUATION

#### Accepted / Changed / Rejected

- N/A — no deliverable from this turn.

### VALIDATION

N/A

---

## Prompt 6 — Type Validation (Resend — Completed)

**Timestamp:** 12:39 PM

### PROMPT SENT

*(Identical to Prompt 5.)*

### AI RESPONSE SUMMARY

Implemented `03_quality_type_validation.py` with type/domain/numeric rules and tests. **Cross-module iteration:** updated `04_quality_referential_integrity.py` to initialize `dq_type_business_pass` / `dq_type_business_errors` when module 04 runs after module 03, so `dq_is_valid` stays consistent.

### MY EVALUATION

#### Accepted

- Full rule set for three entities.
- Documented assumptions for numeric constraints.

#### Changed

- RI module patched for downstream `dq_type_business_*` columns (integration fix).

#### Rejected

- None recorded.

#### Reasoning

- Silver modules run sequentially; later modules must not leave `dq_is_valid` inconsistent.

### VALIDATION

`tests/test_quality_type_validation.py`; RI tests still pass after cross-module update.

---

## Prompt 7 — Business Logic Validation

**Timestamp:** 12:42 PM

### PROMPT SENT

```
Implement:

src/silver/05_quality_business_logic.py

Validate logical business relationships including:

1. quantity must be positive
2. unit_price must be non-negative
3. total_amount should be consistent with quantity * unit_price within an appropriate decimal precision
4. order_status must be a valid domain value
5. payment_date should be logically consistent with order status
6. signup_date should not be in the future
7. product cost should not exceed price unless explicitly allowed/documented

Do not silently modify source values.

Flag invalid records.

For each rule:
- document the rationale
- document the failure condition
- generate row-level quality information
- generate aggregate metrics

Create meaningful tests.

Clearly distinguish:
- assignment-required rules
- reasonable engineering assumptions
```

### AI RESPONSE SUMMARY

Implemented `05_quality_business_logic.py`. **Rule ownership split:** rules 1–4 and signup_date already in module 03; module 05 owns **cross-field** rules only (`TYPE_RULES_OWNED_BY_MODULE_03` constant). **Iteration:** simplified tolerance unit test to exact decimal match after parsing behavior clarified.

### MY EVALUATION

#### Accepted

- Cross-field rules: `total_amount` vs `quantity * unit_price`, payment_date vs status, cost vs price.
- `TYPE_RULES_OWNED_BY_MODULE_03` prevents double-flagging.

#### Changed

- Rules 1–4 remain in module 03 (prompt listed them in business module — agent split by category).
- Tolerance test simplified in `tests/test_quality_business_logic.py`.

#### Rejected

- None recorded.

#### Reasoning

- Type/domain vs cross-field separation reduces contradictory `dq_failed_rules`.

### VALIDATION

`tests/test_quality_business_logic.py`; overlap test asserts no duplicate rule IDs between modules.

---

## Prompt 8 — Create Silver Tables (Consolidation)

**Timestamp:** 12:44 PM

### PROMPT SENT

```
Now design and implement:

src/silver/create_silver_tables.py

The goal is to create clean, reusable Silver datasets from Bronze data.

Silver should provide:

1. customers_silver
2. orders_silver
3. products_silver

and a quality metrics/reporting dataset.

Requirements:

- Preserve traceability to Bronze.
- Preserve invalid records where required for auditability.
- Clearly expose quality status.
- Clearly expose quality error/reason information.
- Avoid multiple contradictory quality columns.
- Consolidate results from completeness, uniqueness, referential integrity, type validation, and business validation.
- Ensure the final Silver layer has a consistent quality model.
- Make it possible to distinguish:
  - valid records
  - invalid records
  - records with multiple failures

Decide whether invalid records should remain in the primary Silver tables with a status, or whether a separate quarantine table is useful.

The assignment requires bad rows not to be deleted, so ensure they remain traceable.

Implement a maintainable approach and explain the design decision.

Also create a final quality metrics table containing:
- dataset
- check_name
- total_records
- passed_records
- failed_records
- pass_percentage
- execution_timestamp
```

### AI RESPONSE SUMMARY

Agent chose **invalid rows in primary Silver tables** (no quarantine). Implemented `create_silver_tables.py`, `silver_dq_rules.py`, `consolidate_quality_model()` for authoritative flags. **Iteration:** complete rewrite after incomplete first draft.

**Naming:** User asked `customers_silver`; shipped **`silver_customers`**, `silver_orders`, `silver_products` per `data-model.md`.

### MY EVALUATION

#### Accepted

- `consolidate_quality_model()` — single source of truth for `dq_*` pass flags.
- Primary-table approach with `dq_is_valid` (no silent deletes).
- `silver_dq_metrics` + `silver_dq_report` views.

#### Changed

- Table names: `silver_*` prefix instead of `*_silver` (alignment with data model).
- Full orchestrator rewrite after first incomplete version.

#### Rejected

- Separate quarantine table (agent recommended primary tables + flags; user did not object in transcript).

#### Reasoning

- One table per entity simplifies Gold joins; flags preserve auditability.

### VALIDATION

`tests/test_create_silver_tables.py`; integration via `create_silver_tables` row-count invariants.

---

## Prompt 9 — Silver Test Suite

**Timestamp:** 12:47 PM

### PROMPT SENT

```
Create a meaningful Silver-layer test suite.

The tests must verify that the intentional quality issues are actually detected.

At minimum verify:

CUSTOMERS:
- 50 NULL emails
- 10 duplicate customer IDs

ORDERS:
- 100 NULL customer IDs
- 200 NULL product IDs
- 50 invalid customer IDs
- 30 invalid product IDs
- 20 duplicate order IDs

Also verify:
- quality status is populated
- quality errors are traceable
- quality metrics are correct
- valid records are not incorrectly flagged

Tests should be deterministic.

Prefer small isolated test fixtures where possible in addition to tests against generated sample data.

Document:
- what each test validates
- expected result
- actual result
- pass/fail

Create:

tests/
├── test_data_generation.py
├── test_bronze.py
├── test_silver_quality.py
└── test_gold.py

Do not introduce a heavy testing framework unless necessary.
Use a practical approach compatible with Databricks/PySpark.
```

### AI RESPONSE SUMMARY

Created `tests/quality_expectations.py` (single source of truth for counts), `tests/test_support.py` with `ValidationCheck` helper, `tests/test_silver_quality.py`, and expanded `test_data_generation.py`, `test_bronze.py`, `test_gold.py`. Uses **pytest** + local Spark session.

**Note:** Prompt says “10 duplicate customer IDs” / “20 duplicate order IDs” (keys); tests use **row counts** under all-members-flagged semantics (20 / 40 rows).

### MY EVALUATION

#### Accepted

- `quality_expectations.py` centralizes seed=42 expectations.
- Small fixtures + full-pipeline-style assertions.
- `ValidationCheck` documents expected vs actual pattern.

#### Changed

- Duplicate assertions use **failed row counts** (20 customer, 40 order), not key counts only.

#### Rejected

- None recorded.

#### Reasoning

- Tests must match uniqueness semantics chosen in Prompt 2.

### VALIDATION

`python -m pytest tests/ -v` (human-run; agent environment often lacked Java/Python). `test_silver_quality.py` asserts all intentional issue rule counts.
