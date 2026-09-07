# Databricks Git Setup — Import from GitHub

Use this guide to **import** the project from GitHub into Databricks **Repos** and **sync** when you push new commits.

**Repository:** https://github.com/RAHUL9868/databricks-medallion-pipeline  
**Default branch:** `main`

---

## 1. Prerequisites

| Item | Requirement |
|------|-------------|
| Databricks workspace | Community Edition or paid workspace |
| GitHub access | Read access to `RAHUL9868/databricks-medallion-pipeline` |
| Git integration | GitHub account linked in Databricks (Settings → Linked accounts) |

---

## 2. Add the Repo in Databricks (first time)

1. Open your **Databricks workspace** in the browser.
2. In the left sidebar, click **Workspace** → **Repos** (or **Repos** directly, depending on UI version).
3. Click **Add Repo** (or **Create** → **Repo**).
4. Fill in:

   | Field | Value |
   |-------|--------|
   | **Git repository URL** | `https://github.com/RAHUL9868/databricks-medallion-pipeline.git` |
   | **Git provider** | GitHub |
   | **Repository name** | `databricks-medallion-pipeline` (or your choice) |
   | **Branch** | `main` |
   | **Path** | Default under `/Repos/<your-email>/databricks-medallion-pipeline` |

5. Click **Create Repo**. Databricks clones the repository into the workspace.

### Community Edition notes

- Repos are supported on CE with a linked GitHub account.
- Use a **single-node all-purpose cluster** for PySpark jobs.
- Unity Catalog may be unavailable — leave `PIPELINE_CATALOG` unset.

---

## 3. Sync / Export New Git Changes into Databricks

After you **push** commits to GitHub from your laptop (or Cursor), refresh Databricks:

### Option A — Pull in the Repos UI

1. Go to **Repos** → open `databricks-medallion-pipeline`.
2. Click **Pull** (or **Git** → **Pull**) to fetch the latest `main`.
3. Confirm the commit hash matches GitHub (e.g. `0826d0d` or newer).

### Option B — Notebook git commands (if enabled)

On a cluster notebook attached to the repo path:

```python
%sh
cd /Workspace/Repos/<your-user>/databricks-medallion-pipeline
git fetch origin
git checkout main
git pull origin main
git log -1 --oneline
```

Replace `<your-user>` with your Databricks Repos folder name (often your email).

### Option C — Re-clone (if pull fails)

Delete the Repo in the UI and **Add Repo** again with the same GitHub URL and branch `main`.

---

## 4. Run the Pipeline from the Repo

### A. Upload seed CSVs to DBFS (one time per reset)

Generate locally or on the cluster, then place files at the default path:

```bash
# Local (laptop)
python src/data_generation/generate_sample_data.py --output-dir ./data --seed 42

# Databricks CLI (from laptop, after configuring CLI)
databricks fs mkdirs dbfs:/FileStore/ecommerce/data
databricks fs cp ./data/customers.csv dbfs:/FileStore/ecommerce/data/customers.csv
databricks fs cp ./data/products.csv  dbfs:/FileStore/ecommerce/data/products.csv
databricks fs cp ./data/orders.csv    dbfs:/FileStore/ecommerce/data/orders.csv
```

### B. Run full pipeline on a cluster

**Notebook (recommended):** open `notebooks/run_full_pipeline.ipynb` from the Repo, attach an all-purpose cluster, set widgets, and run all cells.

**Notebook** (attach all-purpose cluster):

```python
import sys
REPO = "/Workspace/Repos/<your-user>/databricks-medallion-pipeline"
sys.path.insert(0, f"{REPO}/src")

from config.pipeline_config import load_config
from bronze.bronze_ingest import ingest_all_entities, get_spark
from silver.create_silver_tables import run_create_silver_tables
from gold.create_gold_tables import run_create_gold_tables

config = load_config(
    schema_name="ecommerce",
    source_base_path="dbfs:/FileStore/ecommerce/data",
)
spark = get_spark()

ingest_all_entities(spark=spark, config=config)
run_create_silver_tables(spark=spark, config=config)
run_create_gold_tables(spark=spark, config=config)
```

**Shell on cluster** (Repos terminal or `%sh`):

```bash
cd /Workspace/Repos/<your-user>/databricks-medallion-pipeline
pip install -r requirements-dev.txt   # if PySpark deps not on cluster image
python src/run_pipeline.py \
  --schema ecommerce \
  --source-base-path dbfs:/FileStore/ecommerce/data
```

### C. SQL Dashboard

After Gold tables exist, follow `src/dashboard/DASHBOARD_GUIDE.md` using a **SQL warehouse**.

---

## 5. Workflow: Local Git → Databricks

```
┌─────────────────┐     git push      ┌──────────────────────────┐
│ Cursor / local  │ ────────────────► │ GitHub (main)            │
│ git commit      │                   │ RAHUL9868/databricks-... │
└─────────────────┘                   └────────────┬─────────────┘
                                                   │ Pull / sync
                                                   ▼
                                        ┌──────────────────────────┐
                                        │ Databricks Repos         │
                                        │ /Workspace/Repos/.../    │
                                        └────────────┬─────────────┘
                                                     │ run_pipeline.py
                                                     ▼
                                        ┌──────────────────────────┐
                                        │ Delta tables (ecommerce) │
                                        │ + SQL Dashboard          │
                                        └──────────────────────────┘
```

1. Edit code locally → `git add` → `git commit` → `git push origin main`
2. In Databricks Repos → **Pull**
3. Re-run pipeline on cluster (or affected layer only)

---

## 6. Push Changes from Databricks Back to GitHub (optional)

If you edit files **inside** a Databricks Repo:

1. Open the repo → **Git** → review changed files.
2. Commit with a message → **Push** to `main` (requires write access to GitHub).

For this assessment project, **local/Cursor → GitHub → Databricks Pull** is the recommended flow so tests and reviews stay on your machine.

---

## 7. Verify Import Succeeded

In a SQL warehouse or notebook:

```sql
-- After pipeline run
SHOW TABLES IN ecommerce;

SELECT COUNT(*) FROM ecommerce.bronze_orders;   -- expect 100000
SELECT COUNT(*) FROM ecommerce.silver_orders;   -- expect 100000
SELECT COUNT(*) FROM ecommerce.gold_sales_by_product;  -- expect 500
```

In a notebook:

```python
import os
repo = "/Workspace/Repos/<your-user>/databricks-medallion-pipeline"
assert os.path.exists(f"{repo}/src/run_pipeline.py")
assert os.path.exists(f"{repo}/README.md")
print("Repo files present")
```

---

## 8. Troubleshooting

| Issue | Fix |
|-------|-----|
| Cannot add Repo | Link GitHub under **User Settings** → **Linked accounts** |
| Pull shows no changes | Confirm push reached GitHub; check branch is `main` |
| `ModuleNotFoundError: config` | `sys.path.insert(0, ".../src")` or run from repo root with `PYTHONPATH=src` |
| `Source file not found` | Upload CSVs to `dbfs:/FileStore/ecommerce/data` |
| Permission denied on push from Databricks | Use PAT with repo scope or push from local git only |

---

## Related docs

| Document | Purpose |
|----------|---------|
| `database/setup-notes.md` | Full pipeline execution on Databricks |
| `database/seed-data-notes.md` | CSV generation and DBFS upload |
| `README.md` | Quick start and configuration |
