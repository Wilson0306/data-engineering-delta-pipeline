# End-to-End Data Engineering Pipeline (Databricks + Delta Lake)

> Production-style medallion architecture pipeline with incremental loads, Delta MERGE upserts, watermark filtering, and dimensional modeling — built on Databricks and Unity Catalog.

---

## Architecture

```
Raw (CSV) ──► Bronze ──► Silver ──► Gold
                                   ├── dim_product  (surrogate key via sha2)
                                   └── fact_sales   (revenue, FK join)
```

| Layer  | What happens | Load strategy |
|--------|-------------|---------------|
| **Raw** | Source CSV ingested as-is into a Delta table | Full overwrite |
| **Bronze** | `ingestion_time` timestamp added, partitioned by `order_date` | Full overwrite / Append |
| **Silver** | Cleaned, deduplicated, upserted via Delta MERGE on `order_id` | Full overwrite / MERGE |
| **Gold** | `dim_product` with sha2 surrogate key + `fact_sales` with revenue column | Full overwrite / MERGE |

---

## Design Decisions

| Decision | Why |
|----------|-----|
| **sha2 surrogate key** for `dim_product` | Deterministic — same product always gets the same key across reruns, no identity column drift |
| **Watermark over CDC** | Simpler for batch CSV sources; `max(ingestion_time)` avoids full table scans on each incremental run |
| **Config-only changes** (`00.config.py`) | All table names, merge keys, and partition columns live in one file — pipeline is reusable across dev/prod environments without touching notebook logic |
| **Window-function deduplication** | `row_number() OVER (PARTITION BY order_id ORDER BY ingestion_time DESC)` keeps the latest record without a costly self-join |
| **Delta MERGE in Silver + Gold** | Handles late-arriving data and updates in place — no delete+reinsert, no duplicates |

---

## Technologies Used

- **Databricks** (PySpark, Notebooks, Unity Catalog)
- **Delta Lake** (MERGE, time travel, partitioning)
- **SQL Window Functions** (deduplication, surrogate keys)
- **Medallion Architecture** (Bronze / Silver / Gold)
- **Dimensional Modeling** (Fact + Dimension tables)

---

## Project Structure

```
data-engineering-delta-pipeline/
├── notebooks/
│   ├── 00.config.py           # Single source of truth for all config
│   ├── 01.raw_to_bronze.py    # Ingest raw CSV → Bronze with ingestion_time
│   ├── 02.bronze_to_silver.py # Clean, dedup, MERGE → Silver
│   └── 03.silver_to_gold.py   # Build dim_product + fact_sales → Gold
├── data/
│   ├── sales.csv              # Full load source data
│   └── sales_incremental.csv  # Incremental records (updates + new rows)
└── screenshots/               # Layer output screenshots
```

---

## Getting Started

### Prerequisites

- Databricks workspace with **Runtime 13.x LTS** or higher
- Unity Catalog enabled
- Single-node cluster is sufficient for this dataset

### Step 1 — Create Unity Catalog schemas

Run once in Databricks SQL editor:

> 💡 Replace `wilson` with your own catalog name throughout.

```sql
CREATE CATALOG IF NOT EXISTS wilson;
CREATE SCHEMA IF NOT EXISTS wilson.raw;
CREATE SCHEMA IF NOT EXISTS wilson.bronze;
CREATE SCHEMA IF NOT EXISTS wilson.silver;
CREATE SCHEMA IF NOT EXISTS wilson.gold;
CREATE SCHEMA IF NOT EXISTS wilson.quarantine;
```

### Step 2 — Load source data into Raw

Upload `data/sales.csv` to DBFS, then run in a notebook:

```python
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/FileStore/tables/sales.csv")
df.write.format("delta").mode("overwrite").saveAsTable("wilson.raw.sales")
```

### Step 3 — Run the pipeline in order

**Full load (first run):**

```
00.config           → no widget needed
01.raw_to_bronze    → load_type = full
02.bronze_to_silver → load_type = full
03.silver_to_gold   → load_type = full
```

**Incremental load (subsequent runs):**

First append `data/sales_incremental.csv` to `wilson.raw.sales`, then:

```
01.raw_to_bronze    → load_type = incremental
02.bronze_to_silver → load_type = incremental
03.silver_to_gold   → load_type = incremental
```

---

## Incremental Processing Logic

```
Raw ──[watermark filter]──► Bronze (append)
                               │
                    [dedup via row_number()]
                               │
                    [Delta MERGE on order_id]
                               ▼
                            Silver
                               │
                  [sha2 join + revenue calc]
                               │
              [MERGE into dim_product + fact_sales]
                               ▼
                             Gold
```

- **Watermark** — `max(ingestion_time)` from Bronze filters only new records from Raw
- **Deduplication** — `row_number()` window on `order_id` ordered by `ingestion_time DESC` keeps the latest version of each record
- **Upsert** — Delta `MERGE` on `order_id` handles both inserts and updates atomically
- **Surrogate key** — `sha2(product | category, 256)` is stable across reruns — no auto-increment drift
- **Data quality** — null `order_id`, null `price`, or negative `price` records are quarantined before MERGE

---

## Configuration Reference

All variables live in `00.config.py`. To change catalog or schema, edit that file only — all notebooks pick it up via `%run ./00.config`.

| Variable | Value |
|----------|-------|
| `CATALOG` | `wilson` |
| `RAW_TABLE` | `wilson.raw.sales` |
| `BRONZE_TABLE` | `wilson.bronze.sales` |
| `SILVER_TABLE` | `wilson.silver.sales` |
| `GOLD_DIM` | `wilson.gold.dim_product` |
| `GOLD_FACT` | `wilson.gold.fact_sales` |
| `BRONZE_PARTITION_COL` | `order_date` |
| `WATERMARK_COL` | `ingestion_time` |
| `SILVER_MERGE_KEY` | `target.order_id = source.order_id` |
| `FACT_MERGE_KEY` | `target.order_id = source.order_id` |

---

## Screenshots

| Bronze | Silver |
|--------|--------|
| ![Bronze](screenshots/bronze.png) | ![Silver](screenshots/silver.png) |

| Gold — Dimension | Gold — Fact |
|------------------|-------------|
| ![gold_dim](screenshots/gold_dim.png) | ![Gold_fact](screenshots/gold_fact.png) |

---

## What I'd Add Next

- [ ] Orchestrate notebooks with Databricks Workflows (DAG with dependency ordering)
- [ ] Add schema evolution handling (`mergeSchema` option)
- [ ] Delta table VACUUM and OPTIMIZE jobs for storage management
- [ ] Great Expectations or Databricks lakehouse monitoring for data quality SLAs
- [ ] Parameterise catalog/schema for dev/staging/prod via Databricks job parameters

---

## Author

Built by **Wilson Tony M**
[LinkedIn](https://www.linkedin.com/in/wilson-tony-m-2335983a0) · [GitHub](https://github.com/Wilson0306)

---

*Databricks Certified Data Engineer Associate & Professional*
