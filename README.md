# End-to-End Data Engineering Pipeline (Databricks + Delta Lake)

## Overview

This project implements a production-style data pipeline using Databricks and Delta Lake, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Technologies Used

- Databricks (PySpark)
- Delta Lake
- SQL & Window Functions
- Data Modeling (Fact & Dimension tables)

---

## Architecture

```
Raw  ──►  Bronze  ──►  Silver  ──►  Gold
                                   ├── dim_product
                                   └── fact_sales
```

| Layer  | Description |
|--------|-------------|
| Raw    | Source CSV loaded into a Delta table as-is |
| Bronze | Raw data stamped with `ingestion_time`, partitioned by `order_date` |
| Silver | Cleaned, deduplicated, upserted via MERGE on `order_id` |
| Gold   | `dim_product` with surrogate key + `fact_sales` with revenue |

---

## Notebooks

| File | Purpose |
|------|---------|
| `00.config.py` | All config in one place: catalog, schema, table names, merge keys, partition column, watermark column |
| `01.raw_to_bronze.py` | Reads Raw, adds `ingestion_time`, writes to Bronze |
| `02.bronze_to_silver.py` | Cleans, deduplicates, MERGEs into Silver |
| `03.silver_to_gold.py` | Builds `dim_product` and `fact_sales` in Gold |

### Config variables

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

To change catalog or schema, edit `00.config.py` only — all notebooks pick it up automatically via `%run ./00.config`.

---

## Getting Started

### 1. Cluster requirements

- Databricks Runtime **13.x LTS** or higher
- Single-node cluster is sufficient for this dataset

### 2. Create Unity Catalog schemas

Run once in a Databricks SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS wilson;

CREATE SCHEMA IF NOT EXISTS wilson.raw;
CREATE SCHEMA IF NOT EXISTS wilson.bronze;
CREATE SCHEMA IF NOT EXISTS wilson.silver;
CREATE SCHEMA IF NOT EXISTS wilson.gold;
```

### 3. Load source data into Raw

Upload `data/sales.csv` to DBFS, then run in a notebook:

```python
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/FileStore/tables/sales.csv")

df.write.format("delta").mode("overwrite").saveAsTable("wilson.raw.sales")
```

### 4. Run the pipeline

Run notebooks in order using the `load_type` widget:

**Full load** (first run):

```
00.config          (no widget)
01.raw_to_bronze   load_type = full
02.bronze_to_silver load_type = full
03.silver_to_gold  load_type = full
```

**Incremental load** (subsequent runs):

Load `data/sales_incremental.csv` into `wilson.raw.sales` first (append mode), then:

```
01.raw_to_bronze    load_type = incremental
02.bronze_to_silver load_type = incremental
03.silver_to_gold   load_type = incremental
```

---

## load_type Reference

| Value | Behaviour |
|-------|-----------|
| `full` | Overwrites the target table completely |
| `incremental` | Appends / MERGEs only new or changed records |

---

## Incremental Processing Logic

- **Watermark** — `max(ingestion_time)` from Bronze is used to filter new records from Raw
- **Deduplication** — `row_number()` window on `order_id`, ordered by `ingestion_time DESC` keeps the latest record
- **Upsert** — Delta `MERGE` on `order_id` applied to Silver and `fact_sales`
- **Surrogate key** — `dim_product.product_id` uses `sha2(product | category)` so the same product always gets the same key across reruns

---

## Screenshots

### Bronze
![Bronze](screenshots/bronze.png)

### Silver
![Silver](screenshots/silver.png)

### Gold — Dimension
![gold_dim](screenshots/gold_dim.png)

### Gold — Fact
![Gold_fact](screenshots/gold_fact.png)

---

## Conclusion

This project demonstrates real-world data engineering concepts including incremental pipelines with watermark filtering, Delta MERGE upserts, medallion architecture, and dimensional modeling with deterministic surrogate keys.
