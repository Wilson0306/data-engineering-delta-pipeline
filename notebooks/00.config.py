# Databricks notebook source
# DBTITLE 1, Pipeline Configuration
# =============================================================================
# 00.config.py
# Shared configuration for the Delta Lake Medallion Pipeline.
# Run this first, or %run it from each notebook:
#   %run ./00.config
# =============================================================================

# COMMAND ----------

# ---------------- Catalog / Schema ----------------
CATALOG   = "wilson"
SCHEMA_RAW    = f"{CATALOG}.raw"
SCHEMA_BRONZE = f"{CATALOG}.bronze"
SCHEMA_SILVER = f"{CATALOG}.silver"
SCHEMA_GOLD   = f"{CATALOG}.gold"

# ---------------- Table Names ----------------
RAW_TABLE    = f"{SCHEMA_RAW}.sales"
BRONZE_TABLE = f"{SCHEMA_BRONZE}.sales"
SILVER_TABLE = f"{SCHEMA_SILVER}.sales"
GOLD_DIM     = f"{SCHEMA_GOLD}.dim_product"
GOLD_FACT    = f"{SCHEMA_GOLD}.fact_sales"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.sales"

# ---------------- Merge Keys ----------------
SILVER_MERGE_KEY  = "target.order_id = source.order_id"
FACT_MERGE_KEY    = "target.order_id = source.order_id"

# ---------------- Partition Column ----------------
BRONZE_PARTITION_COL = "order_date"

# ---------------- Watermark Column ----------------
# Used to filter new records from Raw during incremental loads.
# Points to Bronze (not Silver) so the watermark stays within the same layer.
WATERMARK_COL = "ingestion_time"

# COMMAND ----------

print("Pipeline config loaded:")
print(f"  RAW    -> {RAW_TABLE}")
print(f"  BRONZE -> {BRONZE_TABLE}")
print(f"  SILVER -> {SILVER_TABLE}")
print(f"  DIM    -> {GOLD_DIM}")
print(f"  FACT   -> {GOLD_FACT}")
print(f"  QUARANTINE -> {QUARANTINE_TABLE}")
