# Databricks notebook source
# DBTITLE 1,Cell 1
%run ./00.config

# COMMAND ----------

dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, max

# COMMAND ----------

if load_type == "incremental" and spark.catalog.tableExists(BRONZE_TABLE):

    last_time = spark.table(BRONZE_TABLE) \
                     .select(max(WATERMARK_COL)) \
                     .collect()[0][0]
    df = spark.table(RAW_TABLE)
    df = df.filter(df.ingestion_time > last_time)
    df_bronze = df.withColumn(WATERMARK_COL, current_timestamp())
    df_bronze.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
else:
    df = spark.table(RAW_TABLE)
    df_bronze = df.withColumn(WATERMARK_COL, current_timestamp())
    df_bronze.write.format("delta").mode("overwrite").partitionBy(BRONZE_PARTITION_COL).saveAsTable(BRONZE_TABLE)
