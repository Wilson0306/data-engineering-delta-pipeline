# Databricks notebook source
# DBTITLE 1,Cell 1
dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, max

# COMMAND ----------

if load_type == "incremental" and spark.catalog.tableExists("wilson.bronze.sales"):
    
    last_time = spark.table("wilson.silver.sales") \
                     .select(max("ingestion_time")) \
                     .collect()[0][0]
    df = spark.table("wilson.raw.sales")                 
    df = df.filter(df.ingestion_time > last_time)
    df_bronze = df.withColumn("ingestion_time", current_timestamp())
    df_bronze.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("wilson.bronze.sales")
else:
     df = spark.table("wilson.raw.sales")
     df_bronze = df.withColumn("ingestion_time", current_timestamp())
     df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("order_id").saveAsTable("wilson.bronze.sales")