# Databricks notebook source
%run ./00.config

# COMMAND ----------

dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from pyspark.sql.functions import max
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# snake_case
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# clean
df = df.dropna()

# dedup
window = Window.partitionBy("order_id").orderBy(col("ingestion_time").desc())

df = df.withColumn("row_num", row_number().over(window)) \
       .filter("row_num = 1") \
       .drop("row_num")

# COMMAND ----------

if load_type == "incremental":
    if not spark.catalog.tableExists(SILVER_TABLE):
        df.write.format("delta").saveAsTable(SILVER_TABLE)
    else:
        delta_table = DeltaTable.forName(spark, SILVER_TABLE)

        delta_table.alias("target").merge(
            df.alias("source"),
            SILVER_MERGE_KEY
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

else:
    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
