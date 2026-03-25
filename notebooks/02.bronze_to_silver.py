# Databricks notebook source
dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from pyspark.sql.functions import max
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.table("wilson.bronze.sales")

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
    if not spark.catalog.tableExists("wilson.silver.sales"):
        df.write.format("delta").saveAsTable("wilson.silver.sales")
    else:
        delta_table = DeltaTable.forName(spark, "wilson.silver.sales")

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    

else:
    df.write.format("delta").mode("overwrite").saveAsTable("wilson.silver.sales")