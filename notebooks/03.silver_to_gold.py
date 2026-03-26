# Databricks notebook source
%run ./00.config

# COMMAND ----------

dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, sha2, concat_ws

# COMMAND ----------

# DBTITLE 1,Dim
df = spark.table(SILVER_TABLE)

dim = df.select("product", "category").dropDuplicates()

dim = dim.withColumn("product_id", sha2(concat_ws("|", col("product"), col("category")), 256))

dim.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM)

# COMMAND ----------

# DBTITLE 1,Fact
df = spark.table(SILVER_TABLE)
dim = spark.table(GOLD_DIM)

fact = df.join(dim, ["product", "category"], "left")

fact = fact.withColumn("revenue", col("price") * col("quantity"))

if load_type == "incremental":
    if not spark.catalog.tableExists(GOLD_FACT):
        fact.write.format("delta").saveAsTable(GOLD_FACT)
    else:
        delta_table = DeltaTable.forName(spark, GOLD_FACT)

        delta_table.alias("target").merge(
            fact.alias("source"),
            FACT_MERGE_KEY
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

else:
    fact.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACT)
