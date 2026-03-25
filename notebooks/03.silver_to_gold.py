# Databricks notebook source
dbutils.widgets.text("load_type", "")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id


# COMMAND ----------

# DBTITLE 1,Dim
df = spark.table("wilson.silver.sales")

dim = df.select("product", "category").dropDuplicates()

dim = dim.withColumn("product_id", monotonically_increasing_id())

dim.write.format("delta").mode("overwrite").saveAsTable("wilson.gold.dim_product")

# COMMAND ----------

# DBTITLE 1,Fact
df = spark.table("wilson.silver.sales")
dim = spark.table("wilson.gold.dim_product")

fact = df.join(dim, ["product", "category"], "left")

fact = fact.withColumn("revenue", col("price") * col("quantity"))

target_table = "wilson.gold.fact_sales"

if load_type == "historical":
    fact.write.format("delta").mode("overwrite").saveAsTable(target_table)

else:
    if not spark.catalog.tableExists(target_table):
        fact.write.format("delta").saveAsTable(target_table)
    else:
        delta_table = DeltaTable.forName(spark, target_table)

        delta_table.alias("target").merge(
            fact.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()