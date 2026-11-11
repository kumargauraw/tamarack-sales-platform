# Databricks notebook source
# Daily sales load: rebuilds gold.daily_sales from silver for every day from a start date.
# Replaces the Oracle LOAD_DAILY_SALES procedure. Revenue is rounded per order line, as finance does.

# COMMAND ----------

dbutils.widgets.text("from_date", "2026-10-01")
from_date = dbutils.widgets.get("from_date")

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("dev_tamarack.silver.orders")
lines = spark.table("dev_tamarack.silver.order_lines")

daily = (
    lines.join(orders, "order_id")
    .where((F.col("status") == "COMPLETE") & (F.col("order_date") >= F.lit(from_date)))
    .withColumn(
        "line_revenue",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0)) / 100), 2),
    )
    .groupBy(F.to_date("order_date").alias("sale_date"), "store_id", "product_id")
    .agg(F.sum("quantity").alias("units"), F.sum("line_revenue").cast("decimal(14,2)").alias("revenue"))
    .withColumn("loaded_at", F.current_timestamp())
)

# COMMAND ----------

(
    daily.write.mode("overwrite")
    .option("replaceWhere", f"sale_date >= '{from_date}'")
    .saveAsTable("dev_tamarack.gold.daily_sales")
)