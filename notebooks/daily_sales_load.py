# Databricks notebook source
# Daily sales load entry point. The logic lives in src/tamarack_sales/daily_sales.py (unit tested);
# this notebook only reads the job parameters and calls it.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from tamarack_sales.daily_sales import load_daily_sales

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("from_date", "2026-10-01")

catalog = dbutils.widgets.get("catalog")
if not catalog:
    raise ValueError("The catalog job parameter is required; it is set per target by the bundle.")

load_daily_sales(spark, catalog, dbutils.widgets.get("from_date"))
