# Tamarack sales platform

Tamarack Home Supply's sales data on Databricks, migrated from Oracle.

- `notebooks/daily_sales_load.py` rebuilds `gold.daily_sales` from the silver tables.
- `sql/reconcile_daily_sales.sql` compares the result with the Oracle extract.

Catalogs: `dev_tamarack`, `stg_tamarack`, `prod_tamarack`, each with bronze, silver and gold schemas.