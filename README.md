# Tamarack sales platform

Tamarack Home Supply's sales data on Databricks, migrated from Oracle. Packaged as a Declarative Automation Bundle.

- `databricks.yml` – bundle config; targets `dev`, `staging`, `prod` set the `catalog` variable.
- `resources/daily_sales_load.job.yml` – the `daily_sales_load` job (daily 06:00 UTC, serverless).
- `src/tamarack_sales/daily_sales.py` – the transformation logic (tested).
- `notebooks/daily_sales_load.py` – thin entry point that calls `load_daily_sales`.
- `sql/reconcile_daily_sales.sql` – compares the result with the Oracle extract (`:catalog` parameter).
- `tests/` – pytest unit tests. Spark tests need `pyspark` and Java and are skipped otherwise.

Catalogs: `dev_tamarack`, `stg_tamarack`, `prod_tamarack`, each with bronze, silver and gold schemas.

```
pip install -e ".[dev]"
python -m pytest
databricks bundle validate --strict --target dev
databricks bundle deploy --target dev     # staging and prod are deployed by CI only
```
