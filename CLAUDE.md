# Tamarack sales project

- Catalogs: dev_tamarack, stg_tamarack, prod_tamarack. Never hard-code a catalog; use the bundle variable.
- Deploy only to the dev target. Staging and prod are deployed by CI, never from this machine.
- Use the Databricks CLI profile "tamarack-dev". There is no prod profile here.
- Business logic goes in src/ as tested Python functions.
- Grants are defined in the bundle, and every grant change must be listed in the PR description.
- Run `databricks bundle validate` and the unit tests before saying anything is finished.