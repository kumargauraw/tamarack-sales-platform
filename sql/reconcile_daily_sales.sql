-- Reconcile gold.daily_sales with the Oracle DAILY_SALES extract, month by month.
-- :catalog is a named parameter (dev_tamarack, stg_tamarack or prod_tamarack).
SELECT date_trunc('MONTH', r.sale_date) AS month,
       sum(r.revenue) AS oracle_revenue,
       sum(m.revenue) AS migrated_revenue,
       sum(m.revenue) - sum(r.revenue) AS difference
FROM IDENTIFIER(:catalog || '.bronze.oracle_daily_sales') r
LEFT JOIN IDENTIFIER(:catalog || '.gold.daily_sales') m USING (sale_date, store_id, product_id)
GROUP BY ALL
ORDER BY month;
