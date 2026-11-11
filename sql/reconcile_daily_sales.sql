-- Reconcile gold.daily_sales with the Oracle DAILY_SALES extract, month by month.
SELECT date_trunc('MONTH', r.sale_date) AS month,
       sum(r.revenue) AS oracle_revenue,
       sum(m.revenue) AS migrated_revenue,
       sum(m.revenue) - sum(r.revenue) AS difference
FROM dev_tamarack.bronze.oracle_daily_sales r
LEFT JOIN dev_tamarack.gold.daily_sales m USING (sale_date, store_id, product_id)
GROUP BY ALL
ORDER BY month;