/* ============================
   Revenue Trend by Month
   ============================ */
SELECT
    FORMAT(purchase_date, 'yyyy-MM') AS year_month,
    COUNT(DISTINCT order_id) AS orders,
    SUM(order_revenue) AS revenue,
    AVG(order_revenue) AS avg_order_value
FROM marts.fact_orders
GROUP BY FORMAT(purchase_date, 'yyyy-MM')
ORDER BY year_month;