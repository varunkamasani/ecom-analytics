/* ============================
   Top Product Categories
   ============================ */
SELECT TOP 15
    COALESCE(dp.product_category_name_english, dp.product_category_name) AS category,
    SUM(foi.item_revenue) AS revenue,
    COUNT(DISTINCT foi.order_id) AS orders
FROM marts.fact_order_items foi
JOIN marts.dim_product dp
  ON foi.product_id = dp.product_id
GROUP BY COALESCE(dp.product_category_name_english, dp.product_category_name)
ORDER BY revenue DESC;