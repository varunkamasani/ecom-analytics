SELECT TOP 10 *
FROM marts.fact_customer_cohort_retention
ORDER BY cohort_month, cohort_index;

SELECT segment, COUNT(*) AS customers
FROM marts.fact_rfm
GROUP BY segment
ORDER BY customers DESC;