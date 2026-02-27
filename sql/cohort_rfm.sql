/* ============================================================
   Project: E-commerce Analytics (Olist) - SQL Server
   Purpose: Cohort Retention + RFM Segmentation (Marts Layer)
   Database: ecom_analytics
   Schema: marts
   ============================================================ */

USE ecom_analytics;
GO

/* ============================================================
   SECTION 1: Cohort Analysis (Active Customers by Cohort)
   Output Table: marts.fact_customer_cohort
   Logic:
     - cohort_month  = first purchase month
     - order_month   = month of each purchase
     - cohort_index  = months since first purchase
     - active_customers = unique customers active that month
   ============================================================ */

DROP TABLE IF EXISTS marts.fact_customer_cohort;
GO

WITH first_purchase AS (
    SELECT
        customer_unique_id,
        MIN(purchase_date) AS first_purchase_date
    FROM marts.fact_orders
    WHERE order_status IN ('delivered', 'shipped', 'invoiced', 'approved', 'processing')
    GROUP BY customer_unique_id
),
orders_with_cohort AS (
    SELECT
        fo.customer_unique_id,
        fo.purchase_date,

        DATEFROMPARTS(YEAR(fp.first_purchase_date), MONTH(fp.first_purchase_date), 1) AS cohort_month,
        DATEFROMPARTS(YEAR(fo.purchase_date),      MONTH(fo.purchase_date),      1) AS order_month,

        DATEDIFF(
            MONTH,
            DATEFROMPARTS(YEAR(fp.first_purchase_date), MONTH(fp.first_purchase_date), 1),
            DATEFROMPARTS(YEAR(fo.purchase_date),      MONTH(fo.purchase_date),      1)
        ) AS cohort_index
    FROM marts.fact_orders fo
    JOIN first_purchase fp
      ON fo.customer_unique_id = fp.customer_unique_id
)
SELECT
    cohort_month,
    order_month,
    cohort_index,
    COUNT(DISTINCT customer_unique_id) AS active_customers
INTO marts.fact_customer_cohort
FROM orders_with_cohort
WHERE cohort_index >= 0
GROUP BY cohort_month, order_month, cohort_index;
GO


/* ============================================================
   SECTION 2: Cohort Retention Rate
   Output Table: marts.fact_customer_cohort_retention
   Logic:
     - cohort_customers = customers active in cohort_index = 0
     - retention_rate   = active_customers / cohort_customers
   ============================================================ */

DROP TABLE IF EXISTS marts.fact_customer_cohort_retention;
GO

WITH cohort_size AS (
    SELECT
        cohort_month,
        MAX(CASE WHEN cohort_index = 0 THEN active_customers END) AS cohort_customers
    FROM marts.fact_customer_cohort
    GROUP BY cohort_month
)
SELECT
    c.cohort_month,
    c.order_month,
    c.cohort_index,
    c.active_customers,
    cs.cohort_customers,
    CAST(1.0 * c.active_customers / NULLIF(cs.cohort_customers, 0) AS decimal(10,4)) AS retention_rate
INTO marts.fact_customer_cohort_retention
FROM marts.fact_customer_cohort c
JOIN cohort_size cs
  ON c.cohort_month = cs.cohort_month;
GO


/* ============================================================
   SECTION 3: RFM Segmentation
   Output Table: marts.fact_rfm
   Definitions:
     - Recency   = days since last purchase (lower is better)
     - Frequency = number of orders (higher is better)
     - Monetary  = total spend (higher is better)

   Scoring:
     - r_score, f_score, m_score via NTILE(5)
   ============================================================ */

DROP TABLE IF EXISTS marts.fact_rfm;
GO

WITH rfm_base AS (
    SELECT
        customer_unique_id,
        MAX(purchase_date) AS last_purchase_date,
        COUNT(DISTINCT order_id) AS frequency_orders,
        SUM(order_revenue) AS monetary_value
    FROM marts.fact_orders
    WHERE order_status IN ('delivered', 'shipped', 'invoiced', 'approved', 'processing')
    GROUP BY customer_unique_id
),
rfm_calc AS (
    SELECT
        customer_unique_id,
        last_purchase_date,
        DATEDIFF(DAY, last_purchase_date, (SELECT MAX(purchase_date) FROM marts.fact_orders)) AS recency_days,
        frequency_orders,
        monetary_value
    FROM rfm_base
),
rfm_scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days ASC)      AS r_score,
        NTILE(5) OVER (ORDER BY frequency_orders DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC)   AS m_score
    FROM rfm_calc
)
SELECT
    customer_unique_id,
    last_purchase_date,
    recency_days,
    frequency_orders,
    monetary_value,
    r_score, f_score, m_score,
    CONCAT(r_score, f_score, m_score) AS rfm_score,

    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score = 1  AND f_score <= 2 THEN 'Lost'
        ELSE 'Need Attention'
    END AS segment
INTO marts.fact_rfm
FROM rfm_scored;
GO


/* ============================================================
   SECTION 4: Validation Queries (Quick Checks)
   ============================================================ */

-- 4.1 Cohort retention sample
SELECT TOP 10 *
FROM marts.fact_customer_cohort_retention
ORDER BY cohort_month, cohort_index;

-- 4.2 RFM segment distribution
SELECT segment, COUNT(*) AS customers
FROM marts.fact_rfm
GROUP BY segment
ORDER BY customers DESC;
GO