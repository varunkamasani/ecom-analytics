# Olist E-commerce Analytics (SQL Server + Python ETL + Tableau)

## Overview
This project builds an end-to-end analytics pipeline using the Olist public e-commerce dataset.
Data is loaded from CSV → SQL Server (staging) → star schema (marts) → advanced analytics (Cohort Retention + RFM Segmentation) → Tableau dashboard.

## Tech Stack
- **Python**: pandas, sqlalchemy, pyodbc (ETL)
- **Database**: SQL Server (SSMS)
- **SQL**: Star schema modeling + analytics queries
- **BI Tool**: Tableau (interactive dashboard)

## Project Structure
```
ecom-analytics/
│
├── README.md
│
├── etl/
│   └── 01_load_to_sqlserver.py
│
├── sql/
│   ├── 01_build_marts.sql
│   ├── cohort_rfm.sql
│   ├── Revenue by month (trend chart).sql
│   └── Top categories by revenue.sql
│
├── dashboards/
│   ├── Olist E-commerce Performance & Retention Dashboard.png
│   ├── Cohort Heatmap.png
│   ├── Revenue by RFM Segment.png
│   └── Top Categories by Revenue.png
│
├── notebooks/      (optional)
└── data_raw/       (local only, not pushed to GitHub)
```