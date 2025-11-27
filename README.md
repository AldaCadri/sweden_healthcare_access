# Care Guarantee Watch — Swedish Healthcare Access Project

![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue)
![dbt](https://img.shields.io/badge/dbt-Core-orange)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)
![Python](https://img.shields.io/badge/Python-Scripts-green)

This project analyzes how Sweden’s 21 regions perform against the **0–3–90–90 Vårdgaranti** (Care Guarantee).  
It includes a full Snowflake + dbt data warehouse, data transformation pipeline, and a multi-tab Power BI dashboard.

---

## Project Purpose
To provide a clear, data-driven overview of healthcare access across Sweden by combining multiple public datasets into a unified analytical model.  
The project focuses on waiting times, overcrowding, capacity, fairness, and international benchmarking.

---

## Repository Structure
```
sweden_healthcare_access/
│
├── care_guarantee_watch/        # Full dbt project (models, sources, tests)
│
├── src/                         # Supporting Python or ETL scripts
│
├── WH, DB creation and grants.sql   # Snowflake warehouse + roles + RBAC
│
├── requirements.txt             # Python library dependencies
│
└── README.md                    # Project documentation
```
---

## Data Sources

All datasets come from official open data platforms:

- **Kolada API** – regional indicators  
- **Vården i Siffror** – ED times, waiting times, diagnostics  
- **Statistics Sweden (SCB)** – population & demographics  
- **OECD Health Data** – beds & international metrics  
- **Socialstyrelsen / Government data** – medical personnel & capacity  
- Additional CSV/Excel files used in staging

---

## Technologies Used

**Data Engineering**
- Snowflake Data Warehouse  
- dbt Core (transformations, documentation, tests)  
- Internal stages & file ingestion  
- GeoJSON preparation

**Analytics**
- Power BI (final dashboard)
- Figma (high-level snapshot design)
- Python (validations & small transformations)

---

## Dashboard Content
The Power BI dashboard contains:

- Overview Snapshot 
- Healthcare Access
- Pressure & Capacity
- Costs & International Comparison

Link to dashboard: https://app.powerbi.com/reportEmbed?reportId=a5684bb9-f772-41fe-80f5-651ff6f749b9&autoAuth=true&ctid=07f5b35b-52fc-4f27-9e84-81da79ab468d
