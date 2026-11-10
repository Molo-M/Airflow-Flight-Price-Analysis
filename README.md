## Project Overview

Develop an end-to-end data pipeline using Apache Airflow to process and analyze flight price data for Bangladesh. The pipeline must ingest raw CSV data, validate and transform it, compute key performance indicators (KPIs), and store the final results in a PostgreSQL database for further analysis.

## Proposed File Structure

```
Airflow-Flight-Price-Analysis/
├── dags/
│   ├── flight_price_pipeline.py      # The DAG: defines tasks and their order
│   └── pipeline/                     # The actual logic, one file per stage
│       ├── __init__.py
│       ├── config.py                 # Connection IDs, table names, column mapping
│       ├── ingest.py
│       ├── validate.py
│       ├── transform.py
│       ├── kpis.py
│       └── load.py
├── sql/
│   ├── mysql_tables.sql              # CREATE TABLE statements for staging
│   └── postgres_tables.sql           # CREATE TABLE statements for analytics
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
├── notebooks/
│   └── explore_dataset.ipynb         # Quick exploration before building
├── docs/
│   └── report.md                     # The required documentation
├── config/  logs/  plugins/          # Already created; Airflow uses these
├── docker-compose.yaml
├── .env                              # Not committed to Git (contains your key)
├── .gitignore
└── README.md
```
