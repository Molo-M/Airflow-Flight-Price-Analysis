"""
# Flight Price Analysis Pipeline

Processes the **Flight Price Dataset of Bangladesh** from a raw CSV file into
analysis-ready tables and KPIs.

## Flow

| Order | Task | What it does |
|---|---|---|
| 1 | `create_tables` | Creates the MySQL tables if they don't exist |
| 2 | `ingest_csv` | Loads the CSV into the MySQL staging table and verifies it |

## Databases

- **MySQL (`mysql_staging`)**: staging area for raw and intermediate data
- **PostgreSQL (`postgres_analytics`)**: final enriched data and KPI tables

## Alerts and retries

If a task fails, it is retried once after 1 minute. If it fails again,
`pipeline.alerts.task_failure_alert` writes a **PIPELINE FAILURE ALERT**
to that task's log, with the DAG, task, run and error.

## Re-running

Every task empties its target table before loading, so the DAG can be
re-run safely without creating duplicate rows.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from pipeline import alerts, ingest, tables

# Settings inherited by every task in the DAG
default_args = {
    "retries": 1,                                     # try once more before failing
    "retry_delay": timedelta(minutes=1),              # wait 1 minute before retrying
    "on_failure_callback": alerts.task_failure_alert, # alert when a task finally fails
}


@dag(
    dag_id="flight_price_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # run manually for now
    catchup=False,
    tags=["flights"],
    default_args=default_args,
    doc_md=__doc__,  # shows the description at the top of this file in the Airflow UI
)
def flight_price_pipeline():
    @task
    def create_tables():
        """
        ### Create tables
        Runs `sql/mysql_tables.sql` against the MySQL staging database.
        Uses `CREATE TABLE IF NOT EXISTS`, so it is safe to run every time.
        """
        tables.create_mysql_tables()

    @task
    def ingest_csv():
        """
        ### Ingest CSV
        Reads `Flight_Price_Dataset_of_Bangladesh.csv`, renames the columns to
        database-friendly names, converts types, and loads every row into
        `flights_staging` (emptying it first).

        **Checks:** the row count and the sum of total fares in MySQL must match
        the CSV, otherwise the task fails.

        **Returns:** the number of rows loaded.
        """
        return ingest.load_csv_to_staging()

    create_tables() >> ingest_csv()


flight_price_pipeline()