"""
# Flight Price Analysis Pipeline

Processes the **Flight Price Dataset of Bangladesh** from a raw CSV file into
analysis-ready tables and KPIs.

## Flow

| Order | Task | What it does |
|---|---|---|
| 1 | `create_tables` | Creates the MySQL and PostgreSQL tables if they don't exist |
| 2 | `ingest_csv` | Loads the CSV into the MySQL staging table and verifies it |
| 3 | `validate_data` | Checks every staged row; splits it into validated and rejected tables |
| 4 | `transform_data` | Fills missing total fares and adds derived columns |
| 5 | `compute_kpis` | Computes the four KPIs into KPI tables |
| 6 | `load_to_postgres` | Copies the enriched data and KPI tables to PostgreSQL in one transaction |

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

from pipeline import alerts, ingest, kpis, load, tables, transform, validate

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
        Runs `sql/mysql_tables.sql` against the MySQL staging database and
        `sql/postgres_tables.sql` against the PostgreSQL analytics database.
        Uses `CREATE TABLE IF NOT EXISTS`, so it is safe to run every time.
        """
        tables.create_mysql_tables()
        tables.create_postgres_tables()

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

    @task
    def validate_data():
        """
        ### Validate data
        Reads `flights_staging` and checks that the required columns exist.
        Then, for every row:

        - **Corrects** extra spaces in text and lowercase airport codes
        - **Rejects** rows with missing required values, non-numeric fares,
          invalid dates, zero or negative fares, invalid airport codes,
          the same source and destination, arrival not after departure,
          or an unknown season
        - **Flags** (but keeps) rows where total fare is not base fare + tax

        Valid rows go to `flights_validated`, rejected rows to `flights_rejected`
        with a `rejection_reason`.

        **Checks:** validated + rejected rows must equal the staged rows.

        **Returns:** a summary of row counts.
        """
        return validate.validate_staging_data()

    @task
    def transform_data():
        """
        ### Transform data
        Reads `flights_validated` and writes `flights_enriched` with:

        - **Total fare filled in** where missing: base fare + tax & surcharge
        - `total_fare_calculated`: 1 if the total was calculated here
        - `is_peak_season`: 1 for Eid, Hajj and Winter Holidays, 0 for Regular
        - `route`: source and destination joined, e.g. `DAC-CXB`

        **Checks:** no rows lost, and no total fare still missing.

        **Returns:** a summary of row counts.
        """
        return transform.transform_validated_data()

    @task
    def compute_kpis():
        """
        ### Compute KPIs
        Reads `flights_enriched` and writes one table per KPI
        (each booking is one row of the dataset):

        | Table | KPI |
        |---|---|
        | `kpi_avg_fare_by_airline` | Mean total fare per airline |
        | `kpi_seasonal_fare_variation` | Mean fare per season vs the non-peak average |
        | `kpi_peak_vs_non_peak` | All peak seasons together vs non-peak |
        | `kpi_booking_count_by_airline` | Bookings per airline and share of total |
        | `kpi_popular_routes` | Routes ranked by booking count |

        **Checks:** count-based KPIs must account for every booking exactly once.

        **Returns:** the number of rows in each KPI table.
        """
        return kpis.compute_kpis()

    @task
    def load_to_postgres():
        """
        ### Load to PostgreSQL
        Copies `flights_enriched` and the five KPI tables from MySQL to the
        PostgreSQL analytics database.

        - **Consistency:** all six tables are replaced in **one transaction**.
          If anything fails, everything is rolled back and PostgreSQL keeps
          its previous data. Readers see the old data until the commit.
        - **Latency:** rows are inserted in batches of 1,000 per statement.
        - Flag columns become true/false `BOOLEAN` columns.

        **Checks:** each table's row count must match MySQL before committing.

        **Returns:** the number of rows loaded into each table.
        """
        return load.load_to_postgres()

    (
        create_tables()
        >> ingest_csv()
        >> validate_data()
        >> transform_data()
        >> compute_kpis()
        >> load_to_postgres()
    )


flight_price_pipeline()