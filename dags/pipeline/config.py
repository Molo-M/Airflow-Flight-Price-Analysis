"""
Settings shared by every stage of the pipeline.
Keeping them in one place means a name or path only ever needs changing here.
"""

# Airflow connection IDs (defined as AIRFLOW_CONN_* variables in docker-compose.yaml)
MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

# File locations inside the Airflow containers
CSV_PATH = "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
MYSQL_TABLES_SQL = "/opt/airflow/sql/mysql_tables.sql"

# Table names
STAGING_TABLE = "flights_staging"

# Original CSV column name -> database column name.
# The order here is also the order of the columns in the staging table.
COLUMN_MAPPING = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}
