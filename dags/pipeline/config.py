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
POSTGRES_TABLES_SQL = "/opt/airflow/sql/postgres_tables.sql"

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

# ---------------------------------------------------------------------------
# Validation settings (used by validate.py)
# ---------------------------------------------------------------------------
VALIDATED_TABLE = "flights_validated"
REJECTED_TABLE = "flights_rejected"

# Columns the project brief says must exist
REQUIRED_COLUMNS = [
    "airline",
    "source",
    "destination",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
]

# Columns a row cannot do without. total_fare_bdt is NOT here on purpose:
# if it is missing, the transformation step calculates it.
NOT_NULL_COLUMNS = [
    "airline",
    "source",
    "destination",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "seasonality",
]

TEXT_COLUMNS = [
    "airline",
    "source",
    "source_name",
    "destination",
    "destination_name",
    "stopovers",
    "aircraft_type",
    "travel_class",
    "booking_source",
    "seasonality",
]

NUMERIC_COLUMNS = [
    "duration_hrs",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
    "days_before_departure",
]

DATETIME_COLUMNS = ["departure_datetime", "arrival_datetime"]

VALID_SEASONS = ["Regular", "Winter Holidays", "Eid", "Hajj"]

# A total fare is flagged as a mismatch if it differs from
# base fare + tax by more than this many BDT (allows for rounding)
FARE_MISMATCH_TOLERANCE = 1.0

# ---------------------------------------------------------------------------
# Transformation and KPI settings (used by transform.py and kpis.py)
# ---------------------------------------------------------------------------
ENRICHED_TABLE = "flights_enriched"

# Seasons counted as peak travel periods. Everything else (Regular) is non-peak.
PEAK_SEASONS = ["Eid", "Hajj", "Winter Holidays"]

KPI_AVG_FARE_BY_AIRLINE = "kpi_avg_fare_by_airline"
KPI_SEASONAL_FARE_VARIATION = "kpi_seasonal_fare_variation"
KPI_PEAK_VS_NON_PEAK = "kpi_peak_vs_non_peak"
KPI_BOOKING_COUNT_BY_AIRLINE = "kpi_booking_count_by_airline"
KPI_POPULAR_ROUTES = "kpi_popular_routes"

# ---------------------------------------------------------------------------
# Loading settings (used by load.py)
# ---------------------------------------------------------------------------
# Tables copied from MySQL to PostgreSQL, in this order
LOAD_TABLES = [
    ENRICHED_TABLE,
    KPI_AVG_FARE_BY_AIRLINE,
    KPI_SEASONAL_FARE_VARIATION,
    KPI_PEAK_VS_NON_PEAK,
    KPI_BOOKING_COUNT_BY_AIRLINE,
    KPI_POPULAR_ROUTES,
]

# Flag columns stored as 1/0 in MySQL that become true/false BOOLEAN columns in PostgreSQL
BOOLEAN_COLUMNS = ["fare_mismatch", "total_fare_calculated", "is_peak_season"]

# Number of rows sent to PostgreSQL in each INSERT statement
POSTGRES_BATCH_SIZE = 1000