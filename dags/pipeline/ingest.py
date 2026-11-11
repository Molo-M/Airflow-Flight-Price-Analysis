"""
Stage 1: Data ingestion.
Loads the raw CSV into the MySQL staging table and checks nothing was lost.
"""

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pipeline.config import COLUMN_MAPPING, CSV_PATH, MYSQL_CONN_ID, STAGING_TABLE


def load_csv_to_staging():
    """
    Load the raw CSV into the MySQL staging table and verify the load.

    Steps: read the CSV, check all expected columns exist, rename columns,
    convert types, empty the staging table, insert all rows, then compare
    the row count and fare total in MySQL with the CSV.

    Returns the number of rows loaded. Raises ValueError if anything is
    missing or the verification fails.
    """
    # 1. Read the CSV
    df = pd.read_csv(CSV_PATH)
    print(f"Read {len(df)} rows and {len(df.columns)} columns from {CSV_PATH}")

    # 2. Make sure every expected column is in the file
    missing_columns = [column for column in COLUMN_MAPPING if column not in df.columns]
    if missing_columns:
        raise ValueError(f"The CSV is missing these columns: {missing_columns}")

    # 3. Rename columns to database-friendly names, keeping only the expected columns
    df = df.rename(columns=COLUMN_MAPPING)
    df = df[list(COLUMN_MAPPING.values())]

    # 4. Convert values to match the table's column types
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"], errors="coerce")
    for column in ["duration_hrs", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").round(2)
    df["days_before_departure"] = pd.to_numeric(df["days_before_departure"], errors="coerce")

    # Database drivers need Python None for missing values, not pandas' NaN/NaT
    df = df.astype(object).where(df.notna(), None)
    rows = list(df.itertuples(index=False, name=None))

    # 5. Empty the staging table, then insert all rows
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(f"TRUNCATE TABLE {STAGING_TABLE}")
    hook.insert_rows(
        table=STAGING_TABLE,
        rows=rows,
        target_fields=list(df.columns),
        commit_every=5000,
        executemany=True,  # sends rows in batches instead of one at a time
    )

    # 6. Verify: row count and fare total in MySQL must match the CSV
    row_count, fare_sum = hook.get_first(
        f"SELECT COUNT(*), SUM(total_fare_bdt) FROM {STAGING_TABLE}"
    )
    expected_fare_sum = pd.to_numeric(df["total_fare_bdt"]).sum()

    if row_count != len(df):
        raise ValueError(f"Row count mismatch: CSV has {len(df)}, MySQL has {row_count}")
    if abs(float(fare_sum or 0) - expected_fare_sum) > 1:
        raise ValueError(f"Fare total mismatch: CSV {expected_fare_sum:.2f}, MySQL {fare_sum}")

    print(f"Verified: {row_count} rows loaded into {STAGING_TABLE}, fare totals match")
    return row_count