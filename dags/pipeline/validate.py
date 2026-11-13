"""
Stage 2: Data validation.

Reads the staging table, corrects small formatting problems, checks every row,
flags fare mismatches, and splits the data into:
    flights_validated  rows that passed every check
    flights_rejected   rows that failed, with the reasons why
"""

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pipeline.config import (
    DATETIME_COLUMNS,
    FARE_MISMATCH_TOLERANCE,
    MYSQL_CONN_ID,
    NOT_NULL_COLUMNS,
    NUMERIC_COLUMNS,
    REJECTED_TABLE,
    REQUIRED_COLUMNS,
    STAGING_TABLE,
    TEXT_COLUMNS,
    VALID_SEASONS,
    VALIDATED_TABLE,
)
from pipeline.db import read_query, replace_table_contents


def add_reason(reasons, mask, reason):
    """Append `reason` to the reasons of every row where `mask` is True."""
    reasons.loc[mask] = reasons.loc[mask] + reason + "; "


def clean_and_check(df):
    """
    Correct small formatting problems, then check every row.

    Returns (cleaned_df, reasons). `reasons` holds one string per row:
    empty if the row passed every check, otherwise the list of problems.
    This function does not touch any database, so it can be tested on its own.
    """
    df = df.copy()
    reasons = pd.Series("", index=df.index)

    # 1. CORRECT: trim spaces, treat blank text as missing, uppercase airport codes
    for column in TEXT_COLUMNS:
        df[column] = df[column].str.strip()
        df[column] = df[column].mask(df[column] == "")  # "" becomes missing
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()

    # 2. MISSING VALUES: columns a row cannot do without
    for column in NOT_NULL_COLUMNS:
        add_reason(reasons, df[column].isna(), f"{column} is missing")

    # 3. DATA TYPES: values that exist but are not valid numbers or dates
    for column in NUMERIC_COLUMNS:
        had_value = df[column].notna()
        df[column] = pd.to_numeric(df[column], errors="coerce")
        add_reason(reasons, had_value & df[column].isna(), f"{column} is not a number")

    for column in DATETIME_COLUMNS:
        had_value = df[column].notna()
        df[column] = pd.to_datetime(df[column], errors="coerce")
        add_reason(reasons, had_value & df[column].isna(), f"{column} is not a valid date")

    # 4. INCONSISTENCIES
    add_reason(reasons, df["base_fare_bdt"] <= 0, "base_fare_bdt is not positive")
    add_reason(reasons, df["tax_surcharge_bdt"] < 0, "tax_surcharge_bdt is negative")
    add_reason(reasons, df["total_fare_bdt"] <= 0, "total_fare_bdt is not positive")

    for column in ["source", "destination"]:
        is_three_letters = df[column].str.fullmatch(r"[A-Z]{3}", na=False)
        add_reason(reasons, df[column].notna() & ~is_three_letters, f"{column} is not a 3-letter airport code")

    add_reason(reasons, df["source"] == df["destination"], "source and destination are the same")
    add_reason(reasons, df["arrival_datetime"] <= df["departure_datetime"], "arrival is not after departure")

    unknown_season = df["seasonality"].notna() & ~df["seasonality"].isin(VALID_SEASONS)
    add_reason(reasons, unknown_season, "seasonality is not a known season")

    return df, reasons


def flag_fare_mismatches(df):
    """
    Return True for rows whose total fare is present but does not equal
    base fare + tax (within FARE_MISMATCH_TOLERANCE). These rows are kept, only flagged.
    """
    expected_total = df["base_fare_bdt"] + df["tax_surcharge_bdt"]
    difference = (df["total_fare_bdt"] - expected_total).abs()
    return df["total_fare_bdt"].notna() & (difference > FARE_MISMATCH_TOLERANCE)


def validate_staging_data():
    """
    Validate the staging table and write the results to the validated and
    rejected tables. Fails if required columns are missing, if no row is valid,
    or if the output row counts don't add up to the input.

    Returns a summary dictionary of row counts.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    # 1. Read the staged data
    df = read_query(hook, f"SELECT * FROM {STAGING_TABLE}")
    print(f"Read {len(df)} rows from {STAGING_TABLE}")
    if df.empty:
        raise ValueError(f"{STAGING_TABLE} is empty. Did the ingestion step run?")

    # 2. Required columns must exist before any row can be checked
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Required columns are missing: {missing_columns}")
    print("All required columns exist")

    df = df.rename(columns={"id": "staging_id"})

    # 3. Correct and check every row
    df, reasons = clean_and_check(df)
    is_valid = reasons == ""

    # 4. Split into valid and rejected rows
    valid_df = df[is_valid].copy()
    valid_df["fare_mismatch"] = flag_fare_mismatches(valid_df).astype(int)

    rejected_df = df[~is_valid].copy()
    rejected_df["rejection_reason"] = reasons[~is_valid].str.rstrip("; ")

    # 5. Write both tables
    replace_table_contents(hook, VALIDATED_TABLE, valid_df)
    replace_table_contents(hook, REJECTED_TABLE, rejected_df)

    # 6. Verify nothing was lost or counted twice
    valid_count = hook.get_first(f"SELECT COUNT(*) FROM {VALIDATED_TABLE}")[0]
    rejected_count = hook.get_first(f"SELECT COUNT(*) FROM {REJECTED_TABLE}")[0]
    if valid_count + rejected_count != len(df):
        raise ValueError(
            f"Row counts don't add up: {len(df)} staged, "
            f"{valid_count} validated + {rejected_count} rejected"
        )
    if valid_count == 0:
        raise ValueError("No rows passed validation")

    # 7. Print a summary for the log
    mismatch_count = int(valid_df["fare_mismatch"].sum())
    print("----- Validation summary -----")
    print(f"Rows checked:             {len(df)}")
    print(f"Rows valid:               {valid_count}")
    print(f"Rows rejected:            {rejected_count}")
    print(f"Valid rows with fare mismatch (flagged, kept): {mismatch_count}")
    if rejected_count > 0:
        print("Rejection reasons:")
        print(rejected_df["rejection_reason"].value_counts().to_string())

    return {
        "rows_checked": int(len(df)),
        "rows_valid": int(valid_count),
        "rows_rejected": int(rejected_count),
        "fare_mismatches_flagged": mismatch_count,
    }
