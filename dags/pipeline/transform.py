"""
Stage 3a: Data transformation.

Reads the validated data, calculates total fare where it is missing,
adds derived columns, and writes the result to flights_enriched.
"""

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pipeline.config import ENRICHED_TABLE, MYSQL_CONN_ID, PEAK_SEASONS, VALIDATED_TABLE
from pipeline.db import read_query, replace_table_contents


def enrich(df):
    """
    Fill in missing total fares and add derived columns.
    Returns a new DataFrame; does not touch any database.

    Added columns:
        total_fare_calculated  1 if total fare was missing and calculated here
        is_peak_season         1 for peak seasons (see PEAK_SEASONS), else 0
        route                  source and destination joined, e.g. "DAC-CXB"
    """
    df = df.copy()

    # MySQL returns DECIMAL values as Python Decimal objects; convert them to numbers pandas can calculate with
    for column in ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]:
        df[column] = pd.to_numeric(df[column])

    # Total Fare = Base Fare + Tax & Surcharge, only where the total is missing
    missing_total = df["total_fare_bdt"].isna()
    calculated_total = (df["base_fare_bdt"] + df["tax_surcharge_bdt"]).round(2)
    df.loc[missing_total, "total_fare_bdt"] = calculated_total[missing_total]
    df["total_fare_calculated"] = missing_total.astype(int)

    df["is_peak_season"] = df["seasonality"].isin(PEAK_SEASONS).astype(int)
    df["route"] = df["source"] + "-" + df["destination"]

    return df


def transform_validated_data():
    """
    Read flights_validated, enrich it, and write flights_enriched.
    Fails if rows are lost or any total fare is still missing.

    Returns a summary dictionary of row counts.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    df = read_query(hook, f"SELECT * FROM {VALIDATED_TABLE}")
    print(f"Read {len(df)} rows from {VALIDATED_TABLE}")
    if df.empty:
        raise ValueError(f"{VALIDATED_TABLE} is empty. Did the validation step run?")

    enriched_df = enrich(df)

    # Checks before writing
    if enriched_df["total_fare_bdt"].isna().any():
        raise ValueError("Some rows still have no total fare after transformation")

    replace_table_contents(hook, ENRICHED_TABLE, enriched_df)

    # Check after writing: every validated row must be in the enriched table
    enriched_count = hook.get_first(f"SELECT COUNT(*) FROM {ENRICHED_TABLE}")[0]
    if enriched_count != len(df):
        raise ValueError(f"Row count mismatch: {len(df)} validated, {enriched_count} enriched")

    calculated_count = int(enriched_df["total_fare_calculated"].sum())
    peak_count = int(enriched_df["is_peak_season"].sum())
    print("----- Transformation summary -----")
    print(f"Rows enriched:                  {enriched_count}")
    print(f"Total fares calculated:         {calculated_count}")
    print(f"Peak season bookings:           {peak_count}")
    print(f"Non-peak season bookings:       {enriched_count - peak_count}")

    return {
        "rows_enriched": int(enriched_count),
        "total_fares_calculated": calculated_count,
        "peak_season_bookings": peak_count,
    }
