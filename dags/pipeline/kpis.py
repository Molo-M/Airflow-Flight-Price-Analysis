"""
Stage 3b: KPI computation.

Reads flights_enriched and computes the four KPIs from the project brief.
Each booking is one row of the dataset, so "booking count" = number of rows.
Each KPI function works on a DataFrame only, so it can be tested on its own.
"""

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pipeline.config import (
    ENRICHED_TABLE,
    KPI_AVG_FARE_BY_AIRLINE,
    KPI_BOOKING_COUNT_BY_AIRLINE,
    KPI_PEAK_VS_NON_PEAK,
    KPI_POPULAR_ROUTES,
    KPI_SEASONAL_FARE_VARIATION,
    MYSQL_CONN_ID,
)
from pipeline.db import read_query, replace_table_contents


def average_fare_by_airline(df):
    """KPI 1: mean total fare for each airline, highest first."""
    result = df.groupby("airline", as_index=False).agg(
        avg_total_fare_bdt=("total_fare_bdt", "mean")
    )
    result["avg_total_fare_bdt"] = result["avg_total_fare_bdt"].round(2)
    return result.sort_values("avg_total_fare_bdt", ascending=False)


def seasonal_fare_variation(df):
    """
    KPI 2: compare average fares in peak and non-peak seasons.

    Returns two tables:
        by_season       one row per season, with its difference from the non-peak average (%)
        peak_vs_non_peak  two rows: all peak seasons together vs non-peak
    """
    df = df.copy()
    df["season_type"] = df["is_peak_season"].map({1: "Peak", 0: "Non-Peak"})
    non_peak_average = df.loc[df["is_peak_season"] == 0, "total_fare_bdt"].mean()

    by_season = df.groupby(["seasonality", "season_type"], as_index=False).agg(
        booking_count=("total_fare_bdt", "count"),
        avg_total_fare_bdt=("total_fare_bdt", "mean"),
    )
    peak_vs_non_peak = df.groupby("season_type", as_index=False).agg(
        booking_count=("total_fare_bdt", "count"),
        avg_total_fare_bdt=("total_fare_bdt", "mean"),
    )

    # Percentage difference from the non-peak average, e.g. 25.0 = 25% more expensive
    for table in [by_season, peak_vs_non_peak]:
        difference = table["avg_total_fare_bdt"] - non_peak_average
        table["pct_diff_vs_non_peak"] = (difference / non_peak_average * 100).round(2)
        table["avg_total_fare_bdt"] = table["avg_total_fare_bdt"].round(2)

    by_season = by_season.sort_values("avg_total_fare_bdt", ascending=False)
    peak_vs_non_peak = peak_vs_non_peak.sort_values("avg_total_fare_bdt", ascending=False)
    return by_season, peak_vs_non_peak


def booking_count_by_airline(df):
    """KPI 3: number of bookings per airline and its share of all bookings, most first."""
    result = df.groupby("airline", as_index=False).agg(
        booking_count=("total_fare_bdt", "count")
    )
    result["pct_of_bookings"] = (result["booking_count"] / len(df) * 100).round(2)
    return result.sort_values("booking_count", ascending=False)


def most_popular_routes(df):
    """
    KPI 4: every source-destination pair ranked by booking count (rank 1 = most popular).
    Ties are ordered alphabetically by route so the ranking is the same on every run.
    """
    result = df.groupby(
        ["route", "source", "source_name", "destination", "destination_name"], as_index=False
    ).agg(
        booking_count=("total_fare_bdt", "count"),
        avg_total_fare_bdt=("total_fare_bdt", "mean"),
    )
    result["avg_total_fare_bdt"] = result["avg_total_fare_bdt"].round(2)
    result = result.sort_values(["booking_count", "route"], ascending=[False, True])
    result.insert(0, "route_rank", range(1, len(result) + 1))
    return result


def build_kpi_tables(df):
    """Compute every KPI and return them as {table name: DataFrame}."""
    by_season, peak_vs_non_peak = seasonal_fare_variation(df)
    return {
        KPI_AVG_FARE_BY_AIRLINE: average_fare_by_airline(df),
        KPI_SEASONAL_FARE_VARIATION: by_season,
        KPI_PEAK_VS_NON_PEAK: peak_vs_non_peak,
        KPI_BOOKING_COUNT_BY_AIRLINE: booking_count_by_airline(df),
        KPI_POPULAR_ROUTES: most_popular_routes(df),
    }


def compute_kpis():
    """
    Read flights_enriched, compute the KPIs and write each to its KPI table.
    Fails if the booking counts in the KPI tables don't add up to the total.

    Returns the number of rows written to each KPI table.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    df = read_query(hook, f"SELECT * FROM {ENRICHED_TABLE}")
    print(f"Read {len(df)} rows from {ENRICHED_TABLE}")
    if df.empty:
        raise ValueError(f"{ENRICHED_TABLE} is empty. Did the transformation step run?")

    # MySQL returns DECIMAL as Decimal objects; convert to numbers for calculating
    df["total_fare_bdt"] = pd.to_numeric(df["total_fare_bdt"])
    df["is_peak_season"] = pd.to_numeric(df["is_peak_season"])

    kpi_tables = build_kpi_tables(df)

    # Every booking must be counted exactly once in each count-based KPI
    for table in [KPI_BOOKING_COUNT_BY_AIRLINE, KPI_SEASONAL_FARE_VARIATION, KPI_POPULAR_ROUTES]:
        counted = kpi_tables[table]["booking_count"].sum()
        if counted != len(df):
            raise ValueError(f"{table} counts {counted} bookings, expected {len(df)}")

    summary = {}
    for table, kpi_df in kpi_tables.items():
        replace_table_contents(hook, table, kpi_df)
        print(f"\n--- {table} (first 10 rows) ---")
        print(kpi_df.head(10).to_string(index=False))
        summary[table] = int(len(kpi_df))

    return summary
