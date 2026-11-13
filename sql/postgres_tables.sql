-- PostgreSQL analytics database tables for the Flight Price Analysis pipeline.
-- These hold the final results: the enriched flight data and the KPI tables.
-- Every statement uses IF NOT EXISTS, so running this file again is safe.
--
-- Compared with the MySQL versions:
--   DATETIME -> TIMESTAMP, DECIMAL -> NUMERIC (the usual PostgreSQL names)
--   flag columns are real BOOLEAN (true/false) columns

-- Enriched flight data (copied from flights_enriched in MySQL)
CREATE TABLE IF NOT EXISTS flights_enriched (
    staging_id             INTEGER PRIMARY KEY,
    airline                VARCHAR(50),
    source                 CHAR(3),
    source_name            VARCHAR(100),
    destination            CHAR(3),
    destination_name       VARCHAR(100),
    departure_datetime     TIMESTAMP,
    arrival_datetime       TIMESTAMP,
    duration_hrs           NUMERIC(6, 2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(30),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          NUMERIC(12, 2),
    tax_surcharge_bdt      NUMERIC(12, 2),
    total_fare_bdt         NUMERIC(12, 2),
    seasonality            VARCHAR(20),
    days_before_departure  INTEGER,
    fare_mismatch          BOOLEAN NOT NULL DEFAULT FALSE,
    total_fare_calculated  BOOLEAN NOT NULL DEFAULT FALSE,
    is_peak_season         BOOLEAN NOT NULL DEFAULT FALSE,
    route                  VARCHAR(7)
);

-- KPI 1: Average total fare per airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline              VARCHAR(50) PRIMARY KEY,
    avg_total_fare_bdt   NUMERIC(12, 2)
);

-- KPI 2a: Average fare per season, compared with the non-peak average
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    seasonality            VARCHAR(20) PRIMARY KEY,
    season_type            VARCHAR(10),
    booking_count          INTEGER,
    avg_total_fare_bdt     NUMERIC(12, 2),
    pct_diff_vs_non_peak   NUMERIC(7, 2)
);

-- KPI 2b: All peak seasons together vs non-peak
CREATE TABLE IF NOT EXISTS kpi_peak_vs_non_peak (
    season_type            VARCHAR(10) PRIMARY KEY,
    booking_count          INTEGER,
    avg_total_fare_bdt     NUMERIC(12, 2),
    pct_diff_vs_non_peak   NUMERIC(7, 2)
);

-- KPI 3: Number of bookings per airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    airline           VARCHAR(50) PRIMARY KEY,
    booking_count     INTEGER,
    pct_of_bookings   NUMERIC(5, 2)
);

-- KPI 4: Routes ranked by number of bookings (1 = most popular)
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    route_rank           INTEGER PRIMARY KEY,
    route                VARCHAR(7),
    source               CHAR(3),
    source_name          VARCHAR(100),
    destination          CHAR(3),
    destination_name     VARCHAR(100),
    booking_count        INTEGER,
    avg_total_fare_bdt   NUMERIC(12, 2)
);
