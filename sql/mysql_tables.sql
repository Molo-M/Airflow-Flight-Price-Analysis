-- MySQL staging database tables for the Flight Price Analysis pipeline.
-- Every statement uses IF NOT EXISTS, so running this file again is safe.

-- Raw data from the CSV, loaded exactly as it arrives.
-- Columns allow NULL on purpose: deciding what is valid is the validation step's job.
CREATE TABLE IF NOT EXISTS flights_staging (
    id                     INT AUTO_INCREMENT PRIMARY KEY,
    airline                VARCHAR(50),
    source                 CHAR(3),
    source_name            VARCHAR(100),
    destination            CHAR(3),
    destination_name       VARCHAR(100),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hrs           DECIMAL(6, 2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(30),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          DECIMAL(12, 2),
    tax_surcharge_bdt      DECIMAL(12, 2),
    total_fare_bdt         DECIMAL(12, 2),
    seasonality            VARCHAR(20),
    days_before_departure  INT
);

-- Rows that passed validation (output of the validate_data task).
-- staging_id points back to the row's id in flights_staging.
-- fare_mismatch = 1 when total fare does not equal base fare + tax.
CREATE TABLE IF NOT EXISTS flights_validated (
    staging_id             INT PRIMARY KEY,
    airline                VARCHAR(50),
    source                 CHAR(3),
    source_name            VARCHAR(100),
    destination            CHAR(3),
    destination_name       VARCHAR(100),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hrs           DECIMAL(6, 2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(30),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          DECIMAL(12, 2),
    tax_surcharge_bdt      DECIMAL(12, 2),
    total_fare_bdt         DECIMAL(12, 2),
    seasonality            VARCHAR(20),
    days_before_departure  INT,
    fare_mismatch          BOOLEAN NOT NULL DEFAULT FALSE
);

-- Rows that failed validation, with the reasons they failed.
CREATE TABLE IF NOT EXISTS flights_rejected (
    staging_id             INT PRIMARY KEY,
    airline                VARCHAR(50),
    source                 CHAR(3),
    source_name            VARCHAR(100),
    destination            CHAR(3),
    destination_name       VARCHAR(100),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hrs           DECIMAL(6, 2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(30),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          DECIMAL(12, 2),
    tax_surcharge_bdt      DECIMAL(12, 2),
    total_fare_bdt         DECIMAL(12, 2),
    seasonality            VARCHAR(20),
    days_before_departure  INT,
    rejection_reason       VARCHAR(500) NOT NULL
);

-- Validated rows plus derived columns (output of the transform_data task).
--   total_fare_calculated = 1 if total fare was missing and calculated as base fare + tax
--   is_peak_season        = 1 for Eid, Hajj and Winter Holidays, 0 for Regular
--   route                 = source and destination codes joined, e.g. DAC-CXB
CREATE TABLE IF NOT EXISTS flights_enriched (
    staging_id             INT PRIMARY KEY,
    airline                VARCHAR(50),
    source                 CHAR(3),
    source_name            VARCHAR(100),
    destination            CHAR(3),
    destination_name       VARCHAR(100),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hrs           DECIMAL(6, 2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(30),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          DECIMAL(12, 2),
    tax_surcharge_bdt      DECIMAL(12, 2),
    total_fare_bdt         DECIMAL(12, 2),
    seasonality            VARCHAR(20),
    days_before_departure  INT,
    fare_mismatch          BOOLEAN NOT NULL DEFAULT FALSE,
    total_fare_calculated  BOOLEAN NOT NULL DEFAULT FALSE,
    is_peak_season         BOOLEAN NOT NULL DEFAULT FALSE,
    route                  VARCHAR(7)
);

-- ---------------------------------------------------------------------------
-- KPI tables (output of the compute_kpis task)
-- ---------------------------------------------------------------------------

-- KPI 1: Average total fare per airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline              VARCHAR(50) PRIMARY KEY,
    avg_total_fare_bdt   DECIMAL(12, 2)
);

-- KPI 2a: Average fare per season, compared with the non-peak average
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    seasonality            VARCHAR(20) PRIMARY KEY,
    season_type            VARCHAR(10),
    booking_count          INT,
    avg_total_fare_bdt     DECIMAL(12, 2),
    pct_diff_vs_non_peak   DECIMAL(7, 2)
);

-- KPI 2b: All peak seasons together vs non-peak
CREATE TABLE IF NOT EXISTS kpi_peak_vs_non_peak (
    season_type            VARCHAR(10) PRIMARY KEY,
    booking_count          INT,
    avg_total_fare_bdt     DECIMAL(12, 2),
    pct_diff_vs_non_peak   DECIMAL(7, 2)
);

-- KPI 3: Number of bookings per airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    airline           VARCHAR(50) PRIMARY KEY,
    booking_count     INT,
    pct_of_bookings   DECIMAL(5, 2)
);

-- KPI 4: Routes ranked by number of bookings (1 = most popular)
-- route_rank is used instead of "rank", which is a reserved word in MySQL
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    route_rank           INT PRIMARY KEY,
    route                VARCHAR(7),
    source               CHAR(3),
    source_name          VARCHAR(100),
    destination          CHAR(3),
    destination_name     VARCHAR(100),
    booking_count        INT,
    avg_total_fare_bdt   DECIMAL(12, 2)
);