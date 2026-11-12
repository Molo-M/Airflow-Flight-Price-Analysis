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