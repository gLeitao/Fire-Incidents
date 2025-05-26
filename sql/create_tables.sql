CREATE SCHEMA IF NOT EXISTS fire_dw;

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'fire_user') THEN
      CREATE ROLE fire_user LOGIN PASSWORD 'your_password';
   END IF;
END
$$;

GRANT USAGE ON SCHEMA fire_dw TO fire_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA fire_dw GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fire_user;

SET search_path TO fire_dw;

DROP TABLE IF EXISTS fire_dw.dim_location CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.dim_location (
    incident_number VARCHAR(255) PRIMARY KEY,
    address VARCHAR(255),
    city VARCHAR(255),
    zipcode VARCHAR(255),
    supervisor_district VARCHAR(255),
    neighborhood_district VARCHAR(255),
    point VARCHAR(255)
);

DROP TABLE IF EXISTS fire_dw.dim_incident CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.dim_incident (
    incident_number VARCHAR(255) PRIMARY KEY,
    primary_situation VARCHAR(255),
    property_use VARCHAR(255),
    area_of_fire_origin VARCHAR(255),
    ignition_cause VARCHAR(255),
    ignition_factor_primary VARCHAR(255),
    ignition_factor_secondary VARCHAR(255),
    heat_source VARCHAR(255),
    item_first_ignited VARCHAR(255),
    human_factors_associated_with_ignition VARCHAR(255),
    structure_type VARCHAR(255),
    structure_status VARCHAR(255),
    floor_of_fire_origin VARCHAR(255),
    fire_spread VARCHAR(255),
    no_flame_spread BOOLEAN
);

DROP TABLE IF EXISTS fire_dw.dim_detection CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.dim_detection (
    incident_number VARCHAR(255) PRIMARY KEY,
    detector_alerted_occupants BOOLEAN,
    detectors_present BOOLEAN,
    detector_type VARCHAR(255),
    detector_operation VARCHAR(255),
    detector_effectiveness VARCHAR(255),
    detector_failure_reason VARCHAR(255),
    automatic_extinguishing_system_present BOOLEAN,
    automatic_extinguishing_system_type VARCHAR(255),
    automatic_extinguishing_system_performance VARCHAR(255),
    automatic_extinguishing_system_failure_reason VARCHAR(255),
    number_of_sprinkler_heads_operating INTEGER
);

DROP TABLE IF EXISTS fire_dw.fact_fire_incident CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.fact_fire_incident (
    incident_number VARCHAR(255) PRIMARY KEY,
    incident_date DATE,
    alarm_time TIMESTAMP,
    arrival_time TIMESTAMP,
    dispatch_time TIMESTAMP,
    turnout_time TIMESTAMP,
    suppression_time TIMESTAMP,
    suppression_units INTEGER,
    suppression_personnel INTEGER,
    ems_units INTEGER,
    ems_personnel INTEGER,
    other_units INTEGER,
    other_personnel INTEGER,
    estimated_property_loss DECIMAL(18,2),
    estimated_contents_loss DECIMAL(18,2),
    fire_fatalities INTEGER,
    fire_injuries INTEGER,
    civilian_fatalities INTEGER,
    civilian_injuries INTEGER,
    floors_minimum_damage INTEGER,
    floors_significant_damage INTEGER,
    floors_heavy_damage INTEGER,
    floors_extreme_damage INTEGER,
    detector_alerted_occupants BOOLEAN,
    detectors_present BOOLEAN,
    automatic_extinguishing_system_present BOOLEAN,
    number_of_sprinkler_heads_operating INTEGER
);

DROP TABLE IF EXISTS fire_dw.dim_time CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.dim_time (
    date_number INTEGER PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    quarter INTEGER,
    week INTEGER,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    full_date_description VARCHAR(50),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    day_of_year INTEGER,
    is_leap_year BOOLEAN,
    month_start BOOLEAN,
    month_end BOOLEAN,
    quarter_start BOOLEAN,
    quarter_end BOOLEAN,
    year_start BOOLEAN,
    year_end BOOLEAN
);

INSERT INTO fire_dw.dim_time (
    date_number, date, year, month, day, day_of_week, quarter, week, day_name, month_name, full_date_description, is_weekend, is_holiday, day_of_year, is_leap_year, month_start, month_end, quarter_start, quarter_end, year_start, year_end
)
WITH RECURSIVE dates AS (
    SELECT DATE '1960-01-01' AS date
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM dates
    WHERE date + INTERVAL '1 day' <= DATE '2160-12-31'
)
SELECT 
    CAST(TO_CHAR(date, 'YYYYMMDD') AS INTEGER) AS date_number,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(DOW FROM date) AS day_of_week,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(WEEK FROM date) AS week,
    TO_CHAR(date, 'Day') AS day_name,
    TO_CHAR(date, 'Month') AS month_name,
    TO_CHAR(date, 'FMDay, FMMonth DD, YYYY') AS full_date_description,
    (EXTRACT(DOW FROM date) IN (0,6)) AS is_weekend,
    EXTRACT(DOY FROM date) AS day_of_year,
    EXTRACT(ISODOW FROM date) = 366 AS is_leap_year,
    (EXTRACT(DAY FROM date) = 1) AS month_start,
    (date = (date_trunc('month', date) + INTERVAL '1 month - 1 day')::date) AS month_end,
    (date = date_trunc('quarter', date)) AS quarter_start,
    (date = (date_trunc('quarter', date) + INTERVAL '3 month - 1 day')::date) AS quarter_end,
    (EXTRACT(DOY FROM date) = 1) AS year_start,
    (date = (date_trunc('year', date) + INTERVAL '1 year - 1 day')::date) AS year_end
FROM dates;

-- Create dim_timeofday table
DROP TABLE IF EXISTS fire_dw.dim_timeofday CASCADE;
CREATE TABLE IF NOT EXISTS fire_dw.dim_timeofday (
    time_number INTEGER PRIMARY KEY,
    time TIME,
    hour INTEGER,
    minute INTEGER,
    second INTEGER,
    am_pm VARCHAR(2),
    hour_24 INTEGER,
    hour_12 INTEGER,
    minute_of_day INTEGER,
    second_of_day INTEGER,
    time_description VARCHAR(20)
);

-- Populate dim_timeofday with all seconds in a day 
INSERT INTO fire_dw.dim_timeofday (
    time_number, time, hour, minute, second, am_pm, hour_24, hour_12, minute_of_day, second_of_day, time_description
)
SELECT
    CAST(TO_CHAR(t, 'HH24MISS') AS INTEGER) AS time_number,
    t AS time,
    EXTRACT(HOUR FROM t) AS hour,
    EXTRACT(MINUTE FROM t) AS minute,
    EXTRACT(SECOND FROM t) AS second,
    TO_CHAR(t, 'AM') AS am_pm,
    EXTRACT(HOUR FROM t) AS hour_24,
    CAST(TO_CHAR(t, 'HH12') AS INTEGER) AS hour_12,
    (EXTRACT(HOUR FROM t) * 60 + EXTRACT(MINUTE FROM t)) AS minute_of_day,
    (EXTRACT(HOUR FROM t) * 3600 + EXTRACT(MINUTE FROM t) * 60 + EXTRACT(SECOND FROM t)) AS second_of_day,
    TO_CHAR(t, 'HH12:MI:SS AM') AS time_description
FROM generate_series(
    TIME '00:00:00',
    TIME '23:59:59',
    INTERVAL '1 second'
) AS t;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fire_dw TO fire_user;