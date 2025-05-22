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

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fire_dw TO fire_user;