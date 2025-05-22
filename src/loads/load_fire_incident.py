from pyspark.sql import SparkSession
from config.config import S3_BUCKET, PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql.functions import col, to_date
from utils.path_utils import get_business_path

logger = setup_logger('load_fire_incident')

def create_temp_table(cur):
    cur.execute("""
        CREATE TEMP TABLE temp_fire_incident (
            incident_number VARCHAR(255),
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
        ) ON COMMIT DROP
    """)

def upsert_from_temp(cur):
    cur.execute("SET search_path TO fire_dw;")
    cur.execute("""
        INSERT INTO fire_dw.fact_fire_incident (
            incident_number, incident_date, alarm_time, arrival_time, dispatch_time, turnout_time, suppression_time, suppression_units, suppression_personnel, ems_units, ems_personnel, other_units, other_personnel, estimated_property_loss, estimated_contents_loss, fire_fatalities, fire_injuries, civilian_fatalities, civilian_injuries, floors_minimum_damage, floors_significant_damage, floors_heavy_damage, floors_extreme_damage, detector_alerted_occupants, detectors_present, automatic_extinguishing_system_present, number_of_sprinkler_heads_operating
        )
        SELECT 
            incident_number, incident_date, alarm_time, arrival_time, dispatch_time, turnout_time, suppression_time, suppression_units, suppression_personnel, ems_units, ems_personnel, other_units, other_personnel, estimated_property_loss, estimated_contents_loss, fire_fatalities, fire_injuries, civilian_fatalities, civilian_injuries, floors_minimum_damage, floors_significant_damage, floors_heavy_damage, floors_extreme_damage, detector_alerted_occupants, detectors_present, automatic_extinguishing_system_present, number_of_sprinkler_heads_operating
        FROM fire_dw.temp_fire_incident
        ON CONFLICT (incident_number) DO UPDATE SET
            incident_date = EXCLUDED.incident_date,
            alarm_time = EXCLUDED.alarm_time,
            arrival_time = EXCLUDED.arrival_time,
            dispatch_time = EXCLUDED.dispatch_time,
            turnout_time = EXCLUDED.turnout_time,
            suppression_time = EXCLUDED.suppression_time,
            suppression_units = EXCLUDED.suppression_units,
            suppression_personnel = EXCLUDED.suppression_personnel,
            ems_units = EXCLUDED.ems_units,
            ems_personnel = EXCLUDED.ems_personnel,
            other_units = EXCLUDED.other_units,
            other_personnel = EXCLUDED.other_personnel,
            estimated_property_loss = EXCLUDED.estimated_property_loss,
            estimated_contents_loss = EXCLUDED.estimated_contents_loss,
            fire_fatalities = EXCLUDED.fire_fatalities,
            fire_injuries = EXCLUDED.fire_injuries,
            civilian_fatalities = EXCLUDED.civilian_fatalities,
            civilian_injuries = EXCLUDED.civilian_injuries,
            floors_minimum_damage = EXCLUDED.floors_minimum_damage,
            floors_significant_damage = EXCLUDED.floors_significant_damage,
            floors_heavy_damage = EXCLUDED.floors_heavy_damage,
            floors_extreme_damage = EXCLUDED.floors_extreme_damage,
            detector_alerted_occupants = EXCLUDED.detector_alerted_occupants,
            detectors_present = EXCLUDED.detectors_present,
            automatic_extinguishing_system_present = EXCLUDED.automatic_extinguishing_system_present,
            number_of_sprinkler_heads_operating = EXCLUDED.number_of_sprinkler_heads_operating
    """)

def main(load_date):
    log_info(logger, f"Starting fire incident data load for date: {load_date}")
    try:
        spark = SparkSession.builder.appName("FireIncidentsLoadFireIncident") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        BUSINESS_PATH = get_business_path(load_date, "fire_incident_data")
        log_info(logger, f"Reading data from: {BUSINESS_PATH}")

        df = spark.read.parquet(BUSINESS_PATH)
        log_info(logger, f"Successfully read {df.count()} records from business layer")

        pdf = df.toPandas()

        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )

        conn.autocommit = False
        cur = conn.cursor()
        try:
            create_temp_table(cur)
            batch_size = 1000
            total_rows = len(pdf)
            for i in range(0, total_rows, batch_size):
                batch = pdf.iloc[i:i + batch_size]
                data = [tuple(x) for x in batch.values]
                execute_values(cur, """
                    INSERT INTO fire_dw.temp_fire_incident (
                        incident_number, incident_date, alarm_time, arrival_time, dispatch_time, turnout_time, suppression_time, suppression_units, suppression_personnel, ems_units, ems_personnel, other_units, other_personnel, estimated_property_loss, estimated_contents_loss, fire_fatalities, fire_injuries, civilian_fatalities, civilian_injuries, floors_minimum_damage, floors_significant_damage, floors_heavy_damage, floors_extreme_damage, detector_alerted_occupants, detectors_present, automatic_extinguishing_system_present, number_of_sprinkler_heads_operating
                    ) VALUES %s
                """, data)

            upsert_from_temp(cur)
            conn.commit()
            log_info(logger, "Successfully committed transaction")
        except Exception as e:
            conn.rollback()
            log_error(logger, e, {"operation": "database_operations", "load_date": load_date})
            raise
        finally:
            cur.close()
            conn.close()
            log_info(logger, "Database connection closed")
    except Exception as e:
        log_error(logger, e, {"operation": "main", "load_date": load_date})
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            log_info(logger, "Spark session stopped")

if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['load_date'])
        load_date = args['load_date']
        main(load_date)
    except Exception as e:
        log_error(logger, e, {"operation": "script_execution"})
        sys.exit(1) 