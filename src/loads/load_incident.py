from pyspark.sql import SparkSession
from config.config import S3_BUCKET, PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql.functions import col, to_date

logger = setup_logger('load_incident')

def create_temp_table(cur):
    cur.execute("""
        CREATE TEMP TABLE temp_incident (
            incident_number VARCHAR(255),
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
            no_flame_spread BOOLEAN,
            incident_date DATE
        ) ON COMMIT DROP
    """)

def upsert_from_temp(cur):
    cur.execute("""
        INSERT INTO dim_incident (
            incident_number, primary_situation, property_use, area_of_fire_origin, ignition_cause, ignition_factor_primary, ignition_factor_secondary, heat_source, item_first_ignited, human_factors_associated_with_ignition, structure_type, structure_status, floor_of_fire_origin, fire_spread, no_flame_spread, incident_date
        )
        SELECT 
            incident_number, primary_situation, property_use, area_of_fire_origin, ignition_cause, ignition_factor_primary, ignition_factor_secondary, heat_source, item_first_ignited, human_factors_associated_with_ignition, structure_type, structure_status, floor_of_fire_origin, fire_spread, no_flame_spread, incident_date
        FROM temp_incident
        ON CONFLICT (incident_number) DO UPDATE SET
            primary_situation = EXCLUDED.primary_situation,
            property_use = EXCLUDED.property_use,
            area_of_fire_origin = EXCLUDED.area_of_fire_origin,
            ignition_cause = EXCLUDED.ignition_cause,
            ignition_factor_primary = EXCLUDED.ignition_factor_primary,
            ignition_factor_secondary = EXCLUDED.ignition_factor_secondary,
            heat_source = EXCLUDED.heat_source,
            item_first_ignited = EXCLUDED.item_first_ignited,
            human_factors_associated_with_ignition = EXCLUDED.human_factors_associated_with_ignition,
            structure_type = EXCLUDED.structure_type,
            structure_status = EXCLUDED.structure_status,
            floor_of_fire_origin = EXCLUDED.floor_of_fire_origin,
            fire_spread = EXCLUDED.fire_spread,
            no_flame_spread = EXCLUDED.no_flame_spread,
            incident_date = EXCLUDED.incident_date
    """)

def main(load_date):
    log_info(logger, f"Starting incident data load for date: {load_date}")
    try:
        spark = SparkSession.builder.appName("FireIncidentsLoadIncident") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        BUSINESS_PATH = get_business_path(load_date, "incident_data")
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
                    INSERT INTO temp_incident (
                        incident_number, primary_situation, property_use, area_of_fire_origin, ignition_cause, ignition_factor_primary, ignition_factor_secondary, heat_source, item_first_ignited, human_factors_associated_with_ignition, structure_type, structure_status, floor_of_fire_origin, fire_spread, no_flame_spread, incident_date
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