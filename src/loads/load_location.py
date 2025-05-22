from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import S3_BUCKET, PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions
import psycopg2
from psycopg2.extras import execute_values

# Setup logger
logger = setup_logger('load_location')

def create_temp_table(cur):
    """Create temporary table for location data"""
    log_info(logger, "Creating temporary table for location data")
    try:
        cur.execute("SET search_path TO fire_dw;")
        cur.execute("""
            CREATE TEMP TABLE temp_location (
                incident_number VARCHAR(255),
                address VARCHAR(255),
                city VARCHAR(255),
                zipcode VARCHAR(255),
                supervisor_district VARCHAR(255),
                neighborhood_district VARCHAR(255),
                point VARCHAR(255),
                incident_date DATE
            ) ON COMMIT DROP
        """)
        log_info(logger, "Successfully created temporary table")
    except Exception as e:
        log_error(logger, e, {"operation": "create_temp_table"})
        raise

def upsert_from_temp(cur):
    """Upsert data from temporary table to dim_location"""
    log_info(logger, "Starting upsert from temporary table")
    try:
        cur.execute("SET search_path TO fire_dw;")
        cur.execute("""
            INSERT INTO dim_location (
                incident_number,
                address,
                city,
                zipcode,
                supervisor_district,
                neighborhood_district,
                point,
                incident_date
            )
            SELECT 
                incident_number,
                address,
                city,
                zipcode,
                supervisor_district,
                neighborhood_district,
                point,
                incident_date
            FROM temp_location
            ON CONFLICT (incident_number) 
            DO UPDATE SET
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                zipcode = EXCLUDED.zipcode,
                supervisor_district = EXCLUDED.supervisor_district,
                neighborhood_district = EXCLUDED.neighborhood_district,
                point = EXCLUDED.point,
                incident_date = EXCLUDED.incident_date
        """)
        log_info(logger, "Successfully upserted data from temporary table")
    except Exception as e:
        log_error(logger, e, {"operation": "upsert_from_temp"})
        raise

def main(load_date):
    """Main function to load location data"""
    log_info(logger, f"Starting location data load for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsLoadLocation") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from business layer
        BUSINESS_PATH = get_business_path(load_date, "location_data")
        log_info(logger, f"Reading data from: {BUSINESS_PATH}")
        
        try:
            df = spark.read.parquet(BUSINESS_PATH)
            log_info(logger, f"Successfully read {df.count()} records from business layer")
                        
            # Convert to pandas for batch processing
            pdf = df.toPandas()
            
            # Connect to PostgreSQL
            log_info(logger, "Connecting to PostgreSQL database")
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
                # Create temporary table
                create_temp_table(cur)
                
                # Process in batches of 1000 rows
                batch_size = 1000
                total_rows = len(pdf)
                
                for i in range(0, total_rows, batch_size):
                    batch = pdf.iloc[i:i + batch_size]
                    log_info(logger, f"Processing batch {i//batch_size + 1} of {(total_rows + batch_size - 1)//batch_size}")
                    
                    # Convert batch to list of tuples
                    data = [tuple(x) for x in batch.values]
                    
                    # Insert batch into temporary table
                    execute_values(cur, """
                        INSERT INTO fire_dw.temp_location (
                            incident_number,
                            address,
                            city,
                            zipcode,
                            supervisor_district,
                            neighborhood_district,
                            point,
                            incident_date
                        ) VALUES %s
                    """, data)
                    
                    log_info(logger, f"Successfully inserted batch of {len(batch)} records")
                
                # Upsert from temporary table
                upsert_from_temp(cur)
                
                # Commit transaction
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
            log_error(logger, e, {"operation": "data_processing", "load_date": load_date})
            raise
            
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