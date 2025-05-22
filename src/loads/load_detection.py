from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import S3_BUCKET, PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions
import psycopg2
from psycopg2.extras import execute_values

# Setup logger
logger = setup_logger('load_detection')

def create_temp_table(cur):
    """Create temporary table for detection data"""
    log_info(logger, "Creating temporary table for detection data")
    try:
        cur.execute("""
            CREATE TEMP TABLE temp_detection (
                incident_number VARCHAR(255),
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
                number_of_sprinkler_heads_operating INTEGER,
                incident_date DATE
            ) ON COMMIT DROP
        """)
        log_info(logger, "Successfully created temporary table")
    except Exception as e:
        log_error(logger, e, {"operation": "create_temp_table"})
        raise

def upsert_from_temp(cur):
    """Upsert data from temporary table to dim_detection"""
    log_info(logger, "Starting upsert from temporary table")
    try:
        cur.execute("""
            INSERT INTO dim_detection (
                incident_number,
                detector_alerted_occupants,
                detectors_present,
                detector_type,
                detector_operation,
                detector_effectiveness,
                detector_failure_reason,
                automatic_extinguishing_system_present,
                automatic_extinguishing_system_type,
                automatic_extinguishing_system_performance,
                automatic_extinguishing_system_failure_reason,
                number_of_sprinkler_heads_operating,
                incident_date
            )
            SELECT 
                incident_number,
                detector_alerted_occupants,
                detectors_present,
                detector_type,
                detector_operation,
                detector_effectiveness,
                detector_failure_reason,
                automatic_extinguishing_system_present,
                automatic_extinguishing_system_type,
                automatic_extinguishing_system_performance,
                automatic_extinguishing_system_failure_reason,
                number_of_sprinkler_heads_operating,
                incident_date
            FROM temp_detection
            ON CONFLICT (incident_number) 
            DO UPDATE SET
                detector_alerted_occupants = EXCLUDED.detector_alerted_occupants,
                detectors_present = EXCLUDED.detectors_present,
                detector_type = EXCLUDED.detector_type,
                detector_operation = EXCLUDED.detector_operation,
                detector_effectiveness = EXCLUDED.detector_effectiveness,
                detector_failure_reason = EXCLUDED.detector_failure_reason,
                automatic_extinguishing_system_present = EXCLUDED.automatic_extinguishing_system_present,
                automatic_extinguishing_system_type = EXCLUDED.automatic_extinguishing_system_type,
                automatic_extinguishing_system_performance = EXCLUDED.automatic_extinguishing_system_performance,
                automatic_extinguishing_system_failure_reason = EXCLUDED.automatic_extinguishing_system_failure_reason,
                number_of_sprinkler_heads_operating = EXCLUDED.number_of_sprinkler_heads_operating,
                incident_date = EXCLUDED.incident_date
        """)
        log_info(logger, "Successfully upserted data from temporary table")
    except Exception as e:
        log_error(logger, e, {"operation": "upsert_from_temp"})
        raise

def main(load_date):
    """Main function to load detection data"""
    log_info(logger, f"Starting detection data load for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsLoadDetection") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from business layer
        BUSINESS_PATH = get_business_path(load_date, "detection_data")
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
                        INSERT INTO temp_detection (
                            incident_number,
                            detector_alerted_occupants,
                            detectors_present,
                            detector_type,
                            detector_operation,
                            detector_effectiveness,
                            detector_failure_reason,
                            automatic_extinguishing_system_present,
                            automatic_extinguishing_system_type,
                            automatic_extinguishing_system_performance,
                            automatic_extinguishing_system_failure_reason,
                            number_of_sprinkler_heads_operating,
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