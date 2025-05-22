from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import get_refined_path, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('location_data')

def main(load_date):
    """Main function to process location data"""
    log_info(logger, f"Starting location data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsLocationData") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from refined layer
        REFINED_PATH = get_refined_path(load_date)
        log_info(logger, f"Reading data from: {REFINED_PATH}")
        
        try:
            df = spark.read.parquet(REFINED_PATH)
            log_info(logger, f"Successfully read {df.count()} records from refined layer")
            
            # Select and transform location columns
            df_location = df.select(
                col("incident_number"),
                col("address"),
                col("city"),
                col("zipcode"),
                col("supervisor_district"),
                col("neighborhood_district"),
                col("point"),
                to_date(col("incident_date")).alias("incident_date")
            )
            
            log_info(logger, f"Transformed {df_location.count()} location records")
            
            # Write to business layer
            LOCATION_PATH = get_business_path(load_date, "location_data")
            log_info(logger, f"Writing data to: {LOCATION_PATH}")
            
            df_location.write.mode("overwrite").parquet(LOCATION_PATH)
            log_info(logger, "Successfully wrote location data to business layer")
            
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