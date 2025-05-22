from pyspark.sql import SparkSession
from config.config import get_landing_path, get_raw_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions


# Setup logger
logger = setup_logger('raw')

def main(load_date):
    """Main function to process raw data"""
    log_info(logger, f"Starting raw data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsRaw") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from landing zone
        LANDING_PATH = get_landing_path(load_date)
        log_info(logger, f"Reading data from: {LANDING_PATH}")
        
        try:
            # Read CSV files with header and infer schema
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .load(LANDING_PATH)
            
            log_info(logger, f"Successfully read {df.count()} records from landing zone")
            
            # Basic data cleaning
            df_cleaned = df.dropDuplicates()
            log_info(logger, f"Removed duplicates, remaining records: {df_cleaned.count()}")
            
            # Write to raw layer as Parquet
            RAW_PATH = get_raw_path(load_date)
            log_info(logger, f"Writing data to: {RAW_PATH}")
            
            df_cleaned.write.mode("overwrite").parquet(RAW_PATH)
            log_info(logger, "Successfully wrote data to raw layer")
            
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