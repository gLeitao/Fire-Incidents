from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import get_refined_path, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('detection_data')

def main(load_date):
    """Main function to process detection data"""
    log_info(logger, f"Starting detection data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsDetectionData") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from refined layer
        REFINED_PATH = get_refined_path(load_date)
        log_info(logger, f"Reading data from: {REFINED_PATH}")
        
        try:
            df = spark.read.parquet(REFINED_PATH)
            log_info(logger, f"Successfully read {df.count()} records from refined layer")
            
            # Select and transform detection columns
            df_detection = df.select(
                col("incident_number"),
                col("detector_alerted_occupants").cast("boolean"),
                col("detectors_present").cast("boolean"),
                col("detector_type"),
                col("detector_operation"),
                col("detector_effectiveness"),
                col("detector_failure_reason"),
                col("automatic_extinguishing_system_present").cast("boolean"),
                col("automatic_extinguishing_system_type"), 
                col("automatic_extinguishing_system_performance"), 
                col("automatic_extinguishing_system_failure_reason"),  
                col("number_of_sprinkler_heads_operating").cast("integer"),
                to_date(col("incident_date")).alias("incident_date")
            )
            
            log_info(logger, f"Transformed {df_detection.count()} detection records")
            
            # Write to business layer
            DETECTION_PATH = get_business_path(load_date, "detection_data")
            log_info(logger, f"Writing data to: {DETECTION_PATH}")
            
            df_detection.write.mode("overwrite").parquet(DETECTION_PATH)
            log_info(logger, "Successfully wrote detection data to business layer")
            
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