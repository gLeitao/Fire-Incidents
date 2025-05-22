from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import get_refined_path, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('incident_data')

def main(load_date):
    """Main function to process incident data"""
    log_info(logger, f"Starting incident data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsIncidentData") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from refined layer
        REFINED_PATH = get_refined_path(load_date)
        log_info(logger, f"Reading data from: {REFINED_PATH}")
        
        try:
            df = spark.read.parquet(REFINED_PATH)
            log_info(logger, f"Successfully read {df.count()} records from refined layer")
            
            # Select and transform incident columns
            df_incident = df.select(
                col("incident_number"),
                col("primary_situation"),
                col("property_use"),
                col("area_of_fire_origin"),
                col("ignition_cause"),
                col("ignition_factor_primary"),
                col("ignition_factor_secondary"),
                col("heat_source"),
                col("item_first_ignited"),
                col("human_factors_associated_with_ignition"),
                col("structure_type"),
                col("structure_status"),
                col("floor_of_fire_origin"),
                col("fire_spread"),
                col("no_flame_spread").cast("boolean"),
                to_date(col("incident_date")).alias("incident_date")
            )
            
            log_info(logger, f"Transformed {df_incident.count()} incident records")
            
            # Write to business layer
            INCIDENT_PATH = get_business_path(load_date, "incident_data")
            log_info(logger, f"Writing data to: {INCIDENT_PATH}")
            
            df_incident.write.mode("overwrite").parquet(INCIDENT_PATH)
            log_info(logger, "Successfully wrote incident data to business layer")
            
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