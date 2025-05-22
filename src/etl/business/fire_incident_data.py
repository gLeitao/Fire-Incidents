from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from config.config import get_refined_path, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('fire_incident_data')

def main(load_date):
    """Main function to process fire incident data"""
    log_info(logger, f"Starting fire incident data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsFactData") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from refined layer
        REFINED_PATH = get_refined_path(load_date)
        log_info(logger, f"Reading data from: {REFINED_PATH}")
        
        try:
            df = spark.read.parquet(REFINED_PATH)
            log_info(logger, f"Successfully read {df.count()} records from refined layer")
            
            # Select and transform fire incident columns
            df_fire_incident = df.select(
                col("incident_number"),
                to_date(col("incident_date")).alias("incident_date"),
                col("alarm_dt_tm").alias("alarm_time"),
                col("arrival_dt_tm").alias("arrival_time"),
                col("dispatch_time"),
                col("turnout_time"),
                col("suppression_time"),
                col("suppression_units").cast("integer"),
                col("suppression_personnel").cast("integer"),
                col("ems_units").cast("integer"),
                col("ems_personnel").cast("integer"),
                col("other_units").cast("integer"),
                col("other_personnel").cast("integer"),
                col("estimated_property_loss").cast("decimal(18,2)"),
                col("estimated_contents_loss").cast("decimal(18,2)"),
                col("fire_fatalities").cast("integer"),
                col("fire_injuries").cast("integer"),
                col("civilian_fatalities").cast("integer"),
                col("civilian_injuries").cast("integer"),
                col("number_of_floors_with_minimum_damage").alias("floors_minimum_damage").cast("integer"),
                col("number_of_floors_with_significant_damage").alias("floors_significant_damage").cast("integer"),
                col("number_of_floors_with_heavy_damage").alias("floors_heavy_damage").cast("integer"),
                col("number_of_floors_with_extreme_damage").alias("floors_extreme_damage").cast("integer"),
                col("detector_alerted_occupants").cast("boolean"),
                col("detectors_present").cast("boolean"),
                col("automatic_extinguishing_system_present").cast("boolean"),
                col("number_of_sprinkler_heads_operating").cast("integer")
            )
            
            log_info(logger, f"Transformed {df_fire_incident.count()} fire incident records")
            
            # Write to business layer
            FIRE_INCIDENT_PATH = get_business_path(load_date, "fire_incident_data")
            log_info(logger, f"Writing data to: {FIRE_INCIDENT_PATH}")
            
            df_fire_incident.write.mode("overwrite").parquet(FIRE_INCIDENT_PATH)
            log_info(logger, "Successfully wrote fire incident data to business layer")
            
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