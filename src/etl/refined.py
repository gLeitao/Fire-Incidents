from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date
from config.config import get_raw_path, get_refined_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('refined')

def deduplicate_records(df, logger=None):
    """Deduplicate records: first on all columns, then on incident_number if present."""
    df_dedup_all = df.dropDuplicates()
    log_info(logger, f"Records after deduplication on all columns: {df_dedup_all.count()}")
    
    if 'incident_number' not in df_dedup_all.columns:
        log_info(logger, "Column 'incident_number' not found. Skipping deduplication on incident_number.")
        return df_dedup_all
 
    df_dedup_incident = df_dedup_all.dropDuplicates(['incident_number'])
    log_info(logger, f"Records after deduplication on incident_number: {df_dedup_incident.count()}")
    return df_dedup_incident

def main(load_date):
    """Main function to process refined data"""
    log_info(logger, f"Starting refined data processing for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsRefined") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Read data from raw layer
        RAW_PATH = get_raw_path(load_date)
        log_info(logger, f"Reading data from: {RAW_PATH}")
        
        try:
            df = spark.read.parquet(RAW_PATH)
            # Replace spaces with underscores in column names
            df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
            log_info(logger, f"Successfully read {df.count()} records from raw layer")
            
            # Data cleaning and transformation
            df_cleaned = df.select([
                trim(col(c)).alias(c) for c in df.columns
            ])
            
            # Deduplicate records using the new function
            df_dedup_incident = deduplicate_records(df_cleaned, logger)
            
            # Convert date columns
            df_transformed = df_dedup_incident.withColumn(
                "incident_date",
                to_date(col("incident_date"))
            )
            
            log_info(logger, f"Transformed {df_transformed.count()} records")
       
            # Write to refined layer
            REFINED_PATH = get_refined_path(load_date)
            log_info(logger, f"Writing data to: {REFINED_PATH}")
            
            df_transformed.write.mode("overwrite").parquet(REFINED_PATH)
            log_info(logger, "Successfully wrote data to refined layer")
            
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