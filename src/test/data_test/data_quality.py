from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from config.config import get_refined_path, get_business_path
from utils.logging_utils import setup_logger, log_error, log_info
import sys
from awsglue.utils import getResolvedOptions

# Setup logger
logger = setup_logger('data_quality')

def check_null_values(df, columns):
    """Check for null values in specified columns"""
    log_info(logger, "Checking null values in columns")
    try:
        null_counts = {}
        for column in columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts[column] = null_count
            log_info(logger, f"Column {column}: {null_count} null values")
        return null_counts
    except Exception as e:
        log_error(logger, e, {"operation": "check_null_values"})
        raise

def check_data_types(df):
    """Check data types of columns"""
    log_info(logger, "Checking data types of columns")
    try:
        schema = df.schema
        for field in schema:
            log_info(logger, f"Column {field.name}: {field.dataType}")
        return schema
    except Exception as e:
        log_error(logger, e, {"operation": "check_data_types"})
        raise

def check_unique_values(df, columns):
    """Check unique values in specified columns"""
    log_info(logger, "Checking unique values in columns")
    try:
        unique_counts = {}
        for column in columns:
            unique_count = df.select(column).distinct().count()
            unique_counts[column] = unique_count
            log_info(logger, f"Column {column}: {unique_count} unique values")
        return unique_counts
    except Exception as e:
        log_error(logger, e, {"operation": "check_unique_values"})
        raise

def check_date_format(df, date_columns):
    """Check if date columns are in correct format"""
    log_info(logger, "Checking date formats")
    try:
        invalid_dates = {}
        for column in date_columns:
            invalid_count = df.filter(col(column).isNotNull() & ~col(column).rlike("^\\d{4}-\\d{2}-\\d{2}$")).count()
            invalid_dates[column] = invalid_count
            log_info(logger, f"Column {column}: {invalid_count} invalid date formats")
        return invalid_dates
    except Exception as e:
        log_error(logger, e, {"operation": "check_date_format"})
        raise

def check_numeric_ranges(df, numeric_columns):
    """Check if numeric columns are within expected ranges"""
    log_info(logger, "Checking numeric ranges")
    try:
        range_violations = {}
        for column, (min_val, max_val) in numeric_columns.items():
            if column in df.columns:
                violations = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
                range_violations[column] = violations
                log_info(logger, f"Column {column}: {violations} values outside range [{min_val}, {max_val}]")
        return range_violations
    except Exception as e:
        log_error(logger, e, {"operation": "check_numeric_ranges"})
        raise

def check_boolean_consistency(df, boolean_columns):
    """Check if boolean columns contain only true/false values"""
    log_info(logger, "Checking boolean consistency")
    try:
        inconsistencies = {}
        for column in boolean_columns:
            if column in df.columns:
                invalid_count = df.filter(col(column).isNotNull() & ~col(column).isin([True, False])).count()
                inconsistencies[column] = invalid_count
                log_info(logger, f"Column {column}: {invalid_count} non-boolean values")
        return inconsistencies
    except Exception as e:
        log_error(logger, e, {"operation": "check_boolean_consistency"})
        raise

def run_quality_checks(load_date):
    """Run quality checks for all data layers"""
    log_info(logger, f"Starting quality checks for date: {load_date}")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FireIncidentsDQ") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Tabelas de negócio e seus campos relevantes
        business_tables = {
            "fire_incident_data": {
                "date_columns": ["incident_date"],
                "numeric_columns": {
                    "estimated_property_loss": (0, float('inf')),
                    "estimated_contents_loss": (0, float('inf')),
                    "fire_fatalities": (0, float('inf')),
                    "fire_injuries": (0, float('inf')),
                    "civilian_fatalities": (0, float('inf')),
                    "civilian_injuries": (0, float('inf')),
                    "floors_minimum_damage": (0, float('inf')),
                    "floors_significant_damage": (0, float('inf')),
                    "floors_heavy_damage": (0, float('inf')),
                    "floors_extreme_damage": (0, float('inf')),
                    "suppression_units": (0, float('inf')),
                    "suppression_personnel": (0, float('inf')),
                    "ems_units": (0, float('inf')),
                    "ems_personnel": (0, float('inf')),
                    "other_units": (0, float('inf')),
                    "other_personnel": (0, float('inf')),
                    "number_of_sprinkler_heads_operating": (0, float('inf'))
                },
                "boolean_columns": [
                    "detector_alerted_occupants",
                    "detectors_present",
                    "automatic_extinguishing_system_present"
                ]
            },
            "location_data": {
                "date_columns": ["incident_date"],
                "numeric_columns": {},
                "boolean_columns": []
            },
            "detection_data": {
                "date_columns": ["incident_date"],
                "numeric_columns": {
                    "number_of_sprinkler_heads_operating": (0, float('inf'))
                },
                "boolean_columns": [
                    "detector_alerted_occupants",
                    "detectors_present",
                    "automatic_extinguishing_system_present"
                ]
            },
            "incident_data": {
                "date_columns": ["incident_date"],
                "numeric_columns": {},
                "boolean_columns": ["no_flame_spread"]
            }
        }

        for table, checks in business_tables.items():
            try:
                BUSINESS_PATH = get_business_path(load_date, table)
                log_info(logger, f"Checking business layer at: {BUSINESS_PATH}")
                df = spark.read.parquet(BUSINESS_PATH)
                log_info(logger, f"Successfully read {df.count()} records from {table}")
                all_columns = df.columns
                check_null_values(df, all_columns)
                check_data_types(df)
                check_unique_values(df, ["incident_number"])
                check_date_format(df, checks["date_columns"])
                check_numeric_ranges(df, checks["numeric_columns"])
                check_boolean_consistency(df, checks["boolean_columns"])
            except Exception as e:
                log_error(logger, e, {"operation": f"business_checks_{table}", "load_date": load_date})
                continue
            
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
        run_quality_checks(load_date)
    except Exception as e:
        log_error(logger, e, {"operation": "script_execution"})
        sys.exit(1) 