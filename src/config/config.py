import os
import json
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def get_secret(secret_name, region_name='us-east-1'):
    """Retrieve a secret from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            raise ValueError("Secret value is not a string")

# Get PostgreSQL configuration from Secrets Manager
pg_secret = get_secret('fire-incidents/postgres')
PG_HOST = pg_secret['host']
PG_PORT = pg_secret['port']
PG_DATABASE = pg_secret['dbname']
PG_USER = pg_secret['username']
PG_PASSWORD = pg_secret['password']

# Get S3 bucket from Secrets Manager
s3_secret = get_secret('fire-incidents/s3')
S3_BUCKET = s3_secret['bucket_name']
AWS_REGION = s3_secret.get('region', 'us-east-1')  # Default to us-east-1 if not specified

# Define S3 paths for each data layer
LANDING_BUCKET = f"s3a://{S3_BUCKET}/landing"
RAW_BUCKET = f"s3a://{S3_BUCKET}/raw"
REFINED_BUCKET = f"s3a://{S3_BUCKET}/refined"
BUSINESS_BUCKET = f"s3a://{S3_BUCKET}/business"

# Helper functions to get full paths with date partitioning
def get_landing_path(load_date):
    """Get the full path for landing data with date partitioning"""
    return f"{LANDING_BUCKET}/incident_date={load_date}/"

def get_raw_path(load_date):
    """Get the full path for raw data with date partitioning"""
    return f"{RAW_BUCKET}/incident_date={load_date}/"

def get_refined_path(load_date):
    """Get the full path for refined data with date partitioning"""
    return f"{REFINED_BUCKET}/incident_date={load_date}/"

def get_business_path(load_date, table_name):
    """Get the full path for business data with table name and date partitioning"""
    return f"{BUSINESS_BUCKET}/table_name={table_name}/incident_date={load_date}/"