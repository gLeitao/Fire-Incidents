import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import json
import sys
import logging

# Create a mock logger
mock_logger = MagicMock()
mock_logger.info = MagicMock()
mock_logger.error = MagicMock()
mock_logger.warning = MagicMock()
mock_logger.debug = MagicMock()

# Mock logging before any imports
class MockFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        # Override to do nothing
        pass

    def emit(self, record):
        # Override to do nothing
        pass

# Mock AWS Glue before any imports
class MockGlueUtils:
    @staticmethod
    def getResolvedOptions(args, options):
        return {opt: f"mock_{opt}" for opt in options}

sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.utils'] = MockGlueUtils()

# Mock AWS credentials and secrets before importing any other modules
mock_secrets = {
    'fire-incidents/postgres': json.dumps({
        'host': 'localhost',
        'port': '5432',
        'dbname': 'test_db',
        'username': 'test_user',
        'password': 'test_pass'
    }),
    'fire-incidents/s3': json.dumps({
        'landing_bucket': 'test-landing',
        'raw_bucket': 'test-raw',
        'refined_bucket': 'test-refined',
        'business_bucket': 'test-business',
        'region': 'us-east-1'
    })
}

def mock_get_secret_value(SecretId):
    return {'SecretString': mock_secrets[SecretId]}

# Create mock boto3 client
mock_client = MagicMock()
mock_client.get_secret_value = MagicMock(side_effect=mock_get_secret_value)

# Create mock session
mock_session = MagicMock()
mock_session.client.return_value = mock_client

def mock_setup_logger(name):
    return mock_logger

# Apply all necessary patches before importing the modules
patches = [
    patch('boto3.session.Session', return_value=mock_session),
    patch('logging.FileHandler', MockFileHandler),
    patch('utils.logging_utils.setup_logger', side_effect=mock_setup_logger)
]

for p in patches:
    p.start()

from pyspark.sql import SparkSession
from etl.refined import deduplicate_records

def make_spark_df(data):
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    return spark, spark.createDataFrame(pd.DataFrame(data))

def test_deduplicate_records():
    data = [
        {"incident_number": "A1", "incident_date": "2023-01-01", "value": "foo"},
        {"incident_number": "A1", "incident_date": "2023-01-01", "value": "foo"},  # duplicate row
        {"incident_number": "A2", "incident_date": "2023-01-02", "value": "bar"},
        {"incident_number": "A2", "incident_date": "2023-01-02", "value": "baz"},  # duplicate incident_number, different value
        {"incident_number": "A3", "incident_date": "2023-01-03", "value": "baz"},
    ]
    spark, sdf = make_spark_df(data)
    # Call deduplicate_records
    result_df = deduplicate_records(sdf)
    # Collect result
    result = result_df.toPandas()
    # Should have 3 unique incident_number rows
    assert result['incident_number'].nunique() == 3
    # Should have only one row for A1, one for A2, one for A3
    assert set(result['incident_number']) == {"A1", "A2", "A3"}
    spark.stop()

# Clean up patches after tests
@pytest.fixture(autouse=True)
def cleanup_patches():
    yield
    for p in patches:
        p.stop() 