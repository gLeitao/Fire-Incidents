import pytest
import pandas as pd
from pyspark.sql import SparkSession
from src.etl.refined import deduplicate_records

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