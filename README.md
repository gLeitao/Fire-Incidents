# Fire Incidents Data Warehouse Project

This project deploys an AWS-based data pipeline to manage and analyze fire incident information for San Francisco. It automates the ingestion, transformation, and storage of data in S3 and PostgreSQL, orchestrated by AWS Step Functions. The pipeline provides insights into fire incidents, including causes, locations, and safety outcomes.


## Deploying the AWS Infrastructure

### Pre-deployment instructions:
- Set up your AWS account.
- Set up a PostgreSQL instance (RDS or self-managed) and run the schema in `sql/create_tables.sql`.
- Store PostgreSQL and S3 credentials as secrets in AWS Secrets Manager (referenced in `src/config/config.py`).

### Deploying the Pipeline
- Upload the daily fire incidents CSV to your S3 landing zone bucket (e.g., `s3://your-bucket/landing/incident_date={yyyy-mm-dd}/`).
- Create AWS Glue jobs for each ETL/business/load script (`src/etl/raw.py`, `src/etl/refined.py`, `src/etl/business/*.py`, `src/loads/*.py`, `src/utils/data_quality.py`).
- Use the Step Function definition in `src/aws/aws_step_function_definition.json` to orchestrate the workflow.
- Ensure IAM roles for Glue and Step Functions have permissions for S3, Secrets Manager, and RDS.
- Trigger the Step Function or run jobs manually for a given date partition.

## Architecture

The architecture is structured as follows: outside the AWS context, a Python script uploads the source CSV file to S3 (landing layer). The Step Function orchestrates the workflow, triggering Glue jobs for each ETL stage, which process and store data in S3 (raw, refined, business) and finally load it into PostgreSQL.

![Step Function Pipeline](img/step_function.png)

## Project Structure
```
src/
├── aws/                    # AWS Step Functions definition
├── config/                 # Configuration files (uses AWS Secrets Manager)
├── etl/                    # ETL scripts
│   ├── raw.py             # Raw data processing (deduplication, initial cleaning)
│   ├── refined.py         # Refined data processing (standardization, date parsing)
│   └── business/          # Business layer scripts (dimension/fact transformations)
│       ├── detection_data.py  # Detection dimension business layer
│       ├── incident_data.py   # Incident dimension business layer
│       ├── location_data.py   # Location dimension business layer
│       └── fire_incident_data.py # Fact table business layer
├── loads/                  # Data loading scripts (to PostgreSQL)
│   ├── load_detection.py  # Detection dimension loading
│   ├── load_incident.py   # Incident dimension loading
│   ├── load_location.py   # Location dimension loading
│   └── load_fire_incident.py # Fact table loading
└── utils/                 # Utility functions
    ├── data_quality.py    # Data quality checks
    └── logging_utils.py   # Logging setup

sql/
└── create_tables.sql      # Database schema definition
```

## Data Modeling Strategy

The data modeling approach for this project is based on the star schema design, with a central fact table and several dimension tables. This approach maximizes performance for analytical queries and BI dashboards.

### Dimensions
- **Location Dimension** (`dim_location`): Address, city, zipcode, districts, point, incident_date
- **Detection Dimension** (`dim_detection`): Detector and suppression system details, incident_date
- **Incident Dimension** (`dim_incident`): Incident type, description, incident_date

### Fact Table
- **Fire Incident Fact** (`fact_fire_incident`): Links all dimensions, contains incident date and metrics

## ETL Pipeline Workflow

1. **Raw Layer** (`raw.py`):
   - Ingests CSV from S3 landing zone
   - Deduplicates and writes to Parquet in the raw zone
2. **Refined Layer** (`refined.py`):
   - Cleans and standardizes data, trims fields, ensures date formats
   - Writes to Parquet in the refined zone
3. **Business Layer** (`etl/business/*.py`):
   - Each business script selects and transforms the relevant fields for its dimension/fact
   - Writes partitioned Parquet files to the business zone
4. **Loaders** (`loads/*.py`):
   - Each loader reads from the business zone and loads data into the corresponding PostgreSQL table
   - Uses upsert logic, batching, and partitioning by date

### Monitoring
- The pipeline logs each step and status to CloudWatch and PostgreSQL.
- Data quality checks are implemented in `src/utils/data_quality.py` and include:
  - Null value checks
  - Data type validation
  - Unique key checks
  - Date format validation
  - Numeric range checks
  - Boolean consistency checks

## Sample Reports & Visualizations

Below are sample visualizations generated from the data warehouse, demonstrating the analytical capabilities of the pipeline:

### 1. Detector Alerted Occupants (Pie Chart)
![Detector Alerted Pie](img/detector_alerted_pie.png)
*Shows the proportion of incidents where detectors alerted occupants, providing insight into fire safety effectiveness.*

### 2. Fire Fatalities and Injuries
![Fatalities and Injuries](img/fatalities_injuries.png)
*Visualizes the number of fatalities and injuries in fire incidents, helping to identify trends and areas for safety improvement.*

### 3. Fire Causes
![Fire Causes](img/fire_causes.png)
*Breaks down the main causes of fires, supporting targeted prevention strategies.*

### 4. Fires by Neighborhood
![Fires by Neighborhood](img/fires_by_neighborhood.png)
*Maps the distribution of fire incidents across neighborhoods, useful for resource allocation and risk assessment.*

All these charts are produced using the data warehouse tables and can be reproduced.

## Example Analytical Queries

Here are some example queries you can run on the PostgreSQL data warehouse:

### Fire Incidents by Week
```sql
SELECT date_trunc('week', incident_date) AS week_start, COUNT(*) AS total_incidents
FROM fact_fire_incident
GROUP BY week_start
ORDER BY week_start;
```
![Fatalities and Injuries](img/fatalities_injuries.png)
*Visualizes the number of fatalities and injuries in fire incidents, helping to identify trends and areas for safety improvement.*

### Top Neighborhoods by Fire Incidents
```sql
SELECT neighborhood_district, COUNT(*) AS incident_count
FROM dim_location
GROUP BY neighborhood_district
ORDER BY incident_count DESC
LIMIT 10;
```
![Fires by Neighborhood](img/fires_by_neighborhood.png)
*Maps the distribution of fire incidents across neighborhoods, useful for resource allocation and risk assessment.*

### Most Common Fire Causes
```sql
SELECT ignition_cause, COUNT(*) AS cause_count
FROM dim_incident
GROUP BY ignition_cause
ORDER BY cause_count DESC
LIMIT 5;
```
![Fire Causes](img/fire_causes.png)
*Breaks down the main causes of fires, supporting targeted prevention strategies.*

## Assumptions and Notes
- Only the dimensions and fact tables present in the codebase are supported.
- All ETL and loader scripts are modular, with logging and error handling.
- Data is partitioned by date for efficient incremental loads.
- Credentials and configuration are externalized for security and flexibility.