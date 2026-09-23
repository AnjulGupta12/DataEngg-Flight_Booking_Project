# Flight Booking Analysis Pipeline

## A) Overview
An automated data engineering pipeline built on Google Cloud Platform that processes flight booking data[cite: 1]. The project utilizes a fully automated CI/CD pipeline to deploy code to distinct environments[cite: 1].

## B) Explain Project
*   **Business Need:** Process 16,654 raw flight booking records to track passenger metrics, booking success rates, and route insights[cite: 1].
*   **Structure:** The project maintains strict separation between `dev` (development) and `prod` (production) environments[cite: 1].
*   **Key Feature:** Uses a Dataproc Serverless cluster to run PySpark transformations, meaning compute resources are only utilized when data is actively being processed[cite: 1].

## C) Technology Stack
*   **Version Control & CI/CD:** GitHub, GitHub Actions[cite: 1].
*   **Data Lake:** Google Cloud Storage (GCS)[cite: 1].
*   **Orchestration:** Apache Airflow (via Google Cloud Composer)[cite: 1].
*   **Data Processing:** PySpark on Dataproc Serverless[cite: 1].
*   **Data Warehouse:** Google BigQuery[cite: 1].

## D) Project Architecture/Structure
1.  **Source:** `Flight_booking.csv` lands in a GCS bucket[cite: 1].
2.  **Monitor:** Airflow File Sensor checks for data arrival[cite: 1].
3.  **Process:** Dataproc Serverless cluster spins up and runs the PySpark transformation job[cite: 1].
4.  **Load:** Transformed data and aggregated insights are written to BigQuery[cite: 1].

## E) Workflow
1. Developer pushes code to the `dev` or `main` branch[cite: 1].
2. GitHub Actions (`cicd.yaml`) triggers[cite: 1].
3. Workflow authenticates to GCP, uploads `variables.json` to the Airflow bucket, syncs the PySpark script, and uploads the Airflow DAG[cite: 1].
4. Once data is manually uploaded to the GCS source folder, the Airflow DAG triggers the data processing steps automatically[cite: 1].
5. Processed data is saved into BigQuery as `transformed_flight_data`, `route_insights`, and `origin_insights`[cite: 1].

## F) Prerequisites (HOW and WHAT)
*   **Airflow Environment:** Create a Google Cloud Composer environment. This automatically provisions the bucket where Airflow monitoring files will sit[cite: 1].
*   **GCS Bucket:** Create a bucket named `airflow-test-projects-gds-dev` to hold the source data and Spark job files[cite: 1].
*   **BigQuery Dataset:** In BigQuery Studio, create datasets (e.g., `flight_data_dev`) in the `us-central1` location[cite: 1]. **Do not create any tables**—the Spark job handles table creation[cite: 1].
*   **GitHub Setup:** Create a repository, clone it locally (`git clone <URL>`), and set up Git CLI[cite: 1]. Create a development branch using `git checkout -b dev`[cite: 1]. 
*   **Secret Keys:** Under GitHub Repository Settings -> Secrets and variables -> Actions, create repository secrets for `GCP_PROJECT_ID` and `GCP_SA_KEY` (the downloaded JSON credentials for your service account)[cite: 1].

## G) Explain All Files
*   `.github/workflows/cicd.yaml`: Automates GCP authentication, variable imports, and file synchronization based on branch pushes[cite: 1].
*   `variables/dev/variables.json` & `variables/prod/variables.json`: Stores environment configurations like project IDs, bucket paths, and table names[cite: 1].
*   `airflow-job.py`: Defines the Airflow DAG, including the `GCSObjectExistenceSensor` and `DataprocCreateBatchOperator`[cite: 1].
*   `spark-transformation-job.py`: PySpark script that adds derived columns (e.g., `is_weekend`), performs group-by aggregations, and writes results to BigQuery[cite: 1].
*   `Flight_booking.csv`: The raw source data containing 16,654 records[cite: 1].

```

---

## Fully Commented Code Files

Here are the scripts with detailed comments injected directly into the code based exactly on the logical breakdown provided in the reference document.

### 1. `.github/workflows/cicd.yaml`

```yaml
# cicd.yaml helps perform all automated actions when code is pushed[cite: 1]
# If successful, a PR is generated. Once approved by seniors, it saves to the main branch[cite: 1].
name: Flight Booking CICD

# Defines the trigger for the actions[cite: 1]
on:
  push:
    branches:
      - dev   # All development happens on the dev branch[cite: 1]
      - main

jobs:
  # Job 1: Upload to Dev Environment[cite: 1]
  upload-to-dev:
    # Runs only if the push happens on the 'dev' branch[cite: 1]
    if: github.ref == 'refs/heads/dev'
    runs-on: ubuntu-latest # Uses standard GitHub runner OS[cite: 1]

    steps:
      # Step 1: Scan and checkout the repository code[cite: 1]
      - name: Checkout Code
        uses: actions/checkout@v3

      # Step 2: Authenticate to GCP so GitHub can talk to GCP[cite: 1]
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v1
        with:
          # Uses the JSON service account key stored in GitHub secrets[cite: 1]
          credentials_json: ${{secrets.GCP_SA_KEY}}

      # Step 3: Setup Google Cloud SDK/Terminal in the runner[cite: 1]
      - name: Setup Google Cloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}

      # Step 4: Upload variables.json file to the Composer GCS bucket using gsutil[cite: 1]
      - name: Upload Variables JSON to GCS
        run: |
          gsutil cp 04-Assets/variables/dev/variables.json gs://us-central1-airflow-dev-22033485-bucket/data/dev/variables.json

      # Step 5: Import variables into Airflow-DEV (Similar to manually creating variables in Airflow Admin UI)[cite: 1]
      - name: Import Variable into Airflow-DEV
        run: |
          gcloud composer environments run airflow-dev \
          --location us-central1 \
          variables import -- /home/airflow/gcs/data/dev/variables.json

      # Step 6: Sync PySpark job to GCS so Dataproc can access it[cite: 1]
      - name: Upload Spark Job to GCS
        run: |
          gsutil cp 04-Assets/spark_transformation_job.py gs://airflow-test-projects-gds-dev/flight-booking-analysis/spark-job/

      # Step 7: Upload Airflow DAG directly to the DEV Environment DAG Folder[cite: 1]
      - name: Upload Airflow DAG to DEV Environment
        run: |
          gcloud composer environments storage dags import \
          --environment airflow-dev \
          --location us-central1 \
          --source 04-Assets/airflow-job.py

  # Job 2: Upload to Prod Environment (Triggers on main branch push)[cite: 1]
  upload-to-prod:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3
        
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{secrets.GCP_SA_KEY}}
          
      - name: Setup Google Cloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}
          
      - name: Upload Variables JSON to GCS
        run: |
          gsutil cp 04-Assets/variables/prod/variables.json gs://us-central1-airflow-prod-45e33290-bucket/data/prod/variables.json
          
      - name: Import variables into Airflow-PROD
        run: |
          gcloud composer environments run airflow-prod \
          --location us-central1 \
          variables import -- /home/airflow/gcs/data/prod/variables.json
          
      - name: Upload Spark Job to GCS
        run: |
          gsutil cp 04-Assets/spark_transformation_job.py gs://airflow-test-projects-gds-prod/flight-booking-analysis/spark-job/
          
      - name: Upload Airflow DAG to PROD Environment
        run: |
          gcloud composer environments storage dags import \
          --environment airflow-prod \
          --location us-central1 \
          --source 04-Assets/airflow-job.py

```

### 2. `airflow-job.py`

```python
from datetime import datetime, timedelta
import uuid # Imported to create unique Job Batch IDs[cite: 1]
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# DAG default arguments[cite: 1]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 15),
}

# Define the DAG using the 'with' context manager[cite: 1]
# This automatically defines tasks globally under this DAG without needing to pass dag=dag to every task[cite: 1]
with DAG(
    dag_id='flight-booking-dataproc-bq-dag',
    default_args=default_args,
    schedule_interval=None, # Set to None to make it trigger manually[cite: 1]
    catchup=False
) as dag:
    
    # Fetch environment dynamically. If in dev Airflow, it fetches 'dev' from variables.json. Otherwise defaults to 'dev'[cite: 1]
    env = Variable.get("env", default_var="dev")
    # Fetch configurations from the uploaded variables.json file rather than Airflow Web UI Admin[cite: 1]
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-test-projects-gds-dev")
    bq_project = Variable.get("bq_project", default_var="project-d1684aga-9dde")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    
    # Tables are stored in JSON format, so we set deserialize_json=True to fetch them[cite: 1]
    tables = Variable.get("tables", deserialize_json=True)
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique batch ID using UUID, converted to string and sliced[cite: 1]
    # Example format: flight-booking-batch-dev-a3bb189e[cite: 1]
    job_batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    # Task 1: File Sensor[cite: 1]
    # Checks if the flight booking CSV file is uploaded to the bucket. If not present, it won't run[cite: 1]
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        # Path where the file is expected: flight-booking-analysis/source-dev/Flight_booking.csv[cite: 1]
        object=f"flight-booking-analysis/source-{env}/Flight_booking.csv",
        # Built-in Airflow connection profile to authenticate with GCP[cite: 1]
        google_cloud_conn_id="google_cloud_default",
        timeout=300, # Fails task if file doesn't arrive in 300 seconds[cite: 1]
        poke_interval=30, # Rechecks the bucket every 30 seconds[cite: 1]
        mode="poke" # Blocking mode: keeps 1 worker node blocked to check file status[cite: 1]
    )

    # Details to build the Dataproc Serverless cluster configuration[cite: 1]
    batch_details = {
        "pyspark_batch": {
            # URI locating the main python PySpark script in GCS[cite: 1]
            "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark-transformation-job.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            # Arguments passed directly to the python script via command-line[cite: 1]
            "args": [
                f"--env={env}",
                f"--bq-project={bq_project}",
                f"--bq-dataset={bq_dataset}",
                f"--transformed-table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2", # Specific Dataproc version required for cluster build[cite: 1]
        },
        "environment_config": {
            "execution_config": {
                "service_account": "1060-compute@developer.gserviceaccount.com",
                "network_uri": f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri": f"projects/{bq_project}/regions/us-central1/subnetworks/default"
            }
        }
    }

    # Task 2: Submit PySpark job to Dataproc Serverless[cite: 1]
    # Creates batches on a serverless cluster, avoiding manual cluster setup in GCP UI[cite: 1]
    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=job_batch_id, # Uses the UUID created earlier[cite: 1]
        project_id=bq_project,
        region="us-central1",
        gcp_conn_id="google_cloud_default"
    )

    # Execution Order: First run file sensor task, then run PySpark task[cite: 1]
    file_sensor >> pyspark_task

```

### 3. `spark-transformation-job.py`

```python
import argparse # To pass command-line arguments passed by DataprocCreateBatchOperator[cite: 1]
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys # Importing system files[cite: 1]

# Initialize and configure logging[cite: 1]
logging.basicConfig(
    level=logging.INFO,
    # Formatting logs to include timestamp, level, and message (e.g., 2026-09-05 19:04:26 - INFO - Input path resolved)[cite: 1]
    format="%(asctime)s - %(levelname)s - %(message)s" 
)
logger = logging.getLogger(__name__)

# Main job process definition[cite: 1]
def job_process(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    try:
        # Initialize Spark Session working on Hive implementation[cite: 1]
        spark = SparkSession.builder \
            .appName("Flight Booking Analysis") \
            .config("spark.sql.catalogImplementation", "hive") \
            .getOrCreate()
        logger.info("Spark session initialized.")

        # Resolve GCS path based on the environment to pick correct CSV file[cite: 1]
        input_path = f"gs://airflow-test-projects-gds-{env}/flight-booking-analysis/source-{env}/Flight_booking.csv"
        logger.info(f"Input path resolved: {input_path}")

        # Read the raw CSV data, inferschema applied[cite: 1]
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from GCS")
        logger.info("Starting data transformations.")

        # Add derived columns utilizing lit for standard python value conversion to Spark Column object[cite: 1]
        transformed_data = data.withColumn(
            # is_weekend: 1 if flight_day is Sat or Sun, else 0[cite: 1]
            "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
        ).withColumn(
            # lead_time_category: Categorizes purchase lead time into Last Minute, Short-Term, Long-Term[cite: 1]
            "lead_time_category", when(col("purchase_lead") < 7, lit("Last Minute"))
            .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 90), lit("Short-Term"))
            .otherwise(lit("Long-Term"))
        ).withColumn(
            # booking_success_rate: Calculates success rate using an expression[cite: 1]
            "booking_success_rate", expr("booking_complete / num_passengers")
        )

        # Aggregation 1: Route Insights[cite: 1]
        # Groups data by route to find total bookings, average flight duration, and average stay length[cite: 1]
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_booking"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

        # Aggregation 2: Origin Insights[cite: 1]
        # Groups data by booking origin to find total bookings, success rate, and average purchase lead[cite: 1]
        origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_booking"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )

        logger.info("Data transformations completed.")

        # Write transformed data back to BigQuery using the 'direct' method to avoid intermediate load issues[cite: 1]
        # Overwrite mode replaces table data on fresh runs[cite: 1]
        logger.info(f"Writing transformed data to BigQuery table: {bq_project}:{bq_dataset}.{transformed_table}")
        transformed_data.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{transformed_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        # Write route insights to BigQuery[cite: 1]
        logger.info(f"Writing route insights to BigQuery table: {bq_project}:{bq_dataset}.{route_insights_table}")
        route_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        # Write origin insights to BigQuery[cite: 1]
        logger.info(f"Writing origin insights to BigQuery table: {bq_project}:{bq_dataset}.{origin_insights_table}")
        origin_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        logger.info("Data written to Bigquery successfully.")

    # Prints message on failure[cite: 1]
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        # Mandatory final step: Stop the Spark session[cite: 1]
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    # Parser object to pass arguments natively (not using Airflow XCom)[cite: 1]
    parser = argparse.ArgumentParser(description="Process flight booking data & write to bigquery.")
    
    # Setting required=True means these parameters must be supplied[cite: 1]
    parser.add_argument("--env", required=True, help="Env (e.g. prod, dev)")
    parser.add_argument("--bq-project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq-dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--transformed-table", required=True, help="Bigquery table for transformed data")
    parser.add_argument("--route_insights_table", required=True, help="Bigquery table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="Bigquery table for origin insights")
    
    args = parser.parse_args()

    # Call job_process function with the parsed arguments[cite: 1]
    job_process(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table
    )

```

### 4. `variables/dev/variables.json`

```json
{
  "env": "dev",
  "gcs_bucket": "airflow-test-projects-gds-dev",
  "bq_project": "project-d1684aga-9dde-3x-974",
  "bq_dataset": "flight_data_dev",
  "tables": {
    "transformed_table": "transformed_flight_data_dev",
    "route_insights_table": "route_insights_dev",
    "origin_insights_table": "origin_insights_dev"
  }
}

```






Here is the video script, following the exact structure you requested, based on the project documentation.

## Video Script: Flight Booking Data Pipeline

**A) Overview**
"Hello everyone! Today, I'll be walking you through my latest data engineering project: a Flight Booking Data Pipeline. This project processes raw flight booking data and automates the entire workflow from Google Cloud Storage to BigQuery using a fully automated CI/CD pipeline."

**B) Explain Project: Business Need, Structure, & Key Features**
"First, let's talk about the business need. The goal is to analyze over 16,600 flight booking records to understand passenger behavior, booking success rates, and route popularity.
For the structure, this project is professionally separated into two environments: 'dev' for development and 'prod' for the live project.
The key feature of this pipeline is its serverless architecture—we are using a Dataproc Serverless cluster that spins up automatically only when data needs to be processed."

**C) Technology Stack**
"For the technology stack, we are utilizing GitHub for version control and CI/CD actions. On Google Cloud Platform, we use Google Cloud Storage (or GCS) for our data lake, Google Cloud Composer for Apache Airflow orchestration, Dataproc Serverless for running our PySpark jobs, and finally, BigQuery as our data warehouse."

**D) Project Architecture/Structure**
"Let's look at the architecture. Our raw data, `Flight_booking.csv`, lands in a GCS bucket. Airflow constantly monitors this bucket using a File Sensor. Once the file is detected, Airflow triggers an Apache Spark job on a Dataproc Serverless cluster. Spark reads the CSV, infers the schema, performs data transformations, and writes the output into three separate tables in BigQuery: a transformed data table, a route insights table, and an origin insights table."

**E) Workflow**
"The workflow starts the moment a developer pushes code to GitHub. If pushed to the `dev` branch, GitHub Actions authenticate with GCP, upload our variables, and sync our Airflow DAGs and PySpark scripts to the dev environment. Once the code is live, you just upload the `Flight_booking.csv` file into the GCS bucket. Airflow senses the file, kicks off the PySpark job, and BigQuery is automatically populated with the fresh data."

**F) Prerequisites (HOW and WHAT)**
"Before running this, you need to set up a few prerequisites:

* **Create an Airflow Environment:** Set this up in Google Cloud Composer to automatically generate the bucket where we will store our DAGs.


* **Create GCS Buckets:** We need a bucket named `airflow-test-projects-gds` (with dev/prod suffixes) to hold our source files and Spark jobs.


* **Create BigQuery Datasets:** Go to BigQuery Studio and create two datasets (e.g., `flight_data_dev` and `flight_data_prod`) in the `us-central1` location. Do not create the tables; our PySpark job will do that automatically.


* **Setup GitHub & Branches:** Create a GitHub repository and clone it to your VS Code using `git clone`. Create a new branch for development using `git checkout -b dev` because all initial development happens here, not on main.


* **Create Secret Keys:** Go to your GitHub repository settings, navigate to Secrets and Variables, and add two repository secrets: `GCP_PROJECT_ID` and `GCP_SA_KEY` (which contains your Google Cloud Service Account JSON credentials)."



**G) Explain All Files**
"Finally, let's break down the core files in this project:

1. `.github/workflows/cicd.yaml`: This file handles our GitHub Actions, triggering deployments depending on whether we push to the dev or main branch.


2. `variables/dev/variables.json` & `prod/variables.json`: These hold our environment-specific configurations like bucket names and BigQuery table names.


3. `airflow-job.py`: This is our DAG file. It creates a unique job batch ID, uses a sensor to wait for the CSV file, and then triggers the Dataproc Batch Operator.


4. `spark-transformation-job.py`: The heart of our logic. It reads the CSV, adds derived columns like `is_weekend` and `lead_time_category`, calculates booking success rates, aggregates insights, and overwrites the data directly into BigQuery."



---

## README.md

```markdown
