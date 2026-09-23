from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}

# Define the DAG
# This automatically defines tasks globally under this DAG without needing to pass dag=dag to every task
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand, Set to None to make it trigger manually
    catchup=False,
) as dag:

    # Fetch environment variables
    # Fetch environment dynamically. If in dev Airflow, it fetches 'dev' from variables.json. Otherwise defaults to 'dev'
    env = Variable.get("env", default_var="dev")
    # Fetch configurations from the uploaded variables.json file rather than Airflow Web UI Admin
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow_flight_booking_bucket")
    bq_project = Variable.get("bq_project", default_var="project-d1694a9a-9dde-4e3c-974")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    # Tables are stored in JSON format, so we set deserialize_json=True to fetch them
    tables = Variable.get("tables", deserialize_json=True)

    # Extract table names from the 'tables' variable
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique batch ID using UUID, converted to string and sliced
    job_batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity
     # Example format: flight-booking-batch-dev-a3bb189e

    # # Task 1: File Sensor for GCS
    # Checks if the flight booking CSV file is uploaded to the bucket. If not present, it won't run
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"flight-booking-analysis/source-{env}/flight_booking.csv",  # Full file path in GCS, Path where the file is expected: flight-booking-analysis/source-dev/Flight_booking.csv
        google_cloud_conn_id="google_cloud_default",  # GCP connection, Built-in Airflow connection profile to authenticate with GCP
        timeout=300,  # Timeout in seconds, Fails task if file doesn't arrive in 300 seconds
        poke_interval=30,  # Time between checks, Rechecks the bucket every 30 seconds
        mode="poke",  # Blocking mode, keeps 1 worker node blocked to check file status
        )

    # Task 2: Submit PySpark job to Dataproc Serverless
    # Details to build the Dataproc Serverless cluster configuration
    batch_details = {
        "pyspark_batch": {
             # URI locating the main python PySpark script in GCS
            "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
             # Arguments passed directly to the python script via command-line
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed),  for cluster build
        },
        "environment_config": {
            "execution_config": {
                "service_account": "1060029794842-compute@developer.gserviceaccount.com",
                "network_uri": f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri": f"projects/{bq_project}/regions/us-central1/subnetworks/default",
            }
        },
    }

     # Task 2: Submit PySpark job to Dataproc Serverless[cite: 1]
    # Creates batches on a serverless cluster, avoiding manual cluster setup in GCP UI[cite: 1]
    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=job_batch_id,
        project_id=bq_project,  # Uses the UUID created earlier
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    # Execution Order: First run file sensor task, then run PySpark task
    file_sensor >> pyspark_task
