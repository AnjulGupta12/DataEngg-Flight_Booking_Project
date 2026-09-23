# Flight Booking Data Engineering Pipeline

## Overview
This project implements a robust, scalable automated data pipeline for processing and analyzing flight booking data. Built entirely on Google Cloud Platform (GCP), it utilizes Apache Airflow (Cloud Composer) for orchestration, Dataproc Serverless for distributed data processing using PySpark, and BigQuery for data warehousing and analytics. 

The project features a strict separation of `dev` and `prod` environments, fully automated via GitHub Actions CI/CD pipelines.

## Architecture & Workflow
1. **Data Ingestion (GCS):** Raw flight booking data (`flight_booking.csv`) is uploaded to a specific Google Cloud Storage (GCS) bucket (`airflow_flight_booking_bucket/flight-booking-analysis/source-{env}`).
2. **Orchestration (Cloud Composer/Airflow):** An Airflow DAG (`flight_booking_dataproc_bq_dag`) runs a `GCSObjectExistenceSensor` to detect the arrival of the raw data file.
3. **Data Processing (Dataproc Serverless):** Once the file is detected, Airflow triggers a PySpark batch job (`spark_transformation_job.py`) on Dataproc Serverless.
4. **Transformations:** The PySpark job reads the CSV, cleans the data, engineers new features (e.g., `is_weekend`, `lead_time_category`, `booking_success_rate`), and aggregates insights.
5. **Data Warehousing (BigQuery):** The transformed dataset and aggregated insights (Route Insights and Origin Insights) are written directly into BigQuery datasets (`flight_data_dev` or `flight_data_prod`).

<p align="center"><img width="468" height="447" alt="flight booking architecture" src="https://github.com/user-attachments/assets/9f6942da-644a-4a03-a7ff-ee63b2d7e2a8" /></p>
<p align="center"><img width="1430" height="194" alt="Flightarc" src="https://github.com/user-attachments/assets/96fd6cae-97d0-400e-b580-4298b839deb9" /></p>


## Tech Stack
* **Orchestration:** Apache Airflow (GCP Cloud Composer 3)
* **Data Processing:** Apache Spark (PySpark), GCP Dataproc Serverless
* **Storage:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery
* **CI/CD:** GitHub Actions
* **Language:** Python 3

## Project Structure
```text
DataEngg-Flight_Booking_Project/
│
├── .github/
│   └── workflows/
│       └── cicd.yaml                  # GitHub Actions pipeline definitions for dev & prod
│
├── variables/
│   ├── dev/
│   │   └── variables.json             # Airflow variables for the DEV environment
│   └── prod/
│       └── variables.json             # Airflow variables for the PROD environment
│
├── airflow_job.py                     # Main Airflow DAG script
├── spark_transformation_job.py        # PySpark data processing and transformation script
├── flight_booking.csv                 # Sample raw data file
└── README.md                          # Project documentation
```

## Data Transformations & Insights
The PySpark job performs several key transformations before loading data into BigQuery:
* **Feature Engineering:** 
  * `is_weekend`: Flags if the flight day is Saturday or Sunday.
  * `lead_time_category`: Categorizes purchase lead times into 'Last-Minute', 'Short-Term', or 'Long-Term'.
  * `booking_success_rate`: Calculates the completion rate based on passenger count.
* **BigQuery Tables Generated:**
  1. `transformed_table`: Contains all granular cleaned and enriched booking data.
  2. `route_insights_table`: Aggregated data grouped by flight route (total bookings, average duration, average stay).
  3. `origin_insights_table`: Aggregated data grouped by booking origin (total bookings, success rate, average purchase lead).

<p align="center"><img width="814" height="611" alt="FlightSpark" src="https://github.com/user-attachments/assets/3c2bd4ef-c0bc-4410-bd0f-487884236396" /></p>

## CI/CD Pipeline (GitHub Actions)
The repository uses GitHub Actions (`cicd.yaml`) to seamlessly deploy code to GCP environments based on the git branch.

### Deployment Triggers:
* **Push to `dev` branch:** Triggers the `upload-to-dev` job.
  * Uploads `variables/dev/variables.json` to the DEV Composer bucket and imports them.
  * Syncs the PySpark script to the designated GCS bucket.
  * Deploys the Airflow DAG to the Cloud Composer `airflow-dev` environment.
* **Push to `main` branch:** Triggers the `upload-to-prod` job.
  * Uploads `variables/prod/variables.json` to the PROD Composer bucket and imports them.
  * Syncs the PySpark script to the designated GCS bucket.
  * Deploys the Airflow DAG to the Cloud Composer `airflow-prod` environment.
 
<p align="center"><img width="612" height="761.6" alt="Github_Action" src="https://github.com/user-attachments/assets/6d214f1c-ffd7-44d3-92c0-07e25a21ee29" /></p>


### Secrets Required for CI/CD:
Ensure the following secrets are configured in your GitHub repository:
* `GCP_SA_KEY`: The JSON key for the GCP Service Account with permissions to access GCS, BigQuery, Composer, and Dataproc.
* `GCP_PROJECT_ID`: Your GCP Project ID (e.g., `project-d1694a9a-9dde-4e3c-974`).

## Setup & Local Development
1. Clone the repository: `git clone <your-repo-url>`
2. Create a `dev` and `main` branch to match the CI/CD requirements.
3. Ensure you have the necessary Google Cloud Service Accounts set up with the following roles:
   * Composer Administrator
   * Dataproc Administrator / Dataproc Worker
   * BigQuery Data Editor
   * Storage Object Admin
4. Modify the `variables/<env>/variables.json` files to match your exact GCP bucket names and environment specifics before pushing code.

## Pipeline Workflow Setup & Execution

Here is the step-by-step breakdown of how the environment was set up and how the pipeline executes:

1. **Airflow Environments:** Setup 2 Google Cloud Composer environments (`airflow-dev` and `airflow-prod`).
   <p align="center"><img width="1112" height="207" alt="image" src="https://github.com/user-attachments/assets/91fa5a61-f1b2-48bf-9497-ee5b37b1517f" /></p>
<br>
2. **GCS Storage:** Created 1 main GCS bucket (`airflow_flight_booking_bucket`). Inside it, created the folder `flight-booking-analysis/`, and within that, set up three subfolders: `source-dev`, `source-prod`, and `spark-job`. Uploaded the raw `flight_booking.csv` to both `source-dev` and `source-prod` folders.
  <p align="center"> <img width="1918" height="286" alt="image" src="https://github.com/user-attachments/assets/175d40aa-b745-4109-9b2c-fd9d1ffbb0a7" /></p>
<br>
3. **BigQuery:** Created two separate BigQuery datasets: `flight_data_dev` and `flight_data_prod`.
<p align="center"><img width="723" height="254" alt="image" src="https://github.com/user-attachments/assets/d973d914-3a7f-48d1-aed2-a3a58e375c16" /></p>
<br>
4. **Version Control:** Created a GitHub repository for the project.
<p align="center"><img width="733" height="663" alt="image" src="https://github.com/user-attachments/assets/cd208d44-6833-42cb-a75b-61f3329863a8" /></p>
<br>
5. **GCP Authentication:** Generated a GCP Service Account Secret JSON key and added it to GitHub Actions Secrets.
   <p align="center"> <img width="1713" height="779" alt="image" src="https://github.com/user-attachments/assets/9a6ec6a2-75ba-44df-9f10-1025107f0247" /></p>
<br>
6. **Local Development:** Set up a VM (or local workspace), linked it to the GitHub repository, and pulled the workspace.
   <p align="center"> <img width="720" height="170" alt="image" src="https://github.com/user-attachments/assets/478fe36a-5b1b-4763-90e4-1b86f414829c" /></p>
<br>
7. **Code Creation:** Authored all necessary scripts, including `variables.json`, the PySpark job (`spark_transformation_job.py`), the Airflow DAG (`airflow_job.py`), and the GitHub Actions `.github/workflows/cicd.yaml` configuration.
    <p align="center"><img width="445" height="412" alt="image" src="https://github.com/user-attachments/assets/32bd5223-3dd7-43c2-a038-e0384a75ba25" /></p>
<br>
8. **Dev Deployment:** Pushed the code to the GitHub repository via the VS Code terminal on the `dev` branch.
   <p align="center"> <img width="853" height="377" alt="image" src="https://github.com/user-attachments/assets/65aee8db-7609-4511-ba78-ea502f5dec3f" /></p>
<br>

9. **CI/CD Orchestration:** The push automatically triggered the GitHub Actions workflow, deploying variables, the PySpark job, and the DAG into the GCP Development environment.
    <p align="center"><img width="1894" height="790" alt="image" src="https://github.com/user-attachments/assets/36c9508e-e37e-4adc-8b21-29b4b6da8b89" /></p>

    
10. **Pull Request:** Opened a Pull Request (PR) on GitHub to merge the `dev` branch into the `main` branch.
   <p align="center"> <img width="840" height="563" alt="image" src="https://github.com/user-attachments/assets/4f2489d2-175c-4e78-801e-10ca308a5124" /></p>
   <br>
    <p align="center"><img width="653" height="157" alt="image" src="https://github.com/user-attachments/assets/6b65abdd-2ea0-45db-803d-8e02c69275c0" /></p>
    
<br>
<br>

11. **Production Deployment:** Approved the PR, merging the `dev` code into `main`. This triggered the GitHub Actions pipeline for Production, deploying all assets automatically into the `prod` GCP environment.
<p align="center"><img width="803" height="650" alt="image" src="https://github.com/user-attachments/assets/0e97696f-faad-42cd-9bbb-2e7d91550271" /></p>
<bR> 
<br>
  <p align="center">  <img width="740" height="704" alt="image" src="https://github.com/user-attachments/assets/218e4016-a32a-4392-87f3-46ba0dbaebc5" /></p>
