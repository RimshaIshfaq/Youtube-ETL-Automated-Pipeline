

Overview
This documentation provides a comprehensive guide to a data pipeline that daily ingests YouTube video IDs, tags them based on a specific logic, retrieves analytics data for each video using the YouTube Data API, and loads the processed information into a BigQuery table.

The pipeline is orchestrated by Apache Airflow and is containerized using Docker for a consistent and reproducible environment. This project ensures that business stakeholders have access to up-to-date video performance metrics for analysis and decision-making.

Key Components
Apache Airflow: The workflow orchestration tool that schedules, manages, and monitors the entire data pipeline. It runs inside a Docker container.

Docker: Used to package the Airflow environment and all its dependencies, ensuring the pipeline runs consistently across different machines.

YouTube Data API: The source of truth for video analytics data (e.g., views, likes, comments).

Google BigQuery: The destination for the processed data. It serves as the data warehouse for all historical and current video metrics.

Python: The primary programming language used for the Airflow DAGs (Directed Acyclic Graphs) and the data fetching logic.

Google Cloud Platform (GCP): The cloud infrastructure hosting BigQuery and providing the necessary service accounts for authentication.

Project Structure
.
├── .env                  # Environment variables for Docker
├── dags/                 # Airflow DAGs
│   ├── __init__.py
│   ├── video_analytics_dag.py
│   └── scripts/
│       ├── get_video_ids.py  # Script to get video IDs and tag them
│       └── fetch_analytics.py # Script to fetch data from YouTube API
├── docker-compose.yaml   # Docker configuration for Airflow
├── Dockerfile            # Custom Dockerfile for dependencies
├── requirements.txt      # Python dependencies
└── README.md             # This document
Local Setup
This project uses Docker to create a self-contained environment. Ensure you have Docker and Docker Compose installed on your machine.

Clone the Repository:

Bash

git clone https://github.com/your-username/your-repo.git
cd your-repo
Configure Environment Variables:
Create a .env file in the root directory. This file will store your sensitive information.

Ini, TOML

# .env
AIRFLOW_UID=50000
GCP_PROJECT_ID=<your-gcp-project-id>
BIGQUERY_DATASET_NAME=<your-bq-dataset-name>
YOUTUBE_API_KEY=<your-youtube-api-key>
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp_credentials.json
Create GCP Service Account and Credentials:

In the GCP Console, create a new Service Account.

Grant the service account the BigQuery Data Editor role to allow it to create and write to tables.

Generate a new JSON key for this service account and save it as gcp_credentials.json in the dags/ folder. This file is referenced by the GOOGLE_APPLICATION_CREDENTIALS environment variable.

Build and Start the Docker Containers:
The docker-compose.yaml file defines the services needed to run Airflow (web server, scheduler, and a database).

Bash

# Initialize the Airflow database
docker compose up airflow-init

# Start all services in detached mode
docker compose up -d
Access the Airflow UI:
Open your web browser and navigate to http://localhost:8080. The default credentials are airflow for both the username and password.

Data Pipeline Process (DAG)
The core logic of the pipeline is defined in dags/video_analytics_dag.py. The DAG runs daily at 10:00 AM.

DAG Name: youtube_video_analytics
Schedule: 0 10 * * * (Daily at 10:00 AM)
Tasks:
extract_video_ids_and_tag

Description: This task executes a Python script (get_video_ids.py) that identifies the video IDs to process. This could be a list from a file, a database, or even another API call. The script also applies a tag to each video ID (e.g., trending, new_upload).

Operator: PythonOperator

Output: Pushes a list of video IDs and their tags to Airflow's XComs for downstream tasks.

fetch_video_analytics

Description: This task retrieves the tagged list of video IDs from the previous task's XCom. For each video ID, it makes a call to the YouTube Data API to fetch analytics data (e.g., viewCount, likeCount, commentCount).

Operator: PythonOperator

Output: Returns a JSON object containing the raw analytics data.

load_to_bigquery

Description: This task takes the raw analytics data from the previous step. It then transforms this data into a structured format and loads it into a dedicated BigQuery table. The table schema should be defined to accommodate the video ID, its tag, and all the relevant metrics. The operation is an upsert to handle updates for existing video IDs and insertions for new ones.

Operator: BigQueryInsertJobOperator

Input: The JSON object from fetch_video_analytics.

Destination Table: [GCP_PROJECT_ID].[BIGQUERY_DATASET_NAME].video_analytics