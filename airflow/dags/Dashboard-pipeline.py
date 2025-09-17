from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Path to your scripts folder
SCRIPTS_DIR = "D:/News-team-individual dashboards/Python-pipeline"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Dashboard_pipeline",
    default_args=default_args,
    description="Run Python pipeline step by step",
    schedule_interval="@daily",   # This is where the schedule is set
    start_date=datetime(2025, 9, 17),
    catchup=False,
)

# Define tasks
task1 = BashOperator(
    task_id="dataingestion1",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion1.py')}",
    dag=dag,
)

task2 = BashOperator(
    task_id="dataingestion2",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion2.py')}",
    dag=dag,
)

task3 = BashOperator(
    task_id="dataingestion3",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion3.py')}",
    dag=dag,
)

task4 = BashOperator(
    task_id="dataprocessing",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing.py')}",
    dag=dag,
)

task5 = BashOperator(
    task_id="dataprocessing2",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing2.py')}",
    dag=dag,
)

task6 = BashOperator(
    task_id="dataprocessing3",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing3.py')}",
    dag=dag,
)

task7 = BashOperator(
    task_id="ingestdatainbigquery",
    bash_command=f"python {os.path.join(SCRIPTS_DIR, 'ingestdatainbigquery.py')}",
    dag=dag,
)

# Set dependencies (execution order)
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
