from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator
import os


SCRIPTS_DIR = "D:/News-team-individual dashboards/Python-pipeline"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Use pendulum for timezone
local_tz = pendulum.timezone("Asia/Karachi")

dag = DAG(
    dag_id="Dashboard_pipeline",
    default_args=default_args,
    description="Run Python pipeline step by step",
    schedule_interval="0 10,18 * * *",  # runs at 10 AM and 6 PM
    start_date=datetime(2025, 9, 17, tzinfo=local_tz),
    catchup=False,
)

task1 = BashOperator(task_id="dataingestion1", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion1.py')}", dag=dag)
task2 = BashOperator(task_id="dataingestion2", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion2.py')}", dag=dag)
task3 = BashOperator(task_id="dataingestion3", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataingestion3.py')}", dag=dag)
task4 = BashOperator(task_id="dataprocessing", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing.py')}", dag=dag)
task5 = BashOperator(task_id="dataprocessing2", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing2.py')}", dag=dag)
task6 = BashOperator(task_id="dataprocessing3", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'dataprocessing3.py')}", dag=dag)
task7 = BashOperator(task_id="ingestdatainbigquery", bash_command=f"python {os.path.join(SCRIPTS_DIR, 'ingestdatainbigquery.py')}", dag=dag)

# Set dependencies
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
