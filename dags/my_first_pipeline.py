from airflow import DAG
from airflow.operators.python import PythonOperator  # , BranchPythonOperator
from datetime import datetime, timedelta
import time


def first():
    print("We just started")


def second():
    time.sleep(15)


def third():
    time.sleep(10)


default_args = {
    "owner": "me",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 12, 10),
}

with DAG(
    dag_id="first_pipeline",
    description="Nothing Special really.",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    task1 = PythonOperator(task_id="First", python_callable=first)
    task2 = PythonOperator(task_id="Second", python_callable=second)
    task3 = PythonOperator(task_id="Third", python_callable=third)

    task1 >> task2 >> task3
