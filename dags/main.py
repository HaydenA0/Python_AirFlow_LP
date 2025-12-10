from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
import os
from scrap import scrap_main
from preprocess import preprocess_main


with DAG(
    dag_id="Scrapper",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as producer_dag:

    scrap_data = PythonOperator(
        task_id="scrap_data", python_callable=scrap_main, outlets=[DATASET_PATH]
    )


# with DAG(
#     dag_id="Analyser",
#     start_date=datetime(2025, 1, 1),
#     schedule=[datset_path],
#     catchup=False,
# ) as consumer_dag:
#
#     process_file = PythonOperator(task_id="process_new_data", python_callable=scrap)
