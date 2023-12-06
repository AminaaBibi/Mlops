from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Import DVC
import dvc.api
import os

# Define default_args dictionary to set the default parameters of the DAG
default_args = {
    'owner': 'amina_bibi',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='DAG for ETL and Data Versioning with DVC',
    schedule_interval=timedelta(days=1),  # set your desired schedule interval
)

# Define functions for ETL steps

def extract_data():
    # Example: Fetch data from a CSV file
    data_source_path = "/home/amina/airflow/dags/iris.csv"
    # Your extraction logic using the data_source
    with open(data_source_path, 'r') as file:
        data = file.read()
    return data

def transform_data(data):
    # Example: Transform the data by converting to uppercase
    transformed_data = data.upper()
    return transformed_data

def load_data_to_github(transformed_data):
    # Example: Save the transformed data to a temporary file
    temp_file_path = "/home/amina/airflow/dags/temp_file.txt"
    with open(temp_file_path, 'w') as file:
        file.write(transformed_data)
    
    # Example: Use DVC to version and push the data to GitHub
    os.system(f"dvc add {temp_file_path}")
    os.system("dvc push")
    os.remove(temp_file_path)  # Clean up the temporary file

# Define the tasks in the DAG

# Task to extract data
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task to transform data
transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,  # Pass the output of extract_data to this task
    python_callable=transform_data,
    dag=dag,
)

# Task to load data to GitHub using DVC
load_to_github_task = PythonOperator(
    task_id='load_data_to_github',
    provide_context=True,  # Pass the output of transform_data to this task
    python_callable=load_data_to_github,
    dag=dag,
)

# Define the order of execution for tasks
extract_task >> transform_task >> load_to_github_task
