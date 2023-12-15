from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import gdown  # for downloading from Google Drive
import os

# Import DVC
import dvc.api
default_args = {
    'owner': 'amina_bibi',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#DAG
dag = DAG(
    'etl_dag2',
    default_args=default_args,
    description='DAG for ETL and Data Versioning with DVC',
    schedule_interval=timedelta(days=1),  
)

# Define functions for ETL steps

def download_data():
    # Google Drive link to the data.csv file
    file_url = "https://drive.google.com/file/d/10iZbAEEw7t8rtaD39OiuE9qPB8eSF3OJ/view?usp=sharing"
    
    # Output path for the downloaded file
    output_path = "/home/amina/airflow/dags/data.csv"
    
    # Download the file using gdown
    gdown.download(file_url, output_path, quiet=False)

def extract_data():
    data_source_path = "/home/amina/airflow/dags/data.csv"
    with open(data_source_path, 'r') as file:
        data = file.read()
    return data

def transform_data(data):
    # data = data.split('\n')
    # data = [row.split(',')[-1] for row in data]
    # data = ','.join(data)
    # transformed_data = data.upper()
    return transformed_data

def load_data_to_github(transformed_data):
    temp_file_path = "/home/amina/airflow/dags/temp_file.txt"
    with open(temp_file_path, 'w') as file:
        file.write(transformed_data)
    
    os.system(f"dvc add {temp_file_path}")
    os.system("dvc push")
    os.remove(temp_file_path)  

# Define the tasks in the DAG

# Task to download data
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

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

# Update the task dependencies
download_task >> extract_task >> transform_task >> load_to_github_task
