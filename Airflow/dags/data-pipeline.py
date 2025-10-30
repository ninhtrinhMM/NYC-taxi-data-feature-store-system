
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import sys
import os

# Chuẩn bị default_args cho DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 10, 20),
    'retry_delay': timedelta(minutes=5),}

with DAG(
    'TEZT-FERNET-SECRETKEY',
    default_args=default_args,
    description='Process data with Spark Delta Lake and export to MinIO',
    schedule_interval='0 5 * * *',  # Chạy lúc 5h sáng mỗi ngày
    catchup=False,
    tags=['spark', 'delta', 'minio'], ) as dag:
    
    spark_task = BashOperator(
        task_id='spark_deltalake',
        bash_command='python /opt/airflow/scripts/Data-lake/spark-delta-table.py',)
    
    export_task = BashOperator(
        task_id='export_to_minio',
        bash_command='python /opt/airflow/scripts/Data-lake/export-to-minio.py',)
    
    batch_task = BashOperator(
        task_id='batch_processing',
        bash_command='python /opt/airflow/scripts/Batch-processing/batch-process.py',)
    
    # Lần lượt đầu tiên là chạy spark_task, sau đó đến export_task và cuối cùng là batch_task
    spark_task >> export_task >> batch_task