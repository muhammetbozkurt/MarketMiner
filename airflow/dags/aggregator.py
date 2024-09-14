# this will be the main DAG file that will be used to run the entire pipeline
# main it will call a spark job to aggregate the data and then save the results to a PostgreSQL database

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s
import json
import os

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")
OBJECT_STORAGE_URL = os.getenv("OBJECT_STORAGE_URL", "http://minio.default.svc.cluster.local:9000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'pyspark_submit_dag',
    default_args=default_args,
    description='A DAG to submit a PySpark job',
    schedule_interval=timedelta(days=7),
    catchup=False,
) as dag:
    
    pyspark_task = SparkSubmitOperator(
        task_id='pyspark_submit_task',
        application="/spark/src/pyspark_script.py",
        conn_id='spark_default',
        conf={
            'spark.kubernetes.container.image': 'custom-spark:1.0.0',
            'spark.kubernetes.namespace': 'default',
            'spark.executor.instances': '2',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '1'
        },
    )