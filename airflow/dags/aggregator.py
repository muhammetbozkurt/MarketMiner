# this will be the main DAG file that will be used to run the entire pipeline
# main it will call a spark job to aggregate the data and then save the results to a PostgreSQL database

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s
import json
import os

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

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
    
    pyspark_task = KubernetesPodOperator(
        task_id='pyspark_submit_task',
        name='pyspark-submit-task',
        namespace='default',
        image='custom-spark:1.0.0',
        cmds=['spark-submit'],
        arguments=[
            '--master', 'k8s://https://kubernetes.default.svc.cluster.local:443',
            '--deploy-mode', 'cluster',
            '--executor-memory', '2g',
            '--executor-cores', '1',
            '--num-executors', '2',
            '--conf', 'spark.kubernetes.namespace=default',
            '--conf', 'spark.kubernetes.container.image=custom-spark:1.0.0',
            '/spark/src/pyspark_script.py'
        ],
        get_logs=True
    )