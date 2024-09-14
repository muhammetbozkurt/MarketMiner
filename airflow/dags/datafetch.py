from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from loguru import logger
from kubernetes.client import models as k8s
import json




def fetch_and_save_data():
    import yfinance as yf
    import pandas as pd
    import os
    import boto3
    import io

    ACCESS_KEY = os.getenv("ACCESS_KEY")
    ACCESS_SECRET = os.getenv("ACCESS_SECRET")
    OBJECT_STORAGE_URL = os.getenv("OBJECT_STORAGE_URL", "http://minio.default.svc.cluster.local:9000")
    config_file = "/configs/config.json"

    if not ACCESS_KEY or not ACCESS_SECRET:
        raise Exception("ACCESS_KEY or ACCESS_SECRET not found")

    with open(config_file, "r") as f:
        config = json.load(f)

    minio_client = boto3.client(
        "s3",
        endpoint_url=OBJECT_STORAGE_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=ACCESS_SECRET,
    )

    for market, symbols in config.items():
        for symbol in symbols:
            ticker = f"{symbol}.{market}" if market != "NASDAQ" else symbol
            try:
                # Fetch historical data for the past day
                data = yf.download(ticker, period="1d")

                # Convert to parquet
                buffer = io.BytesIO()
                pd.DataFrame(data).reset_index().to_parquet(
                    buffer, engine="pyarrow", compression="gzip"
                )
                buffer.seek(0)

                # Define object path in MinIO
                today = datetime.now()
                object_path = f"market/{market}/{symbol}/{today.year}/{today.month}/{today.day}/data.parquet"

                # Upload to MinIO
                response = minio_client.put_object(
                    Bucket="market-miner", Key=object_path, Body=buffer
                )
                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    logger.info("Object uploaded successfully!")
                else:
                    logger.error(
                        f"Unexpected HTTP status code: {response['ResponseMetadata']['HTTPStatusCode']}"
                    )
                    raise Exception("Object upload failed")

                logger.info(f"Data for {ticker} saved to MinIO: {object_path}")
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")


dag = DAG(
    "datafetch",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "catchup": False,

    },
    description="Fetch data from yfinance and save to MinIO",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 8),
    catchup=False,
)

# Task to fetch data
fetch_data_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_and_save_data,
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                volumes=[
                    k8s.V1Volume(
                        name="config-volume",
                        config_map=k8s.V1ConfigMapVolumeSource(name="symbol-config"),
                    )
                ],
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="custom-image:1.0.0",
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="config-volume", mount_path="/configs/config.json", sub_path="config.json"
                            )
                        ],
                        env=[
                            k8s.V1EnvVar(
                                name="ACCESS_KEY",
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(
                                        name="minio-custom-credentials",
                                        key="admin-key",
                                    )
                                ),
                            ),
                            k8s.V1EnvVar(
                                name="ACCESS_SECRET",
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(
                                        name="minio-custom-credentials",
                                        key="password-key",
                                    )
                                ),
                            ),
                        ],
                    )
                ],
            )
        )
    },
)


fetch_data_task
