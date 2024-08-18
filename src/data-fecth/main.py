import io
import json
import boto3
import logging
import argparse

import yfinance as yf
import pandas as pd

from datetime import datetime, timedelta
from os import getenv

ACCESS_KEY = getenv("ACCESS_KEY")
ACCESS_SECRET = getenv("ACCESS_SECRET")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_and_save_data(config_file, minio_client):
    
    with open(config_file, 'r') as f:
        config = json.load(f)

    for market, symbols in config.items():
        for symbol in symbols:
            ticker = f"{symbol}.{market}" if market != "NASDAQ" else symbol
            try:
                # Fetch historical data for the past day
                data = yf.download(ticker, period="1d")

                # Convert to parquet
                buffer = io.BytesIO()
                pd.DataFrame(data).reset_index().to_parquet(
                    buffer, engine='pyarrow', compression='gzip')
                buffer.seek(0)

                # Define object path in MinIO
                today = datetime.now()
                object_path = f"market/{market}/{symbol}/{today.year}/{today.month}/{today.day}/data.parquet"

                # Upload to MinIO
                response = minio_client.put_object(Bucket="market-miner", Key=object_path, Body=buffer)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path",
                        help="path of configuration",
                        default="/configs/config.json")
    parser.add_argument("--object_storage_url",
                        help="url of object storage servie",
                        default="minio.default.svc.cluster.local:9000")

    arguments = parser.parse_args()

    if ACCESS_KEY is None or ACCESS_SECRET is None:
        logger.error("ACCESS_KEY and ACCESS_SECRET must be set")
        raise Exception("Object storage connection problem 1")


    s3 = boto3.client('s3',
                  endpoint_url=arguments.object_storage_url,
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=ACCESS_SECRET)
    
    fetch_and_save_data(arguments.config_path, s3)

if __name__ == "__main__":
    config_file = "sample.json"  # Replace with your config file path

    
