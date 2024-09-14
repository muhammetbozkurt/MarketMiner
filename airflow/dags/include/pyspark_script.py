import os
import json

from loguru import logger
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col
from pyspark.sql.types import FloatType


ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")
OBJECT_STORAGE_URL = os.getenv("OBJECT_STORAGE_URL", "http://minio.default.svc.cluster.local:9000")

# Create a SparkSession
spark = SparkSession.builder.appName("StockMarketAnalysis").getOrCreate()


hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", OBJECT_STORAGE_URL)
hadoopConf.set("fs.s3a.access.key", "admin")
hadoopConf.set("fs.s3a.secret.key", "minio123")

config_file = "/configs/config.json"

with open(config_file, "r") as f:
    config = json.load(f)

today = datetime.now().date()

for market, symbols in config.items():
    for symbol in symbols:
        ticker = f"{symbol}.{market}" if market != "NASDAQ" else symbol
        try:
            data_collector = list()
            # Fetch historical data for the past 7 days
            for date in [today - timedelta(days=i) for i in range(7)]:
                daily_data = spark.read.parquet(
                    f"s3a://market-miner/market/{market}/{symbol}/{date.year}/{date.month}/{date.day}/data.parquet"
                )
                data_collector.append(daily_data)
            
            # Combine the data for the past 7 days
            df = spark.union(*data_collector)

            # data already contains data for on a specific symbol for 7 days
            # Convert the date column to a date type
            df = df.withColumn("date", df.date.cast("date"))

            # Calculate the weekly average closing price
            windowSpec = Window.orderBy("date").rowsBetween(-6, 0)
            df = df.withColumn("weekly_avg_close", F.avg("close").over(windowSpec))

            # Calculate the weekly percentage change in closing price
            df = df.withColumn(
                "weekly_pct_change",
                (df.close - lag("close").over(windowSpec)) / lag("close").over(windowSpec) * 100,
            )

            # Calculate other indicators as needed
            df = df.withColumn("week_of_year", weekofyear("date"))
            df = df.withColumn("daily_pct_change", (df.close - df.open) / df.open * 100)


            # Save the results to PostgreSQL
            url = "jdbc:postgresql://postgresql:5432/postgres"
            properties = {
                "user": "admin",
                "password": "bDj3QNn3Ha",
                "driver": "org.postgresql.Driver",
            }

            df.write.jdbc(
                url=url, table="stock_indicators", mode="overwrite", properties=properties
            )



        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")

spark.stop()