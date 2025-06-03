# Databricks notebook source
import requests
import os

def download_file_from_gdrive(file_id, destination):
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    URL = "https://drive.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params={'id': file_id}, stream=True)

    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    token = get_confirm_token(response)
    if token:
        params = {'id': file_id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    with open(destination, "wb") as f:
        for chunk in response.iter_content(32768):
            if chunk:
                f.write(chunk)


file_id = "1AGXVlDhbMbhoGXDJG0IThnqz86Qy3hqb"
destination_path = "/dbfs/tmp/transactions.csv"

download_file_from_gdrive(file_id, destination_path)


# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/tmp/transactions.csv")
df.display()

# COMMAND ----------

import time
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------
INPUT_FILE = "dbfs:/tmp/transactions.csv"
CHUNK_SIZE = 10000
OUTPUT_PATH_BASE = "s3a://detection-bucket-devdemo/transactions/"  # replace your S3 path

# Optional: Set AWS credentials if not using IAM role
spark.conf.set("fs.s3a.access.key", "AKIASAHESZNHXN366E7Z")
spark.conf.set("fs.s3a.secret.key", "Vaw5MpfPiIvwMEePEyRJh6Q176c5x70px9P6deK/")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# ---------------------------------------------
# LOAD TRANSACTIONS FILE
# ---------------------------------------------
df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_FILE)

# Add an index column (use zipWithIndex on RDD for deterministic chunking)
indexed_rdd = df.rdd.zipWithIndex().map(lambda row_idx: (row_idx[1],) + tuple(row_idx[0]))
columns = ["row_id"] + df.columns
indexed_df = indexed_rdd.toDF(columns)

# ---------------------------------------------
# CHUNKING AND WRITING TO S3
# ---------------------------------------------
total_rows = indexed_df.count()
total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

print(f"Total records: {total_rows}")
print(f"Writing {total_chunks} chunks of {CHUNK_SIZE} records to S3...")

for i in range(total_chunks):
    start = i * CHUNK_SIZE
    end = min(start + CHUNK_SIZE, total_rows)

    chunk_df = indexed_df.filter((col("row_id") >= start) & (col("row_id") < end)).drop("row_id")

    output_path = f"{OUTPUT_PATH_BASE}chunk_{i+1}.csv"

    # coalesce(1): single file per chunk
    chunk_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"[{i+1}/{total_chunks}] Written rows {start} to {end} → {output_path}")
    time.sleep(1)  # Simulate 1 second delay as per requirement


# COMMAND ----------

# MAGIC %md
# MAGIC **Y** - **Mechanism**
# MAGIC

# COMMAND ----------

import time
import boto3
import pandas as pd
from uuid import uuid4
from botocore.exceptions import NoCredentialsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configs
TRANSACTION_CHUNKS_PATH = "s3a://detection-bucket-devdemo/transactions/"
DETECTIONS_OUTPUT_PATH = "s3a://detection-bucket-devdemo/patterndetections/"
CUSTOMER_IMPORTANCE_PATH = "dbfs:/FileStore/CustomerImportance.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("MechanismY-Detection").getOrCreate()
spark.conf.set("fs.s3a.access.key", "AKIASAHESZNHXN366E7Z")
spark.conf.set("fs.s3a.secret.key", "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")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# Load Customer Importance once
customer_importance_df = (
    spark.read.option("header", True).csv(CUSTOMER_IMPORTANCE_PATH)
    .withColumnRenamed("Source", "customerName")
    .withColumnRenamed("Weight", "weight")
)

# Define schema for transactions
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("zipcodeOri", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("zipMerchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("fraud", IntegerType(), True)
])

# S3 & State Tracking
session = boto3.Session(
    aws_access_key_id="AKIASAHESZNHXN366E7Z",
    aws_secret_access_key="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"
)
s3 = session.client("s3")
bucket = "detection-bucket-devdemo"
prefix = "transactions/"

processed_files = set()

# === Pattern Detection Functions ===

def detect_pat1(df, merchant):
    merchant_df = df.filter(col("merchant") == merchant)
    total_txn_count = merchant_df.count()
    if total_txn_count < 50000:
        return []

    joined = merchant_df.join(
        customer_importance_df.filter(col("typeTrans") == "transaction"),
        on="customerName", how="left"
    ).withColumn("weight", col("weight").cast("double"))

    txn_counts = joined.groupBy("customerName").agg(count("*").alias("txn_count"))
    percentiles = txn_counts.approxQuantile("txn_count", [0.99], 0.01)
    top_1_percent_cutoff = percentiles[0]

    bottom_weights = joined.approxQuantile("weight", [0.01], 0.01)[0]

    candidates = (
        joined.groupBy("customerName")
        .agg(count("*").alias("txn_count"), avg("weight").alias("avg_weight"))
        .filter((col("txn_count") >= top_1_percent_cutoff) & (col("avg_weight") <= bottom_weights))
        .select("customerName")
        .distinct()
    )

    return [{
        "YStartTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "detectionTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "patternId": "PatId1",
        "ActionType": "UPGRADE",
        "customerName": row.customerName,
        "MerchantId": merchant
    } for row in candidates.collect()]

def detect_pat2(df):
    avg_txn = (
        df.groupBy("customerName", "merchant")
        .agg(avg("transactionValue").alias("avg_val"), count("*").alias("txn_count"))
        .filter((col("avg_val") < 23) & (col("txn_count") >= 80))
        .select("customerName", "merchant")
    )
    return [{
        "YStartTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "detectionTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "patternId": "PatId2",
        "ActionType": "CHILD",
        "customerName": row.customerName,
        "MerchantId": row.merchant
    } for row in avg_txn.collect()]

def detect_pat3(df):
    gender_count = (
        df.groupBy("merchant", "gender")
        .agg(count("*").alias("cnt"))
        .groupBy("merchant")
        .pivot("gender", ["M", "F"])
        .sum("cnt")
        .fillna(0)
    ).filter((col("F") > 100) & (col("F") < col("M")))

    return [{
        "YStartTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "detectionTime": time.strftime('%Y-%m-%d %H:%M:%S'),
        "patternId": "PatId3",
        "ActionType": "DEI-NEEDED",
        "customerName": "",
        "MerchantId": row.merchant
    } for row in gender_count.collect()]

def write_detections_to_s3(detections, chunk_id):
    if not detections:
        return
    for i in range(0, len(detections), 50):
        part = detections[i:i + 50]
        df = spark.createDataFrame(pd.DataFrame(part))
        unique_filename = f"detections_{chunk_id}_{uuid4().hex[:8]}.csv"
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DETECTIONS_OUTPUT_PATH}{unique_filename}")
        print(f"Wrote {len(part)} detections to {unique_filename}")

# === Continuous Polling Loop ===

while True:
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            print("No files found yet. Waiting...")
            time.sleep(5)
            continue

        new_file_keys = []
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv") and key not in processed_files:
                new_file_keys.append(key)

        if not new_file_keys:
            print("No new files to process. Waiting...")
            time.sleep(5)
            continue

        for file_key in new_file_keys:
            print(f"Processing file: {file_key}")
            chunk_s3_path = f"s3a://{bucket}/{file_key}"

            chunk_df = (
                spark.read
                    .option("header", True)
                    .option("quote", "'")
                    .option("mergeSchema", "true")
                    .option("enforceSchema", "true")
                    .schema(schema)
                    .csv(chunk_s3_path)
                    .withColumnRenamed("customer", "customerName")
                    .withColumnRenamed("amount", "transactionValue")
            )

            chunk_df_clean = chunk_df.withColumn(
                "customerName", regexp_replace(col("customerName"), "''", "")
            ).withColumn(
                "merchant", regexp_replace(col("merchant"), "''", "")
            )

            detections = []
            merchant_ids = [row.merchant for row in chunk_df_clean.select("merchant").distinct().collect()]
            for merchant in merchant_ids:
                detections += detect_pat1(chunk_df_clean, merchant)
            detections += detect_pat2(chunk_df_clean)
            detections += detect_pat3(chunk_df_clean)

            chunk_id = file_key.split("/")[-1].replace(".csv", "")
            write_detections_to_s3(detections, chunk_id)

            processed_files.add(file_key)

        time.sleep(1)

    except NoCredentialsError:
        print("AWS credentials not found.")
        break
    except Exception as e:
        print(f"Error: {str(e)}")
        time.sleep(5)
