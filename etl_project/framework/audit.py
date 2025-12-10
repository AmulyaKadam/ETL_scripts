# Databricks notebook source
from pyspark.sql.types import *
from datetime import datetime

def write_audit_log(spark, record):


    task_name = record.get("task_name", "unknown")
    layer = record.get("layer", "unknown")
    status = record.get("status", "UNKNOWN")
    rows = int(record.get("rows_processed", 0)) if record.get("rows_processed") is not None else 0
    start = record.get("start_time", datetime.now())
    end = record.get("end_time", datetime.now())
    duration = float(record.get("duration_seconds", (end - start).total_seconds()))
    error = record.get("error_message")

    schema = StructType([
        StructField("task_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rows_processed", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("error_message", StringType(), True),
        StructField("executed_at", TimestampType(), True)
    ])

    row = [(task_name, layer, status, rows, start, end, duration, error, datetime.now())]

    df = spark.createDataFrame(row, schema=schema)


    df.write.format("parquet") \
            .mode("append") \
            .option("header", "true")\
            .option('path', "/Volumes/workspace/default/mydata/audit_logs/etl_logs") \
            .save()
