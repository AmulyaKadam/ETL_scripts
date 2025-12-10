# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("crm_sales_details")

def run(spark):
    task = 'silver.crm_sales_details'
    layer = 'silver'
    rows_processed = 0
    start = datetime.now()
    error_message = None
    status = 'SUCCESS'

    try:
        logging.info(f"Starting Task {task_name}...")


        logging.info(f"Reading Bronze data...")
        df = spark.read.format('csv')\
                .option("header", "true")\
                    .option("inferSchema", "true")\
                        .load("/Volumes/workspace/default/source_crm/source_crm/sales_details.csv")

        logging.info('Cleaning data...')
        df.fixed = df.withColumn('fixed_price', when(col('sls_price').isNull() | (col('sls_price') <=0) \
                        , col('sls_sales') / when(col('sls_quantity')==0,None)\
                            .otherwise(col('sls_quantity')))\
                                .otherwise(col('sls_price')))
        
        df_final = df.fixed.select(
            col('sls_ord_num'),
            col('sls_prd_key'),
            col('sls_cust_id'),

            valid_date('sls_order_dt').alias('sls_order_dt'),
            valid_date('sls_ship_dt').alias('sls_ship_dt'),
            valid_date('sls_due_dt').alias('sls_due_dt'),

            # Recalculate sales if missing or incorrect
            when(
                col('sls_sales').isNull() |
                (col('sls_sales') <= 0) |
                (col('sls_sales') != col('sls_quantity') * abs(col('fixed_price'))),
                col('sls_quantity') * abs(col('fixed_price'))
            ).otherwise(col('sls_sales')).alias('sls_sales'),

            col('sls_quantity'),
            col('fixed_price')
        )

        rows_processed = df_final.count()

        logging.info('Writing data to Silver layer...')
        df_final.write.format('parquet').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_crm/crm_sales_details.parquet")\
                .save()

    except Exception as e:
        logger.exception(f'Task {task} failed!')
        status = 'FAILED'
        rows_processed = 0
        error_message = str(e)    
    end = datetime.now()
    logging.info(f"Task {task} completed in {end-start} seconds")
    
    return : 
        {
            'task_name': task,
            'layer': layer,
            'status': status,
            'rows_processed': rows_processed,
            'start_time': start,
            'end_time': end,
            'duration_seconds': (end-start).total_seconds(),
            'error_message': error_message
        }