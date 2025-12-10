
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("erp_cust_az12")

def run(spark):
    task = 'silver.erp_cust_az12'
    layer = 'silver'
    rows_processed = 0
    start = datetime.now()
    error_message = None
    status = 'SUCCESS'

    try:
        logging.info(f"Starting Task {task}...")


        logging.info(f"Reading Bronze data...")
        df = spark.read.format('csv')\
                .option("header", "true")\
                    .option("inferSchema", "true")\
                        .load("/Volumes/workspace/default/source_erp/source_erp/CUST_AZ12.csv")


        logging.info('Cleaning data...')
        df_clean =  df.select(
            when(
                col('CID').startswith('NAS'),substring(col('CID'),4,100)
                ).otherwise(col('CID')).alias('CID'),
            
            when(
                (col('BDATE') > current_date()),None
            ).otherwise(col('BDATE')).alias('BDATE'),

            when(col("gen").isNull(), "n/a")
            .when(trim(regexp_replace(col("gen"), "\u00A0", "")) == "", "n/a") 
            .when(upper(trim(regexp_replace(col("gen"), "\u00A0", ""))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(regexp_replace(col("gen"), "\u00A0", ""))).isin("M", "MALE"), "Male")
            .otherwise("n/a")
            .alias("gen")

        )

        rows_processed = df_clean.count()

        logging.info('Writing data to Silver layer...')
        df_clean.write.format('delta').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_erp/erp_cust_az12")\
                .save()

    except Exception as e:
        logger.exception(f'Task {task} failed!')
        status = 'FAILED'
        rows_processed = 0
        error_message = str(e)    
    end = datetime.now()
    logging.info(f"Task {task} completed in {end-start} seconds")
    
    return {
            'task_name': task,
            'layer': layer,
            'status': status,
            'rows_processed': rows_processed,
            'start_time': start,
            'end_time': end,
            'duration_seconds': (end-start).total_seconds(),
            'error_message': error_message
           }
