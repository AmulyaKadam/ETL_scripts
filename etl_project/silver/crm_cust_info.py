from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("crm_cust_info")

def run(spark):
    task = 'silver.crm_cust_info'
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
                    .load("/Volumes/workspace/default/source_crm/source_crm/cust_info.csv")  

        logging.info('Filtering data...')
        df_filtered = df.filter(col("cst_id") > 0)
        window_spec = Window.partitionBy(col('cst_id')).orderBy(col('cst_create_date').desc())
        df_filtered = df_filtered.withColumn('row_num',dense_rank().over(window_spec)).filter(col('row_num') == 1).drop('row_num')

        logging.info('Cleaning data...')
        clean_df = df_filtered.select(col('cst_id'),col('cst_key'),trim(col('cst_firstname'))\
                .alias('cst_firstname'),trim(col('cst_lastname'))\
                    .alias('cst_lastname'),when(upper(trim(col('cst_marital_status'))) == 'S','Single')\
                        .when(upper(trim(col('cst_marital_status'))) == 'M','Married')\
                            .otherwise('N/a')\
                                .alias('cst_marital_status'),when(upper(trim(col('cst_gndr'))) == 'M','Male')\
                                    .when(upper(trim(col('cst_gndr'))) == 'F','Female')\
                                        .otherwise('N/a')\
                                            .alias('cst_gndr'),col('cst_create_date'))
        rows_processed = clean_df.count()

        logging.info('Writing data to Silver layer...')
        clean_df.write.format('parquet').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_crm/crm_cust_info.parquet")\
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
