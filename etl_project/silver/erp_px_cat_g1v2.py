
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("erp_px_cat_g1v2")

def run(spark):
    task = 'silver.erp_px_cat_g1v2'
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
                        .load("/Volumes/workspace/default/source_erp/source_erp/PX_CAT_G1V2.csv")


        logging.info('Cleaning data...')
        df_clean  = df.select(
            col('ID'),
            col('CAT'),
            col('SUBCAT'),
            col('MAINTENANCE')
        )

        rows_processed = df_clean.count()

        logging.info('Writing data to Silver layer...')
        df_clean.write.format('parquet').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_erp/erp_px_cat_g1v2.parquet")\
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
