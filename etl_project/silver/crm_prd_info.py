
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("crm_prd_info")

def run(spark):
    task = 'silver.crm_prd_info'
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
                    .load("/Volumes/workspace/default/source_crm/source_crm/prd_info.csv")  

        logging.info('Cleaning data...')
        window_spec = Window.partitionBy('prd_key').orderBy(col('prd_start_dt'))

        clean_df = df.select(col('prd_id'), regexp_replace(substring(col('prd_key'), 1,5), '-','_')\
                        .alias('cat_id'), substring(col('prd_key'), 7,length(col('prd_key')))\
                            .alias('prd_key'),col('prd_nm'),col('prd_cost'), when(upper(trim(col('prd_line'))) == 'M','Mountain')\
                                .when(upper(trim(col('prd_line'))) == 'R','Road')\
                                    .when(upper(trim(col('prd_line'))) == 'S','Other Sales')\
                                        .when(upper(trim(col('prd_line'))) == 'T','Touring')\
                                            .otherwise('N/a')\
                                            .alias('prd_line'), col('prd_start_dt'),date_sub(lead('prd_start_dt')\
                                                .over(window_spec),1).alias('prd_end_days'))
        rows_processed = clean_df.count()

        logging.info('Writing data to Silver layer...')
        clean_df.write.format('parquet').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_crm/crm_prd_info.parquet")\
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