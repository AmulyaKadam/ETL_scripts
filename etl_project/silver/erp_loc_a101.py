
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("erp_loc_a101")

def run(spark):
    task = 'silver.erp_loc_a101'
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
                        .load("/Volumes/workspace/default/source_erp/source_erp/LOC_A101.csv")


        logging.info('Cleaning data...')
        df_clean = df.select(
            trim(regexp_replace(col("CID"),'-','')).alias("CID"),

            when(col("CNTRY").isNull(), "n/a")
            .when(trim(regexp_replace(col("CNTRY"), "\u00A0", "")) == "", "n/a") 
            .when(upper(trim(regexp_replace(col("CNTRY"), "\u00A0", ""))).isin("US", "USA"), "United States")
            .when(upper(trim(regexp_replace(col("CNTRY"), "\u00A0", ""))).isin("DE"), "Germany")
            .otherwise(trim(regexp_replace(col("CNTRY"), "\u00A0", "")))
            .alias("CNTRY")
        )

        rows_processed = df_clean.count()

        logging.info('Writing data to Silver layer...')
        df_clean.write.format('delta').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/silver/source_erp/erp_loc_a101")\
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
