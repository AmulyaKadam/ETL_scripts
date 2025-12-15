
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dim_customers")

def run(spark):
    task = 'gold.dim_customers'
    layer = 'gold'
    rows_processed = 0
    start = datetime.now()
    error_message = None
    status = 'SUCCESS'

    try:
        logging.info(f"Starting Task {task}...")


        logging.info(f"Reading Silver data...")
        crm_cust = spark.read.format("delta").load("abfss://silver@awstorageamulya.dfs.core.windows.net/source_crm/crm_cust_info")
        erp_cust = spark.read.format("delta").load("abfss://silver@awstorageamulya.dfs.core.windows.net/source_erp/erp_cust_az12")
        erp_loc  = spark.read.format("delta").load("abfss://silver@awstorageamulya.dfs.core.windows.net/source_erp/erp_loc_a101")


        logging.info('Applying Trasnformations...')
        w = Window.orderBy('cst_id')

        df_final = (
            crm_cust.alias('ci')
            .join(erp_cust.alias('cb'), col('ci.cst_key') == col('cb.CID'),'left')
            .join(erp_loc.alias('cl'), col('ci.cst_key') == col('cl.CID'),'left')
            .select(
                row_number().over(w).alias('Customer_key'),
                col('ci.cst_id').alias('Customer_id'),
                col('ci.cst_key').alias('Customer_number'),
                col('ci.cst_firstname').alias('first_name'),
                col('ci.cst_lastname').alias('last_name'),
                col('cl.CNTRY').alias('Country'),
                col('ci.cst_marital_status').alias('marital_status'),
                when(col('ci.cst_gndr') != 'N/a',col('ci.cst_gndr')).otherwise(coalesce(col('cb.gen'),lit("N/a"))).alias('Gender'),
                col('cb.BDATE').alias('Birthdate'),
                col('ci.cst_create_date').alias('create_date')

            )

        )
        rows_processed = df_final.count()

        logging.info('Writing data to Gold layer...')
        df_final.write.format('delta').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/gold/dim_customers")\
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
