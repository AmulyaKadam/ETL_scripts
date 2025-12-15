
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fact_sales")

def run(spark):
    task = 'gold.fact_sales'
    layer = 'gold'
    rows_processed = 0
    start = datetime.now()
    error_message = None
    status = 'SUCCESS'

    try:
        logging.info(f"Starting Task {task}...")


        logging.info(f"Reading Silver data...")
        crm_sales = spark.read.format("delta").load("abfss://silver@awstorageamulya.dfs.core.windows.net/source_crm/crm_sales_details")
        
        logging.info(f"Reading gold dimentional data...")
        dim_prd = spark.read.format("delta").load("abfss://gold@awstorageamulya.dfs.core.windows.net/dim_products")
        dim_cst = spark.read.format("delta").load("abfss://gold@awstorageamulya.dfs.core.windows.net/dim_customers")


        logging.info('Applying Trasnformations...')
        df_final = (
            crm_sales.alias('sd')
            .join(dim_prd.alias('pd'), trim(col('sd.sls_prd_key')) == trim(col('pd.product_number')), 'left')
            .join(dim_cst.alias('cd'), col('sd.sls_cust_id') == col('cd.Customer_id'), 'left')
            .select(
                col('sd.sls_ord_num').alias('order_number'),
                col('pd.Product_key').alias('product_key'),
                col('cd.Customer_key').alias('customer_key'),
                col('sd.sls_order_dt').alias('order_date'),
                col('sd.sls_ship_dt').alias('ship_date'),
                col('sd.sls_due_dt').alias('due_date'),
                col('sd.sls_sales').alias('sales_amount'),
                col('sd.sls_quantity').alias('quantity'),
                col('sd.fixed_price').alias('fixed_price')
            )
        )
        rows_processed = df_final.count()

        logging.info('Writing data to Gold layer...')
        df_final.write.format('delta').mode('overwrite')\
            .option('path',"abfss://gold@awstorageamulya.dfs.core.windows.net/fact_sales")\
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
