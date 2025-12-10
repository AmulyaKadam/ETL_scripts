
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dim_products")

def run(spark):
    task = 'gold.dim_products'
    layer = 'gold'
    rows_processed = 0
    start = datetime.now()
    error_message = None
    status = 'SUCCESS'

    try:
        logging.info(f"Starting Task {task}...")


        logging.info(f"Reading Silver data...")
        crm_prd = spark.read.format("parquet").load("/Volumes/workspace/default/mydata/silver/source_crm/crm_prd_info.parquet")
        erp_cat = spark.read.format("parquet").load("/Volumes/workspace/default/mydata/silver/source_erp/erp_px_cat_g1v2.parquet")


        logging.info('Applying Trasnformations...')
        w = Window.orderBy('prd_start_dt','prd_id')

        df_final = (
            crm_prd.alias('pm')
            .join(erp_cat.alias('pc'), col('pm.cat_id')==col('pc.ID'), 'left')
            .where(col('pm.prd_end_days').isNull())
            .select(
                row_number().over(w).alias('Product_key'),
                col("pm.prd_id").alias("product_id"),
                col("pm.prd_key").alias("product_number"),
                col("pm.prd_nm").alias("product_name"),

                col("pm.cat_id").alias("category_id"),
                col("pc.cat").alias("category"),
                col("pc.subcat").alias("sub_category"),
                col("pc.MAINTENANCE").alias("maintenance"),

                col("pm.prd_cost").alias("cost"),
                col("pm.prd_line").alias("product_line"),

                col("pm.prd_start_dt").alias("start_date")

            )    

        )
        rows_processed = df_final.count()

        logging.info('Writing data to Gold layer...')
        df_final.write.format('parquet').mode('overwrite')\
            .option('path',"/Volumes/workspace/default/mydata/gold/dim_products.parquet")\
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