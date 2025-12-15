# Databricks notebook source
import sys
import os
import logging
from datetime import datetime


repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# imports

from framework.audit import write_audit_log
from config.storage_config import configure_adls

# silver
from silver.crm_prd_info import run as run_crm_prd_info
from silver.crm_cust_info import run as run_crm_cust_info
from silver.crm_sales_details import run as run_crm_sales_details
from silver.erp_cust_az12 import run as run_erp_cust_az12
from silver.erp_loc_a101 import run as run_erp_loc_a101
from silver.erp_px_cat_g1v2 import run as run_erp_px_cat_g1v2

# gold
from gold.dim_customers import run as run_dim_customers
from gold.dim_products import run as run_dim_products
from gold.fact_sales import run as run_fact_sales

# ---- logging config ----
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("main_runner")

def run_all_etl(spark):
    logger.info("Starting ETL pipeline")
    configure_adls(spark,dbutils)

    steps = [
        run_crm_prd_info,
        run_crm_cust_info,
        run_crm_sales_details,
        run_erp_cust_az12,
        run_erp_loc_a101,
        run_erp_px_cat_g1v2,
        run_dim_customers,
        run_dim_products,
        run_fact_sales,
    ]

  
    audit_records = []
    for func in steps:
        task_name = getattr(func, "__name__", str(func))
        logger.info(f"Running task: {task_name}")
        start = datetime.now()
        try:
            record = func(spark)  
            
            if not isinstance(record, dict):
                raise ValueError(f"ETL task {task_name} must return a dict audit record.")
            
            record.setdefault("task_name", task_name)
            record.setdefault("start_time", start)
            record.setdefault("end_time", datetime.now())
            duration = (record["end_time"] - record["start_time"]).total_seconds()
            record.setdefault("duration_seconds", duration)
        except Exception as e:
            logger.exception(f"Task {task_name} failed")
            record = {
                "task_name": task_name,
                "layer": "unknown",
                "status": "FAILED",
                "rows_processed": 0,
                "start_time": start,
                "end_time": datetime.now(),
                "duration_seconds": (datetime.now() - start).total_seconds(),
                "error_message": str(e)
            }
            
            audit_records.append(record)
            write_audit_log(spark, record)
            raise

        logger.info(f"Completed {task_name} | status={record.get('status')} rows={record.get('rows_processed')}")
        audit_records.append(record)

    
    for rec in audit_records:
        write_audit_log(spark, rec)

    logger.info("ETL pipeline finished successfully")


if __name__ == "__main__":

    run_all_etl(spark)
