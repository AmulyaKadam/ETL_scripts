# Databricks notebook source
import sys
import os
import logging
from datetime import datetime


if "__file__" in globals():
    project_root = os.path.dirname(os.path.abspath(__file__))
else:
    project_root = os.getcwd()  # Databricks Repo / Job environment

if project_root not in sys.path:
    sys.path.append(project_root)

# imports

from framework.audit import write_audit_log

# silver
from silver.crm_prd_info import run as run_crm_prd_info
from silver.crm_customers import run as run_crm_customers
from silver.crm_sales import run as run_crm_sales
from silver.erp_items import run as run_erp_items
from silver.erp_customers import run as run_erp_customers
from silver.erp_sales import run as run_erp_sales

# gold
from gold.sales_summary import run as run_sales_summary
from gold.customer_profile import run as run_customer_profile
from gold.product_performance import run as run_product_performance

# ---- logging config ----
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("main_runner")

def run_all_etl(spark):
    logger.info("Starting ETL pipeline")


    steps = [
        run_crm_prd_info,
        run_crm_customers,
        run_crm_sales,
        run_erp_items,
        run_erp_customers,
        run_erp_sales,
        run_sales_summary,
        run_customer_profile,
        run_product_performance,
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
