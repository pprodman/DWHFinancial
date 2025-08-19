# main.py
import os
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get("GCP_PROJECT")
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW")
BQ_DATASET_BRONZE = os.environ.get("BQ_DATASET_BRONZE")
BQ_DATASET_SILVER = os.environ.get("BQ_DATASET_SILVER")
BQ_DATASET_GOLD = os.environ.get("BQ_DATASET_GOLD")
BQ_TABLE_TRANSACTIONS = os.environ.get("BQ_TABLE_TRANSACTIONS")
BQ_TABLE_SUMMARY = os.environ.get("BQ_TABLE_SUMMARY")

client = bigquery.Client()

def execute_transformation(query_file_path, job_config):
    logging.info(f"Executing transformation from: {query_file_path}")
    with open(query_file_path, 'r') as f:
        sql = f.read()
    query_job = client.query(sql, job_config=job_config)
    query_job.result()
    logging.info(f"Successfully executed: {query_file_path}")

def main(event, context):
    logging.info("Starting scheduled BigQuery transformation job.")
    try:
        # Paso 0: Raw External to Bronze
        job_config_r2b = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
                bigquery.ScalarQueryParameter("raw_dataset", "STRING", BQ_DATASET_RAW),
                bigquery.ScalarQueryParameter("bronze_dataset", "STRING", BQ_DATASET_BRONZE),
                bigquery.ScalarQueryParameter("bronze_table", "STRING", BQ_TABLE_TRANSACTIONS),
            ]
        )
        execute_transformation('queries/00_raw_external_to_bronze.sql', job_config_r2b)

        # Paso 1: Bronze to Silver
        job_config_b2s = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
                bigquery.ScalarQueryParameter("bronze_dataset", "STRING", BQ_DATASET_BRONZE),
                bigquery.ScalarQueryParameter("silver_dataset", "STRING", BQ_DATASET_SILVER),
                bigquery.ScalarQueryParameter("bronze_table", "STRING", BQ_TABLE_TRANSACTIONS),
                bigquery.ScalarQueryParameter("silver_table", "STRING", BQ_TABLE_TRANSACTIONS),
            ]
        )
        execute_transformation('queries/01_bronze_to_silver.sql', job_config_b2s)

        # Paso 2: Silver to Gold
        job_config_s2g = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
                bigquery.ScalarQueryParameter("silver_dataset", "STRING", BQ_DATASET_SILVER),
                bigquery.ScalarQueryParameter("gold_dataset", "STRING", BQ_DATASET_GOLD),
                bigquery.ScalarQueryParameter("silver_table", "STRING", BQ_TABLE_TRANSACTIONS),
                bigquery.ScalarQueryParameter("gold_table", "STRING", BQ_TABLE_SUMMARY),
            ]
        )
        execute_transformation('queries/02_silver_to_gold.sql', job_config_s2g)
        
        logging.info("All transformations completed successfully.")
    except Exception as e:
        logging.critical(f"Transformation pipeline failed: {e}")