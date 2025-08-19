import os
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

# --- Environment Variables ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW")
BQ_DATASET_BRONZE = os.environ.get("BQ_DATASET_BRONZE")
BQ_DATASET_SILVER = os.environ.get("BQ_DATASET_SILVER")
BQ_DATASET_GOLD = os.environ.get("BQ_DATASET_GOLD")
BQ_TABLE_TRANSACTIONS = os.environ.get("BQ_TABLE_TRANSACTIONS")
BQ_TABLE_SUMMARY = os.environ.get("BQ_TABLE_SUMMARY")

client = bigquery.Client()

def execute_transformation(query_file_path, job_config):
    """Reads a SQL file and executes it as a parameterized query."""
    logging.info(f"Executing transformation from: {query_file_path}")
    try:
        with open(query_file_path, 'r') as f:
            sql = f.read()
        
        query_job = client.query(sql, job_config=job_config)
        query_job.result() # Wait for the job to complete
        logging.info(f"Successfully executed: {query_file_path}")
    except FileNotFoundError:
        logging.error(f"Query file not found: {query_file_path}")
        raise
    except Exception as e:
        logging.error(f"Failed to execute query from {query_file_path}: {e}")
        raise

def main(event, context):
    """Cloud Function entry point to run BigQuery transformations."""
    logging.info("Starting scheduled BigQuery transformation job.")

    try:
        # --- Step 1: Raw External to Bronze ---
        job_config_r2b = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
                bigquery.ScalarQueryParameter("raw_dataset", "STRING", BQ_DATASET_RAW),
                bigquery.ScalarQueryParameter("bronze_dataset", "STRING", BQ_DATASET_BRONZE),
                bigquery.ScalarQueryParameter("bronze_table", "STRING", BQ_TABLE_TRANSACTIONS),
            ]
        )
        # Asegúrate de que este fichero exista en la carpeta de la función
        execute_transformation('queries/00_raw_external_to_bronze.sql', job_config_r2b)

        # --- Step 2: Bronze to Silver ---
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

        # --- Step 3: Silver to Gold ---
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
        return "OK", 200

    except Exception as e:
        logging.critical(f"Transformation pipeline failed: {e}")
        return "Error", 500
