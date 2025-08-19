import os
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get("GCP_PROJECT")
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW")

def main(event: dict, context: object):
    """
    Creates or replaces an external table in BigQuery pointing to a GCS file.
    """
    bucket_name = event['bucket']
    file_name = event['name']
    logging.info(f"File detected: gs://{bucket_name}/{file_name}")

    client = bigquery.Client()
    
    # Define la tabla externa (una por cada fuente/tipo)
    # Ejemplo: bankinter/account/fichero.jsonl -> tabla bankinter_account
    source_path = os.path.dirname(file_name) # ej: bankinter/account
    table_name = source_path.replace('/', '_') # ej: bankinter_account
    table_id = f"{PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"
    uri = f"gs://{bucket_name}/{source_path}/*" # Apunta a todos los ficheros de la carpeta

    logging.info(f"Creating or replacing external table {table_id} for URI {uri}")

    try:
        # Configuración de la tabla externa
        external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
        external_config.source_uris = [uri]
        # Permite que BigQuery infiera el schema automáticamente
        external_config.autodetect = True

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        # Borra la tabla si ya existe para asegurar que siempre esté actualizada
        try:
            client.delete_table(table, not_found_ok=True)
            logging.info(f"Deleted existing table {table_id} to recreate it.")
        except NotFound:
            pass # No hace falta hacer nada si no existe

        # Crea la nueva tabla externa
        client.create_table(table)
        logging.info(f"Successfully created external table {table_id}")

    except Exception as e:
        logging.error(f"Failed to create external table {table_id}. Error: {e}")
        raise

    return "External table creation process finished.", 200
