import os
import logging
from google.cloud import bigquery

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get("GCP_PROJECT")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_TABLE_RAW = os.environ.get("BIGQUERY_TABLE_RAW") 

def main(event: dict, context: object):
    """
    Cloud Function activada por un nuevo archivo en GCS. Carga el archivo en BigQuery.
    """
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        logging.info(f"CARGA A BQ: Archivo detectado: {file_name} en bucket: {bucket_name}.")
    except KeyError as e:
        logging.error(f"El evento no tiene el formato esperado: {event}. Error: {e}")
        return "Formato de evento incorrecto", 400

    if not (file_name.startswith('bankinter/cuenta/') or file_name.startswith('bankinter/tarjeta/')):
        logging.info(f"El archivo {file_name} no está en una carpeta de datos procesados. Se ignora.")
        return "Archivo ignorado", 200

    client = bigquery.Client()
    dataset_ref = client.dataset(BIGQUERY_DATASET)
    table_ref = dataset_ref.table(BIGQUERY_TABLE_RAW)
    uri = f"gs://{bucket_name}/{file_name}"

    try:
        # ✅ CAMBIO CLAVE: Construimos una ruta segura al archivo que ahora está en el mismo directorio.
        script_dir = os.path.dirname(__file__)  # Directorio del script actual
        schema_path = os.path.join(script_dir, "BANKINTER.json")
        
        logging.info(f"Cargando esquema desde: {schema_path}")
        schema = client.schema_from_json(schema_path)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        logging.info(f"Iniciando trabajo de carga desde {uri} a la tabla {BIGQUERY_TABLE_RAW}.")
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()

        destination_table = client.get_table(table_ref)
        logging.info(f"¡Éxito! Se cargaron {load_job.output_rows} filas. La tabla ahora tiene {destination_table.num_rows} filas en total.")

    except FileNotFoundError:
        logging.error(f"ERROR CRÍTICO: El archivo 'BANKINTER.json' no fue encontrado. Asegúrate de que esté en el mismo directorio que main.py (`cloud_functions/gcs_to_bq/`).")
        raise
    except Exception as e:
        logging.error(f"Fallo en el trabajo de carga a BigQuery. Error: {e}")
        raise

    return "Carga a BigQuery completada", 200