import os
import logging
from google.cloud import bigquery

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)

# --- Variables de Entorno (se configuran en el despliegue) ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_TABLE_RAW = os.environ.get("BIGQUERY_TABLE_RAW") 

def main(event: dict, context: object):
    """
    Cloud Function activada por un nuevo archivo en GCS. 
    Carga el archivo en la tabla raw de BigQuery.
    """
    # 1. Extraer información del evento de GCS
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        logging.info(f"Archivo detectado: gs://{bucket_name}/{file_name}")
    except KeyError as e:
        logging.error(f"El evento no tiene el formato esperado: {event}. Error: {e}")
        return "Formato de evento incorrecto", 400

    # 2. Configurar el cliente y las referencias de BigQuery
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_RAW}"
    uri = f"gs://{bucket_name}/{file_name}"

    # 3. Configurar el trabajo de carga (Load Job)
    try:
        # Carga el schema desde el archivo .json que debe estar en el mismo directorio
        # que este main.py.
        schema = client.schema_from_json("schema.json")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            # ¡CLAVE! Añade los datos nuevos sin borrar los existentes.
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # 4. Iniciar el trabajo de carga y esperar a que termine
        logging.info(f"Iniciando trabajo de carga desde {uri} a la tabla {table_id}.")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        
        load_job.result()  # Espera a que el trabajo se complete

        destination_table = client.get_table(table_id)
        logging.info(
            f"¡Éxito! Se cargaron {load_job.output_rows} filas. "
            f"La tabla ahora tiene {destination_table.num_rows} filas en total."
        )

    except FileNotFoundError:
        logging.critical(
            "ERROR CRÍTICO: El archivo 'schema.json' no fue encontrado. "
            "Asegúrate de que esté desplegado junto a main.py."
        )
        raise
    except Exception as e:
        logging.error(f"Fallo en el trabajo de carga a BigQuery. Error: {e}")
        raise

    return "Carga a BigQuery completada", 200
