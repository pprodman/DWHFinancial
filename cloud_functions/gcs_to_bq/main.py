import os
import logging
from google.cloud import bigquery

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)

# Estas variables se configurarán en el despliegue
PROJECT_ID = os.environ.get("GCP_PROJECT")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
# Esta es la tabla donde se cargarán los datos "crudos" de ambas fuentes
BIGQUERY_TABLE_RAW = os.environ.get("BIGQUERY_TABLE_RAW") 


def main(event: dict, context: object):
    """
    Cloud Function activada por un nuevo archivo en GCS. Carga el archivo en BigQuery.
    """
    # 1. Extraer información del evento de GCS
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        logging.info(f"CARGA A BQ: Archivo detectado: {file_name} en bucket: {bucket_name}.")
    except KeyError as e:
        logging.error(f"El evento no tiene el formato esperado: {event}. Error: {e}")
        return "Formato de evento incorrecto", 400

    # Opcional: Ignorar archivos que no estén en las subcarpetas esperadas
    if not (file_name.startswith('CUENTA/') or file_name.startswith('TARJETA/')):
        logging.info(f"El archivo {file_name} no está en una carpeta de datos. Se ignora.")
        return "Archivo ignorado", 200

    # 2. Configurar el cliente y las referencias de BigQuery
    client = bigquery.Client()
    dataset_ref = client.dataset(BIGQUERY_DATASET)
    table_ref = dataset_ref.table(BIGQUERY_TABLE_RAW)
    
    # La URI completa del archivo en GCS que activó la función
    uri = f"gs://{bucket_name}/{file_name}"

    # 3. Configurar el trabajo de carga (Load Job)
    # Esta es la parte más importante del código
    try:
        # Carga el schema desde el archivo .json que está en el mismo directorio
        schema = client.schema_from_json("BANKINTER.json")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            # ¡CLAVE! Esto asegura que los datos nuevos se AÑADAN a la tabla, no que la reemplacen.
            # Así es como unificamos todos los meses en una sola tabla.
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # 4. Iniciar el trabajo de carga y esperar a que termine
        logging.info(f"Iniciando trabajo de carga desde {uri} a la tabla {BIGQUERY_TABLE_RAW}.")
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        
        load_job.result()  # Espera a que el trabajo se complete

        destination_table = client.get_table(table_ref)
        logging.info(f"¡Éxito! Se cargaron {load_job.output_rows} filas. La tabla ahora tiene {destination_table.num_rows} filas en total.")

    except FileNotFoundError:
        logging.error("ERROR CRÍTICO: El archivo 'bankinter_schema.json' no fue encontrado. Asegúrate de que esté en el mismo directorio que main.py.")
        raise
    except Exception as e:
        logging.error(f"Fallo en el trabajo de carga a BigQuery. Error: {e}")
        raise

    return "Carga a BigQuery completada", 200