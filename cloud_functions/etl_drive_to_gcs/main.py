import os
import pandas as pd
import io
import logging
import hashlib
import json
from pathlib import Path

# Librerías de Google Cloud
from google.cloud import storage, secretmanager
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
SECRET_NAME = os.environ.get("DRIVE_SECRET_NAME")

# --- Clientes de GCP (se inicializan una vez) ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()


def get_drive_credentials() -> service_account.Credentials:
    """Obtiene las credenciales de la cuenta de servicio desde Secret Manager."""
    try:
        secret_version_name = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_version_name})
        creds_json = json.loads(response.payload.data.decode("UTF-8"))
        scopes = ['https://www.googleapis.com/auth/drive']
        return service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
    except Exception as e:
        logging.error(f"Error al obtener las credenciales de Secret Manager: {e}")
        raise


def generate_transaction_id(row: pd.Series, entity: str) -> str:
    """Genera un ID único para una fila para evitar duplicados."""
    unique_string = f"{entity}-{row['fecha']}-{row['concepto']}-{row['importe']}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()


def move_drive_file(drive_service, file_id: str, source_folder: str, destination_folder: str):
    """Mueve un archivo entre carpetas en Google Drive."""
    try:
        drive_service.files().update(
            fileId=file_id, addParents=destination_folder, removeParents=source_folder, fields='id, parents'
        ).execute()
        logging.info(f"Archivo {file_id} movido a la carpeta {destination_folder}.")
    except HttpError as e:
        logging.error(f"No se pudo mover el archivo {file_id}: {e}")


def process_and_upload(drive_service, file_metadata: dict, config: dict):
    """Función genérica para descargar, procesar y subir un archivo de Drive a GCS."""
    file_id, file_name = file_metadata['id'], file_metadata['name']
    parser_cfg = config['parser_config']
    logging.info(f"Procesando '{file_name}' con config '{config['description']}'")

    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_bytes = io.BytesIO(request.execute())
        
        # --- Lógica dinámica para leer Excel o CSV ---
        if config.get('file_type') == 'csv':
            df = pd.read_csv(file_bytes, skiprows=parser_cfg.get('skiprows', 0))
        else: # Por defecto, asume Excel
            df = pd.read_excel(file_bytes, engine='openpyxl', skiprows=parser_cfg.get('skiprows', 0), header=None)
            df.columns = parser_cfg['columns']

        if parser_cfg.get('drop_last_row', False):
            df = df.iloc[:-1]
        
        # --- Lógica dinámica para renombrar columnas ---
        if 'columns_map' in parser_cfg:
            df.rename(columns=parser_cfg['columns_map'], inplace=True)
            final_columns = list(parser_cfg['columns_map'].values())
        else:
            final_columns = parser_cfg['final_columns']

        final_df = df[final_columns].copy()
        final_df['importe'] = pd.to_numeric(final_df['importe'], errors='coerce')
        
        # --- Lógica dinámica para formato de fecha ---
        if 'date_format' in parser_cfg:
            final_df['fecha'] = pd.to_datetime(final_df['fecha'], format=parser_cfg['date_format'], errors='coerce').dt.strftime('%Y-%m-%d')
        else:
            final_df['fecha'] = pd.to_datetime(final_df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d')

        final_df.dropna(subset=final_columns, inplace=True)

        if final_df.empty:
            logging.warning(f"El archivo {file_name} no produjo datos. Se moverá.")
        else:
            entity = config.get('entity', 'unknown')
            final_df['origen'] = config['source_type']
            final_df['entidad'] = entity
            final_df['transaccion_id'] = final_df.apply(lambda row: generate_transaction_id(row, entity), axis=1)
            final_df = final_df[['transaccion_id', 'fecha', 'concepto', 'importe', 'origen', 'entidad']]
            
            json_data = final_df.to_json(orient='records', lines=True, date_format='iso')
            blob = storage_client.bucket(BUCKET_NAME).blob(f"{config['gcs_path']}/{Path(file_name).stem}_{file_id}.jsonl")
            blob.upload_from_string(json_data, content_type='application/jsonl+json')
            logging.info(f"Archivo {blob.name} subido a {BUCKET_NAME}.")

    except Exception as e:
        logging.error(f"Fallo crítico al procesar {file_name}: {e}")
    finally:
        move_drive_file(drive_service, file_id, config['drive_folder_pending'], config['drive_folder_processed'])


def process_files_for_source(drive_service, config: dict):
    """Lista y procesa todos los archivos para una configuración de fuente dada."""
    folder_id = config['drive_folder_pending']
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        files = drive_service.files().list(q=query, fields='files(id, name)').execute().get('files', [])
        if not files:
            logging.info(f"No se encontraron archivos para '{config['description']}'.")
            return
        logging.info(f"Se encontraron {len(files)} archivos para '{config['description']}'.")
        for f in files:
            process_and_upload(drive_service, f, config)
    except HttpError as e:
        logging.error(f"No se pudo listar archivos en la carpeta {folder_id}: {e}")


def main(event: dict, context: object):
    """Punto de entrada principal de la Cloud Function."""
    logging.info("Iniciando ejecución del pipeline Drive-to-GCS.")
    try:
        with open('config.json', 'r') as f:
            configs = json.load(f)
        credentials = get_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)
        for source_name, config in configs.items():
            process_files_for_source(drive_service, config)
        logging.info("Ejecución del pipeline completada.")
        return "Proceso completado.", 200
    except Exception as e:
        logging.critical(f"Error fatal en la ejecución principal: {e}")
        return "Error en la ejecución.", 500
