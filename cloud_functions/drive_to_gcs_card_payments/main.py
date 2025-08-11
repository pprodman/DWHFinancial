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

# --- Variables de Entorno ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
SECRET_NAME = os.environ.get("DRIVE_SECRET_NAME")
# ¡NUEVO! IDs de carpetas para tarjetas
FOLDER_ID_PENDING_CARD = os.environ.get("FOLDER_ID_PENDING_CARD") 
FOLDER_ID_PROCESSED_CARD = os.environ.get("FOLDER_ID_PROCESSED_CARD") 


def get_drive_credentials() -> service_account.Credentials:
    """Obtiene las credenciales de la cuenta de servicio desde Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_version_name = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": secret_version_name})
        creds_json = json.loads(response.payload.data.decode("UTF-8"))
        
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
        return credentials
    except Exception as e:
        logging.error(f"Error al obtener las credenciales de Secret Manager: {e}")
        raise

def generate_transaction_id(row: pd.Series) -> str:
    """Genera un ID único para una fila para evitar duplicados."""
    unique_string = f"{row['fecha']}-{row['concepto']}-{row['importe']}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def move_file_to_processed(drive_service, file_id: str, original_folder_id: str):
    """Mueve un archivo de la carpeta PENDING a la PROCESSED en Google Drive."""
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=FOLDER_ID_PROCESSED_CARD, # Usa la variable correcta de la tarjeta
            removeParents=original_folder_id,
            fields='id, parents'
        ).execute()
        logging.info(f"Archivo de tarjeta {file_id} movido a la carpeta PROCESSED.")
    except Exception as e:
        logging.error(f"No se pudo mover el archivo de tarjeta {file_id} a la carpeta PROCESSED: {e}")


def process_single_file(drive_service, storage_client, file_metadata: dict):
    """Descarga, procesa y sube un único archivo de tarjeta de Drive a GCS."""
    file_id = file_metadata['id']
    file_name = file_metadata['name']
    original_folder_id = file_metadata['parents'][0]

    logging.info(f"TARJETA: Procesando '{file_name}' (ID: {file_id})")

    # 1. Descargar el archivo
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_bytes = io.BytesIO(request.execute())
    except HttpError as e:
        logging.error(f"No se pudo descargar el archivo {file_name} (ID: {file_id}). Error: {e}")
        return 

    # 2. Lógica de procesamiento de Pandas específica para la TARJETA
    try:
        df = pd.read_excel(file_bytes, engine='openpyxl', skiprows=6, header=None)
        df.columns = ["fecha", "concepto", "importe"]
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
        # Eliminar la última fila si es de totales
        df = df[:-1]

        final_df = df[['fecha', 'concepto', 'importe']].copy()
        final_df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
        final_df['origen'] = 'tarjeta'
        final_df['entidad'] = 'bankinter'
        final_df['transaccion_id'] = final_df.apply(generate_transaction_id, axis=1)
        final_df = final_df[['transaccion_id', 'fecha', 'concepto', 'importe', 'origen', 'entidad']]
    except Exception as e:
        logging.error(f"Fallo al procesar el DataFrame del archivo de tarjeta {file_name}. Error: {e}")
        move_file_to_processed(drive_service, file_id, original_folder_id)
        return

    if final_df.empty:
        logging.warning(f"El archivo de tarjeta {file_name} no produjo datos. Se moverá sin subir nada a GCS.")
        move_file_to_processed(drive_service, file_id, original_folder_id)
        return

    # 3. Convertir a JSONL y subir a GCS
    json_data = final_df.to_json(orient='records', lines=True, date_format='iso')
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Ruta de destino específica para la tarjeta
    destination_blob_name = f"BANKINTER/TARJETA/{Path(file_name).stem}_{file_id}.jsonl" 
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json_data, content_type='application/jsonl+json')
    logging.info(f"Archivo {destination_blob_name} subido a {BUCKET_NAME}.")

    # 4. Mover el archivo original en Drive a "PROCESSED"
    move_file_to_processed(drive_service, file_id, original_folder_id)


def main(event: dict, context: object):
    """
    Cloud Function activada por Cloud Scheduler.
    Busca archivos en la carpeta PENDING de Tarjetas y los procesa uno por uno.
    """
    logging.info("Iniciando ejecución programada para procesar extractos de tarjeta.")

    # 1. Obtener credenciales y construir los clientes
    creds = get_drive_credentials()
    drive_service = build('drive', 'v3', credentials=creds)
    storage_client = storage.Client()

    # 2. Listar archivos en la carpeta PENDING de Drive
    try:
        query = (
            f"'{FOLDER_ID_PENDING_CARD}' in parents and "
            f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
            f"trashed=false"
        )
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, parents)'
        ).execute()
        files_to_process = response.get('files', [])
    except HttpError as e:
        logging.error(f"No se pudo listar archivos en la carpeta de Drive de Tarjeta. Error: {e}")
        raise

    # 3. Procesar los archivos encontrados
    if not files_to_process:
        logging.info("No se encontraron nuevos archivos de tarjeta para procesar.")
        return "Proceso completado, sin archivos nuevos.", 200

    logging.info(f"Se encontraron {len(files_to_process)} archivos de tarjeta para procesar.")
    
    for file_metadata in files_to_process:
        try:
            process_single_file(drive_service, storage_client, file_metadata)
        except Exception as e:
            logging.error(f"Error inesperado al procesar el archivo de tarjeta {file_metadata.get('name')}: {e}")

    return f"Proceso completado. Se procesaron {len(files_to_process)} archivos de tarjeta.", 200