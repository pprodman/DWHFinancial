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
from google.oauth2 import service_account

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)

# Estas variables se configurarán en el despliegue de la Cloud Function
PROJECT_ID = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
SECRET_NAME = os.environ.get("DRIVE_SECRET_NAME")
# CAMBIO CLAVE 1: Variable de entorno para la carpeta PROCESSED de la TARJETA
FOLDER_ID_PROCESSED_CARD = os.environ.get("FOLDER_ID_PROCESSED_CARD") 


def get_drive_credentials() -> service_account.Credentials:
    """Obtiene las credenciales de la cuenta de servicio desde Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_version_name = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": secret_version_name})
        creds_json = json.loads(response.payload.data.decode("UTF-8"))
        
        # Define los permisos (scopes) que necesita la credencial
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

def move_file_to_processed(credentials: service_account.Credentials, file_id: str, original_folder_id: str):
    """Mueve un archivo de la carpeta PENDING a la PROCESSED en Google Drive."""
    try:
        drive_service = build('drive', 'v3', credentials=credentials)
        drive_service.files().update(
            fileId=file_id,
            addParents=FOLDER_ID_PROCESSED_CARD, # Usa la variable correcta
            removeParents=original_folder_id,
            fields='id, parents'
        ).execute()
        logging.info(f"Archivo {file_id} movido a la carpeta PROCESSED de Tarjeta.")
    except Exception as e:
        logging.error(f"No se pudo mover el archivo {file_id } a la carpeta PROCESSED: {e}")

def main(event: dict, context: object):
    """Cloud Function activada por Eventarc al subir un archivo a la carpeta PENDING de Tarjetas."""
    
    # 1. Extraer información del evento de Drive
    try:
        file_id = event["data"]["payload"]["id"]
        file_name = event["data"]["payload"]["name"]
        original_folder_id = event["data"]["payload"]["parents"][0]["id"]
        logging.info(f"TARJETA: Procesando '{file_name}' (ID: {file_id})")
    except (KeyError, IndexError) as e:
        logging.error(f"El evento no tiene el formato esperado: {event}. Error: {e}")
        return "Formato de evento incorrecto", 400

    # 2. Obtener credenciales y descargar el archivo
    creds = get_drive_credentials()
    drive_service = build('drive', 'v3', credentials=creds)
    request = drive_service.files().get_media(fileId=file_id)
    file_bytes = io.BytesIO(request.execute())
    
    # 3: Lógica de procesamiento de Pandas específica para la TARJETA
    try:
        df = pd.read_excel('tarjeta_julio.xlsx', engine='openpyxl', skiprows=6, header=None)

        # Renombrar columnas para facilitar el manejo
        df.columns = ["fecha", "concepto", "importe"]

        # Limpieza y conversión de datos
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')

        # Eliminar la última fila
        df = df[:-1]

        final_df = df[['fecha', 'concepto', 'importe']].copy()
        final_df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
        final_df['origen'] = 'tarjeta'
        final_df['entidad'] = 'bankinter'
        final_df['transaccion_id'] = final_df.apply(generate_transaction_id, axis=1)

        # Reordenar columnas para que coincida con el schema de BigQuery
        final_df = final_df[['transaccion_id', 'fecha', 'concepto', 'importe', 'origen', 'entidad']]

    except Exception as e:
        logging.error(f"Fallo al procesar el DataFrame del archivo {file_name}. Error: {e}")
        raise

    if final_df.empty:
        logging.warning(f"El archivo {file_name} no produjo datos tras la limpieza. Finalizando.")
        return "Archivo vacío o sin datos válidos", 200

    # Convertir a JSONL y subir a GCS (casi sin cambios)
    json_data = final_df.to_json(orient='records', lines=True, date_format='iso')
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    # CAMBIO CLAVE 5: Guardar en una subcarpeta 'tarjeta' en GCS para mayor orden
    destination_blob_name = f"tarjeta/{Path(file_name).stem}_{context.event_id}.jsonl"
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json_data, content_type='application/jsonl+json')
    logging.info(f"Archivo {destination_blob_name} subido a {BUCKET_NAME}.")

    # Mover el archivo original en Drive (lógica sin cambios)
    move_file_to_processed(creds, file_id, original_folder_id)

    return "Proceso de TARJETA completado", 200