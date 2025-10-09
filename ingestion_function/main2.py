# ingestion_function/main.py

import os
import io
import json
import logging
import hashlib
import locale
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import functions_framework

# Librerías de Google Cloud
from google.cloud import storage, secretmanager, run_v2
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from cloudevents.http import CloudEvent

# --- Configuración y Constantes ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Uso de Path para una mejor gestión de archivos de configuración
CONFIG_FILE = Path('bank_configs.json')

# Nombres de las carpetas de estado
PENDING_FOLDER = 'pending'
PROCESSED_FOLDER = 'processed'
IN_PROGRESS_FOLDER = 'in_progress'

# Variables de entorno
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
DRIVE_PARENT_FOLDER_ID = os.environ.get('DRIVE_PARENT_FOLDER_ID')
DRIVE_SECRET_NAME = os.environ.get('DRIVE_SECRET_NAME')

# --- Clientes de GCP (inicialización única para reutilización en "warm instances") ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()
run_client = run_v2.JobsClient()

# --- Funciones Auxiliares de Configuración y Autenticación ---

def _load_configs() -> Dict[str, Any]:
    """Carga las configuraciones de los bancos desde el archivo JSON."""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical(f"Archivo de configuración '{CONFIG_FILE}' no encontrado.")
        raise
    except json.JSONDecodeError:
        logging.critical(f"Error al decodificar el JSON en '{CONFIG_FILE}'.")
        raise

def get_drive_credentials() -> service_account.Credentials:
    """Obtiene las credenciales de la cuenta de servicio desde Secret Manager."""
    logging.info(f"Obteniendo credenciales del secreto: {DRIVE_SECRET_NAME}")
    try:
        secret_version_name = f"projects/{PROJECT_ID}/secrets/{DRIVE_SECRET_NAME}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_version_name})
        creds_json = json.loads(response.payload.data.decode("UTF-8"))
        scopes = ['https://www.googleapis.com/auth/drive']
        return service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
    except Exception as e:
        logging.error(f"No se pudieron obtener las credenciales de Secret Manager: {e}")
        raise

# --- Funciones de Procesamiento de Datos ---

def _generate_hash_id(row: pd.Series) -> str:
    """Genera un ID único y determinista para una transacción."""
    fecha_str = str(row['fecha'])
    concepto_str = str(row['concepto']).strip().lower()
    importe_str = f"{float(row['importe']):.2f}"
    
    unique_string = f"{fecha_str}-{concepto_str}-{importe_str}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def _download_drive_file_as_bytes(drive_service: Resource, file_id: str, file_name: str) -> io.BytesIO:
    """
    OPTIMIZACIÓN: Aísla la lógica de descarga de Google Drive.
    Maneja tanto Hojas de Cálculo de Google como archivos binarios (Excel).
    """
    file_metadata = drive_service.files().get(fileId=file_id, fields='mimeType').execute()
    mime_type = file_metadata.get('mimeType')

    if mime_type == 'application/vnd.google-apps.spreadsheet':
        logging.info(f"'{file_name}' es una Hoja de Cálculo. Exportando a XLSX...")
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        logging.info(f"'{file_name}' es un archivo binario ({mime_type}). Descargando...")
        request = drive_service.files().get_media(fileId=file_id)
        
    return io.BytesIO(request.execute())

def _transform_dataframe(file_bytes: io.BytesIO, file_name: str, config: Dict[str, Any], bank: str, account_type: str) -> pd.DataFrame:
    """
    OPTIMIZACIÓN: Esta función ahora solo se enfoca en la transformación con Pandas.
    """
    file_extension = Path(file_name).suffix.lower()
    engine = 'openpyxl' if file_extension in ['.xlsx', '.xlsm'] else 'xlrd' if file_extension == '.xls' else None
    
    # Si es una Hoja de Cálculo exportada, siempre será .xlsx compatible
    if not engine:
        engine = 'openpyxl' 

    try:
        df = pd.read_excel(
            file_bytes,
            skiprows=config.get('skip_rows', 0),
            skipfooter=config.get('skip_footer', 0),
            engine=engine
        )
    except Exception as e:
        logging.error(f"Error al leer el archivo '{file_name}' con Pandas: {e}")
        return pd.DataFrame()

    df.rename(columns=config['column_mapping'], inplace=True)
    required_cols = list(config['column_mapping'].values())
    
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logging.error(f"Faltan columnas requeridas en '{file_name}': {missing}")
        return pd.DataFrame()

    df = df[required_cols].copy() # Usar .copy() para evitar SettingWithCopyWarning
    
    df['fecha'] = pd.to_datetime(df['fecha'], format=config.get('date_format'), errors='coerce').dt.strftime('%Y-%m-%d')
    df['importe'] = df['importe'].astype(str).str.replace(r'[^\d,.-]', '', regex=True).apply(locale.atof)
    df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
    
    df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
    if df.empty:
        return df

    df['entidad'] = bank.capitalize()
    df['origen'] = account_type.capitalize()
    df['hash_id'] = df.apply(_generate_hash_id, axis=1)

    final_columns = ['hash_id', 'fecha', 'concepto', 'importe', 'entidad', 'origen']
    return df[final_columns]

# --- Funciones de Interacción con Google Drive ---

def _move_file_in_drive(drive_service: Resource, file_id: str, current_parent_id: str, new_parent_id: str):
    """Mueve un archivo en Drive de una carpeta a otra."""
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=current_parent_id,
            fields='id, parents'
        ).execute()
        logging.info(f"Archivo ID {file_id} movido de {current_parent_id} a {new_parent_id}.")
    except HttpError as e:
        logging.error(f"Error al mover el archivo ID {file_id}: {e}")
        raise

def _get_subfolder_ids(drive_service: Resource, parent_id: str) -> Dict[str, str]:
    """
    OPTIMIZACIÓN: Obtiene los IDs de todas las subcarpetas necesarias en una sola llamada a la API.
    """
    folder_ids = {}
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folders = drive_service.files().list(q=q, fields="files(id, name)").execute().get('files', [])
    
    for folder in folders:
        folder_ids[folder['name']] = folder['id']
    
    # Crear 'in_progress' si no existe
    if IN_PROGRESS_FOLDER not in folder_ids:
        logging.info(f"Creando carpeta '{IN_PROGRESS_FOLDER}' en la carpeta padre {parent_id}.")
        file_metadata = {'name': IN_PROGRESS_FOLDER, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        created_folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        folder_ids[IN_PROGRESS_FOLDER] = created_folder.get('id')

    return folder_ids

# --- Lógica Principal del Pipeline ---

def _process_account_folder(drive_service: Resource, account_folder: Dict[str, str], bank_name: str, config: Dict[str, Any]) -> int:
    """
    Procesa todos los archivos para un tipo de cuenta.
    Devuelve el número de archivos procesados con éxito.
    """
    account_type_name = account_folder['name']
    account_folder_id = account_folder['id']
    logging.info(f"--- Procesando carpeta: {bank_name}/{account_type_name} ---")
    
    # OPTIMIZACIÓN: Una sola llamada a la API para obtener todas las subcarpetas.
    subfolder_ids = _get_subfolder_ids(drive_service, account_folder_id)
    pending_id = subfolder_ids.get(PENDING_FOLDER)
    processed_id = subfolder_ids.get(PROCESSED_FOLDER)
    in_progress_id = subfolder_ids.get(IN_PROGRESS_FOLDER)
    
    if not all([pending_id, processed_id, in_progress_id]):
        logging.error(f"Falta la estructura de carpetas críticas en {bank_name}/{account_type_name}. Se omite.")
        return 0

    files_in_pending = drive_service.files().list(q=f"'{pending_id}' in parents and trashed=false", fields="files(id, name)").execute().get('files', [])
    if not files_in_pending:
        logging.info(f"No hay archivos nuevos en '{PENDING_FOLDER}'.")
        return 0
    
    successful_files = 0
    for file_item in files_in_pending:
        file_id, file_name = file_item['id'], file_item['name']
        logging.info(f"Procesando archivo: {file_name} (ID: {file_id})")

        try:
            _move_file_in_drive(drive_service, file_id, pending_id, in_progress_id)
            
            file_bytes = _download_drive_file_as_bytes(drive_service, file_id, file_name)
            clean_df = _transform_dataframe(file_bytes, file_name, config, bank_name, account_type_name)
            
            if clean_df.empty:
                raise ValueError(f"El DataFrame para '{file_name}' está vacío tras la transformación.")

            jsonl_data = clean_df.to_json(orient='records', lines=True, date_format='iso')
            gcs_path = f"{bank_name}/{account_type_name}/{Path(file_name).stem}.jsonl"
            
            blob = storage_client.bucket(GCS_BUCKET_NAME).blob(gcs_path)
            blob.upload_from_string(jsonl_data, content_type='application/jsonl')
            logging.info(f"Archivo subido a GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")

            _move_file_in_drive(drive_service, file_id, in_progress_id, processed_id)
            logging.info(f"Archivo '{file_name}' procesado con éxito.")
            successful_files += 1

        except Exception as e:
            logging.error(f"Error procesando '{file_name}'. Se devolverá a 'pending'. Error: {e}")
            try:
                _move_file_in_drive(drive_service, file_id, in_progress_id, pending_id)
            except Exception as move_error:
                logging.critical(f"¡FALLO IRRECUPERABLE! No se pudo devolver '{file_name}' a 'pending'. Se quedará en 'in_progress'. Error: {move_error}")
    
    return successful_files

def _trigger_dbt_job():
    """Inicia la ejecución del job de dbt en Cloud Run."""
    try:
        logging.info("Iniciando ejecución del job de dbt...")
        job_name = f"projects/{PROJECT_ID}/locations/us-central1/jobs/dbt-transform-job"
        operation = run_client.run_job(request=run_v2.RunJobRequest(name=job_name))
        logging.info(f"Job de dbt iniciado con operación: {operation.operation.name}")
    except Exception as e:
        logging.error(f"Error al iniciar el job de dbt: {e}")

# --- Punto de Entrada de la Cloud Function ---
@functions_framework.cloud_event
def ingest_bank_statements_pubsub(cloud_event: CloudEvent):
    """Punto de entrada de la Cloud Function, activado por Pub/Sub."""
    logging.info(f"Función activada por evento Pub/Sub: {cloud_event['id']}")
    logging.info("===== INICIANDO PIPELINE DE INGESTA DE EXTRACTOS BANCARIOS =====")
    
    total_files_processed = 0
    try:
        bank_configs = _load_configs()
        credentials = get_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)

        q_banks = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        bank_folders = drive_service.files().list(q=q_banks, fields="files(id, name)").execute().get('files', [])

        if not bank_folders:
            logging.warning(f"No se encontraron carpetas de bancos en la carpeta padre ID: {DRIVE_PARENT_FOLDER_ID}")

        for bank_folder in bank_folders:
            bank_name = bank_folder['name']
            q_accounts = f"'{bank_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            account_folders = drive_service.files().list(q=q_accounts, fields="files(id, name)").execute().get('files', [])

            for account_folder in account_folders:
                account_type = account_folder['name']
                try:
                    config = bank_configs[bank_name][account_type]
                    total_files_processed += _process_account_folder(drive_service, account_folder, bank_name, config)
                except KeyError:
                    logging.warning(f"No se encontró configuración para '{bank_name}/{account_type}'. Se omite.")
                except Exception as e:
                    logging.error(f"Error inesperado procesando '{bank_name}/{account_type}': {e}", exc_info=True)

    except Exception as e:
        logging.critical(f"Error fatal durante la inicialización o ejecución principal: {e}", exc_info=True)
        # Podrías decidir si retornar un error aquí o no.
    
    # OPTIMIZACIÓN: Solo ejecutar dbt si se procesó algo.
    if total_files_processed > 0:
        logging.info(f"Se procesaron {total_files_processed} archivos en total. Disparando job de dbt.")
        _trigger_dbt_job()
    else:
        logging.info("No se procesaron archivos nuevos. El job de dbt no se ejecutará.")

    logging.info("===== PIPELINE DE INGESTA COMPLETADO =====")
    return 'Proceso completado.', 200