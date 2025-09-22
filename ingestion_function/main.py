# ingestion_function/main.py

import os
import io
import json
import logging
import hashlib
import base64
import pandas as pd
import functions_framework

# Librerías de Google Cloud
from google.cloud import storage, secretmanager
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from cloudevents.http import CloudEvent

# --- Configuración y Constantes ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
DRIVE_PARENT_FOLDER_ID = os.environ.get('DRIVE_PARENT_FOLDER_ID')
DRIVE_SECRET_NAME = os.environ.get('DRIVE_SECRET_NAME')

CONFIG_FILE = 'bank_configs.json'
PENDING_FOLDER_NAME = 'pending'
PROCESSED_FOLDER_NAME = 'processed'
IN_PROGRESS_FOLDER_NAME = 'in_progress'

# --- Clientes de GCP (se inicializan una vez) ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()


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

def _generate_transaction_id(row: pd.Series) -> str:
    """Genera un ID único y determinista para una transacción."""
    fecha_str = str(row['fecha'])
    concepto_str = str(row['concepto']).strip().lower()
    importe_str = f"{float(row['importe']):.2f}"
    
    unique_string = f"{fecha_str}-{concepto_str}-{importe_str}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

# ingestion_function/main.py

def _process_and_enrich_dataframe(drive_service, file_id: str, file_name: str, config: dict, bank: str, account_type: str) -> pd.DataFrame:
    """
    MEJORADO: Lee, limpia, enriquece y estandariza los datos.
    Usa una estrategia de selección por posición para ser más robusto.
    """
    try:
        file_extension = os.path.splitext(file_name)[1].lower()
        engine = None
        if file_extension == '.xlsx':
            engine = 'openpyxl'
        elif file_extension == '.xls':
            engine = 'xlrd'
        else:
            logging.error(f"Formato de archivo no soportado para '{file_name}': {file_extension}")
            return pd.DataFrame()

        logging.info(f"Leyendo archivo '{file_name}' con el motor de Pandas: '{engine}'")
        
        request = drive_service.files().get_media(fileId=file_id)
        file_bytes = io.BytesIO(request.execute())
        
        # Leemos el excel SIN encabezado, para que las columnas sean numéricas (0, 1, 2...)
        df = pd.read_excel(
            file_bytes,
            sheet_name=config.get('sheet_name', 0),
            skiprows=config.get('skip_rows', 0),
            skipfooter=config.get('skip_footer', 0),
            engine=engine,
            header=None  # <-- ¡CLAVE! Le decimos a Pandas que no hay encabezado.
        )
    except Exception as e:
        logging.error(f"Error al leer el archivo Excel '{file_name}' con Pandas: {e}")
        return pd.DataFrame()

    # --- Nueva Lógica de Selección y Renombrado ---
    try:
        # 1. Seleccionar columnas por su posición (ej. 1, 2, 3)
        column_positions = config['use_columns_by_position']
        df = df.iloc[:, column_positions]
        
        # 2. Asignar los nombres de columna finales
        final_names = config['final_column_names']
        df.columns = final_names
        
        required_cols = final_names
    except (KeyError, IndexError) as e:
        logging.error(f"Error de configuración o de estructura de archivo para '{file_name}'. Revisa 'use_columns_by_position' y 'final_column_names' en bank_configs.json. Error: {e}")
        return pd.DataFrame()
        
    # --- El resto de la lógica continúa igual, ahora con un DataFrame limpio ---
    df['fecha'] = pd.to_datetime(df['fecha'], format=config.get('date_format'), errors='coerce').dt.strftime('%Y-%m-%d')
    df['importe'] = pd.to_numeric(df['importe'].astype(str).str.replace(',', '.'), errors='coerce')
    
    df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
    if df.empty:
        return df

    df['banco'] = bank
    df['tipo_cuenta'] = account_type
    df['transaction_id'] = df.apply(_generate_transaction_id, axis=1)

    final_columns = ['transaction_id', 'fecha', 'concepto', 'importe', 'banco', 'tipo_cuenta']
    return df[final_columns]

def _move_file_in_drive(drive_service, file_id: str, current_parent_id: str, new_parent_id: str):
    """Mueve un archivo en Drive y maneja errores."""
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=current_parent_id,
            fields='id, parents'
        ).execute()
        logging.info(f"Archivo ID {file_id} movido de la carpeta {current_parent_id} a {new_parent_id}.")
    except HttpError as e:
        logging.error(f"Error al mover el archivo ID {file_id}: {e}")
        raise

def _get_folder_id(drive_service, parent_id: str, folder_name: str, create_if_not_exists: bool = False) -> str | None:
    """Busca el ID de una subcarpeta por su nombre. Opcionalmente, la crea si no existe."""
    q = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folders = drive_service.files().list(q=q, fields="files(id)").execute().get('files', [])
    
    if folders:
        return folders[0]['id']
    elif create_if_not_exists:
        logging.info(f"La carpeta '{folder_name}' no existe en la carpeta padre {parent_id}. Creándola...")
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')
    return None

def _process_account_folder(drive_service, account_folder: dict, bank_name: str, config: dict):
    """
    MEJORADO: Procesa todos los archivos para un tipo de cuenta específico.
    Si un archivo falla, lo devuelve a la carpeta 'pending' para reintentarlo.
    """
    account_type_name = account_folder['name']
    account_folder_id = account_folder['id']
    logging.info(f"--- Procesando carpeta de cuenta: {bank_name}/{account_type_name} ---")

    # Obtener IDs de carpetas, creando 'in_progress' si no existe.
    pending_id = _get_folder_id(drive_service, account_folder_id, PENDING_FOLDER_NAME)
    processed_id = _get_folder_id(drive_service, account_folder_id, PROCESSED_FOLDER_NAME)
    in_progress_id = _get_folder_id(drive_service, account_folder_id, IN_PROGRESS_FOLDER_NAME, create_if_not_exists=True)

    if not all([pending_id, processed_id, in_progress_id]):
        logging.error(f"Falta la estructura de carpetas críticas en {bank_name}/{account_type_name}. Se omite.")
        return

    # Buscar archivos en la carpeta 'pending'
    q_files = f"'{pending_id}' in parents and trashed=false"
    files = drive_service.files().list(q=q_files, fields="files(id, name)").execute().get('files', [])

    if not files:
        logging.info(f"No hay archivos nuevos en la carpeta '{PENDING_FOLDER_NAME}'.")
        return

    for file_item in files:
        file_id = file_item['id']
        file_name = file_item['name']
        logging.info(f"Procesando archivo: {file_name} (ID: {file_id})")

        try:
            # 1. Mover a 'in_progress' para evitar que otra instancia lo tome.
            _move_file_in_drive(drive_service, file_id, pending_id, in_progress_id)
            
            # 2. Intentar procesar el archivo.
            clean_df = _process_and_enrich_dataframe(drive_service, file_id, file_name, config, bank_name, account_type_name)
            
            if clean_df.empty:
                raise ValueError(f"El DataFrame resultante para '{file_name}' está vacío. No hay datos válidos para procesar.")

            # 3. Si hay datos, subirlos a GCS.
            jsonl_data = clean_df.to_json(orient='records', lines=True, date_format='iso')
            gcs_path = f"{bank_name}/{account_type_name}/{os.path.splitext(file_name)[0]}.jsonl"
            
            blob = storage_client.bucket(GCS_BUCKET_NAME).blob(gcs_path)
            blob.upload_from_string(jsonl_data, content_type='application/jsonl')
            logging.info(f"Archivo subido a GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")

            # 4. Si todo ha ido bien, mover de 'in_progress' a 'processed'.
            _move_file_in_drive(drive_service, file_id, in_progress_id, processed_id)
            logging.info(f"Archivo '{file_name}' procesado con éxito.")

        except Exception as e:
            # --- NUEVA LÓGICA DE MANEJO DE ERRORES ---
            # Si cualquier paso del bloque 'try' falla, se ejecuta este bloque.
            logging.error(f"Error procesando el archivo '{file_name}'. Se devolverá a 'pending' para reintentarlo. Error: {e}")
            try:
                # Intentar mover el archivo de 'in_progress' de vuelta a 'pending'.
                _move_file_in_drive(drive_service, file_id, in_progress_id, pending_id)
            except Exception as move_error:
                # Si incluso el movimiento de vuelta falla, el archivo se quedará en 'in_progress'.
                logging.critical(f"¡FALLO IRRECUPERABLE! No se pudo devolver el archivo '{file_name}' a 'pending'. Se quedará en 'in_progress'. Error de movimiento: {move_error}")

# --- Punto de Entrada de la Cloud Function (Modificado para Pub/Sub) ---
@functions_framework.cloud_event
def ingest_bank_statements_pubsub(cloud_event: CloudEvent):
    """
    Punto de entrada de la Cloud Function, activado por un mensaje de Pub/Sub.
    El contenido del mensaje no se usa, solo sirve como señal para iniciar el proceso.
    """
    logging.info(f"Función activada por el evento de Pub/Sub: {cloud_event['id']}")
    
    logging.info("===== INICIANDO PIPELINE DE INGESTA DE EXTRACTOS BANCARIOS =====")
    try:
        with open('bank_configs.json', 'r') as f:
            bank_configs = json.load(f)
        
        credentials = get_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.critical(f"Error fatal en la inicialización (config o credenciales): {e}")
        raise e

    q_banks = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    banks = drive_service.files().list(q=q_banks, fields="files(id, name)").execute().get('files', [])

    if not banks:
        logging.warning(f"No se encontraron carpetas de bancos dentro de la carpeta padre ID: {DRIVE_PARENT_FOLDER_ID}")

    for bank_folder in banks:
        bank_name = bank_folder['name']
        q_accounts = f"'{bank_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        account_types = drive_service.files().list(q=q_accounts, fields="files(id, name)").execute().get('files', [])

        for account_folder in account_types:
            account_type_name = account_folder['name']
            try:
                config_for_file = bank_configs[bank_name][account_type_name]
                _process_account_folder(drive_service, account_folder, bank_name, config_for_file)
            except KeyError:
                logging.warning(f"No se encontró configuración para '{bank_name}/{account_type_name}'. Se omite.")
            except Exception as e:
                logging.error(f"Error inesperado procesando la carpeta de cuenta '{bank_name}/{account_type_name}': {e}")

    logging.info("===== PIPELINE DE INGESTA COMPLETADO =====")
    return 'Proceso completado.', 200