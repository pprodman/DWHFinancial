import os
import io
import json
import logging
import hashlib
import pandas as pd
import functions_framework

# Librerías de Google Cloud
from google.cloud import storage, secretmanager
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# --- Configuración y Constantes ---
# Configura un logging más informativo.
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Cargar configuración desde variables de entorno para mayor flexibilidad.
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
DRIVE_PARENT_FOLDER_ID = os.environ.get('DRIVE_PARENT_FOLDER_ID')
DRIVE_SECRET_NAME = os.environ.get('DRIVE_SECRET_NAME')

CONFIG_FILE = 'bank_configs.json'
PENDING_FOLDER_NAME = 'pending'
PROCESSED_FOLDER_NAME = 'processed'
IN_PROGRESS_FOLDER_NAME = 'in_progress' # Carpeta para garantizar idempotencia.

# --- Clientes de GCP (se inicializan una vez para reutilizar conexiones) ---
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
    """
    Genera un ID único y determinista para una transacción.
    Normaliza los datos antes de crear el hash para asegurar consistencia.
    """
    # Normalizar los datos: quitar espacios, formato de fecha estándar, importe con 2 decimales.
    fecha_str = str(row['fecha'])
    concepto_str = str(row['concepto']).strip().lower() # Añadir .lower() para más consistencia
    importe_str = f"{float(row['importe']):.2f}"
    
    unique_string = f"{fecha_str}-{concepto_str}-{importe_str}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def _process_and_enrich_dataframe(drive_service, file_id: str, config: dict, bank: str, account_type: str) -> pd.DataFrame:
    """
    Lee, limpia, enriquece y estandariza los datos de un archivo XLSX a un DataFrame.
    """
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_bytes = io.BytesIO(request.execute())
        
        df = pd.read_excel(
            file_bytes,
            sheet_name=config.get('sheet_name', 0),
            skiprows=config.get('skip_rows', 0),
            skipfooter=config.get('skip_footer', 0),
            engine='openpyxl'
        )
    except HttpError as e:
        logging.error(f"Error de la API de Drive al descargar el archivo ID {file_id}: {e}")
        return pd.DataFrame() # Devuelve un DataFrame vacío en caso de error

    # Renombrar columnas de forma más segura
    df.rename(columns=config['column_mapping'], inplace=True)
    required_cols = list(config['column_mapping'].values())
    
    # Verificar si todas las columnas requeridas existen después de renombrar
    if not all(col in df.columns for col in required_cols):
        logging.error(f"Faltan columnas requeridas en el archivo. Se esperaban: {required_cols}, se encontraron: {list(df.columns)}")
        return pd.DataFrame()

    df = df[required_cols] # Seleccionar solo las columnas necesarias

    # Limpieza y estandarización
    df['fecha'] = pd.to_datetime(df['fecha'], format=config.get('date_format'), errors='coerce')
    df['importe'] = pd.to_numeric(df['importe'].astype(str).str.replace(',', '.'), errors='coerce')
    
    # Elimina filas con valores nulos en columnas críticas
    df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
    if df.empty:
        return df

    # Enriquecimiento y generación de ID
    df['banco'] = bank
    df['tipo_cuenta'] = account_type
    df['transaction_id'] = df.apply(_generate_transaction_id, axis=1)

    # Orden final de las columnas
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
    """Procesa todos los archivos para un tipo de cuenta específico."""
    account_type_name = account_folder['name']
    account_folder_id = account_folder['id']
    logging.info(f"--- Procesando carpeta de cuenta: {bank_name}/{account_type_name} ---")

    # Obtener IDs de carpetas (crear 'in_progress' si no existe)
    pending_id = _get_folder_id(drive_service, account_folder_id, PENDING_FOLDER_NAME)
    processed_id = _get_folder_id(drive_service, account_folder_id, PROCESSED_FOLDER_NAME)
    in_progress_id = _get_folder_id(drive_service, account_folder_id, IN_PROGRESS_FOLDER_NAME, create_if_not_exists=True)

    if not all([pending_id, processed_id, in_progress_id]):
        logging.warning(f"Falta la estructura de carpetas 'pending' o 'processed' en {bank_name}/{account_type_name}. Se omite.")
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
            # LÓGICA DE IDEMPOTENCIA: Mover a 'in_progress' ANTES de procesar.
            _move_file_in_drive(drive_service, file_id, pending_id, in_progress_id)
            
            clean_df = _process_and_enrich_dataframe(drive_service, file_id, config, bank_name, account_type_name)
            
            if not clean_df.empty:
                # Convertir el DataFrame a formato JSON Lines
                jsonl_data = clean_df.to_json(orient='records', lines=True, date_format='iso')
                
                # Cambiar la extensión del archivo en GCS
                gcs_path = f"{bank_name}/{account_type_name}/{os.path.splitext(file_name)[0]}.jsonl"
                
                blob = storage_client.bucket(GCS_BUCKET_NAME).blob(gcs_path)
                blob.upload_from_string(jsonl_data, content_type='application/jsonl')
                logging.info(f"Archivo subido a GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")
            else:
                logging.warning(f"El archivo {file_name} no contenía datos válidos tras la limpieza.")

            # Mover a 'processed' solo si todo lo anterior tuvo éxito
            _move_file_in_drive(drive_service, file_id, in_progress_id, processed_id)

        except Exception as e:
            logging.error(f"Error CRÍTICO procesando el archivo {file_name}. Se quedará en 'in_progress' para revisión manual. Error: {e}")
            # El archivo se queda en 'in_progress', evitando reintentos infinitos.

@functions_framework.http
def ingest_bank_statements(request):
    """Punto de entrada de la Cloud Function que escanea Google Drive y procesa archivos."""
    logging.info("===== INICIANDO PIPELINE DE INGESTA DE EXTRACTOS BANCARIOS =====")
    try:
        with open(CONFIG_FILE, 'r') as f:
            bank_configs = json.load(f)
        
        credentials = get_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.critical(f"Error fatal en la inicialización (config o credenciales): {e}")
        return "Error de configuración", 500

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