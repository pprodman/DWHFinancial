import os
import io
import json
import logging
import hashlib
import pandas as pd

# Librerías de Google Cloud
from google.cloud import storage, secretmanager
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- Configuración Inicial ---
logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
DRIVE_PARENT_FOLDER_ID = os.environ.get('DRIVE_PARENT_FOLDER_ID')
DRIVE_SECRET_NAME = os.environ.get('DRIVE_SECRET_NAME') # Nuevo: Nombre del secreto en Secret Manager

CONFIG_FILE = 'bank_configs.json'

# --- Clientes de GCP (se inicializan una vez para reutilizar conexiones) ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

def get_drive_credentials() -> service_account.Credentials:
    """Obtiene las credenciales de la cuenta de servicio desde Secret Manager."""
    try:
        secret_version_name = f"projects/{PROJECT_ID}/secrets/{DRIVE_SECRET_NAME}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_version_name})
        creds_json = json.loads(response.payload.data.decode("UTF-8"))
        scopes = ['https://www.googleapis.com/auth/drive']
        return service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
    except Exception as e:
        logging.error(f"Error al obtener las credenciales de Secret Manager: {e}")
        raise

def generate_transaction_id(row: pd.Series) -> str:
    """Genera un ID único para una fila para evitar duplicados."""
    # Aseguramos un formato consistente para que el hash sea determinista
    unique_string = f"{row['fecha']}-{row['concepto']}-{row['importe']}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def process_and_enrich_dataframe(drive_service, file_id: str, config: dict, bank: str, account_type: str) -> pd.DataFrame:
    """
    Lee, limpia, enriquece y estandariza los datos de un archivo XLSX a un DataFrame.
    """
    request = drive_service.files().get_media(fileId=file_id)
    file_bytes = io.BytesIO(request.execute())
    
    df = pd.read_excel(
        file_bytes,
        sheet_name=config.get('sheet_name', 0),
        skiprows=config.get('skip_rows', 0),
        skipfooter=config.get('skip_footer', 0),
        engine='openpyxl'
    )
    
    df = df[list(config['column_mapping'].keys())].rename(columns=config['column_mapping'])
    
    # --- Limpieza y estandarización de tipos de datos ---
    date_format = config.get('date_format')
    df['fecha'] = pd.to_datetime(df['fecha'], format=date_format, errors='coerce').dt.strftime('%Y-%m-%d')
    # Convierte importe a numérico, manejando comas y puntos.
    df['importe'] = pd.to_numeric(df['importe'].astype(str).str.replace(',', '.'), errors='coerce')
    
    # Elimina filas que no se pudieron parsear correctamente
    df.dropna(subset=['fecha', 'concepto', 'importe'], inplace=True)
    if df.empty:
        return df # Devuelve DataFrame vacío si no hay datos válidos

    # --- Enriquecimiento y generación de ID ---
    df['banco'] = bank
    df['tipo_cuenta'] = account_type
    df['transaction_id'] = df.apply(generate_transaction_id, axis=1)

    # Orden final de las columnas
    final_columns = ['transaction_id', 'fecha', 'concepto', 'importe', 'banco', 'tipo_cuenta']
    return df[final_columns]

# (La función move_file_in_drive no cambia, pero podemos añadirle logging)
def move_file_in_drive(drive_service, file_id: str, current_parent_id: str, new_parent_id: str):
    drive_service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=current_parent_id,
        fields='id, parents'
    ).execute()


@functions_framework.http
def ingest_bank_statements(request):
    """Punto de entrada de la Cloud Function."""
    logging.info("Iniciando pipeline de ingesta de extractos bancarios.")
    try:
        with open(CONFIG_FILE, 'r') as f:
            bank_configs = json.load(f)
        
        credentials = get_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.critical(f"Error en la inicialización: {e}")
        return "Error de configuración", 500

    # Lógica de descubrimiento de carpetas (de nuestra versión anterior)
    q_banks = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder'"
    banks = drive_service.files().list(q=q_banks).execute().get('files', [])

    for bank_folder in banks:
        bank_name = bank_folder['name']
        q_accounts = f"'{bank_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder'"
        account_types = drive_service.files().list(q=q_accounts).execute().get('files', [])

        for account_folder in account_types:
            account_type_name = account_folder['name']

            try:
                config_for_file = bank_configs[bank_name][account_type_name]
            except KeyError:
                logging.warning(f"No se encontró config para {bank_name}/{account_type_name}. Se omite.")
                continue
            
            # (Buscar carpetas pending/processed...)
            q_structure = f"'{account_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder'"
            subfolders = drive_service.files().list(q=q_structure).execute().get('files', [])
            pending_folder = next((f for f in subfolders if f['name'] == 'pending'), None)
            processed_folder = next((f for f in subfolders if f['name'] == 'processed'), None)

            if not pending_folder or not processed_folder: continue

            q_files = f"'{pending_folder['id']}' in parents and trashed=false"
            files_to_process = drive_service.files().list(q=q_files).execute().get('files', [])

            for file_item in files_to_process:
                logging.info(f"Procesando archivo: {file_item['name']}")
                try:
                    clean_df = process_and_enrich_dataframe(drive_service, file_item['id'], config_for_file, bank_name, account_type_name)
                    
                    if clean_df.empty:
                        logging.warning(f"El archivo {file_item['name']} no contenía datos válidos tras la limpieza.")
                    else:
                        csv_data = clean_df.to_csv(index=False)
                        gcs_filename = f"{os.path.splitext(file_item['name'])[0]}.csv"
                        gcs_path = f"{bank_name}/{account_type_name}/{gcs_filename}"
                        
                        blob = storage_client.bucket(GCS_BUCKET_NAME).blob(gcs_path)
                        blob.upload_from_string(csv_data, content_type='text/csv')
                        logging.info(f"Subido a GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")

                    # Mover el archivo original independientemente de si tenía datos o no
                    move_file_in_drive(drive_service, file_item['id'], pending_folder['id'], processed_folder['id'])
                    logging.info(f"Archivo {file_item['name']} movido a 'processed' en Drive.")

                except Exception as e:
                    logging.error(f"Error crítico procesando el archivo {file_item['name']}: {e}")

    logging.info("Pipeline de ingesta completado.")
    return 'Proceso completado.', 200