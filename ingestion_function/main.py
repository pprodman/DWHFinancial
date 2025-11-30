import os
import io
import json
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# --- Configuración y Constantes ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

CONFIG_FILE = Path("ingestion_function/bank_configs.json") # Ajusta ruta si es necesario
if not CONFIG_FILE.exists(): CONFIG_FILE = Path("bank_configs.json")

PENDING_FOLDER = "PENDING"
PROCESSED_FOLDER = "PROCESSED"
IN_PROGRESS_FOLDER = "in_progress"

# Variables de Entorno (Vienen de GitHub Actions)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
DRIVE_PARENT_FOLDER_ID = os.environ.get("DRIVE_PARENT_FOLDER_ID")

# Cliente GCS (Auth automática por variable de entorno)
storage_client = storage.Client()

# --- Funciones Auxiliares (Idénticas a tu lógica) ---

def load_configs() -> Dict[str, Any]:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical(f"Archivo de configuración '{CONFIG_FILE}' no encontrado.")
        return {}

def generate_hash_id(row: pd.Series) -> str:
    fecha_str = str(row["fecha"])
    concepto_str = str(row["concepto"]).strip().lower()
    importe_str = f"{float(row['importe']):.2f}"
    unique_string = f"{fecha_str}-{concepto_str}-{importe_str}"
    return hashlib.sha256(unique_string.encode("utf-8")).hexdigest()

def download_drive_file_as_bytes(drive_service: Resource, file_id: str) -> tuple[io.BytesIO, str, str]:
    file_metadata = drive_service.files().get(fileId=file_id, fields="mimeType, name").execute()
    mime_type = file_metadata.get("mimeType")
    file_name = file_metadata.get("name")

    if mime_type == "application/vnd.google-apps.spreadsheet":
        logging.info(f"Exportando hoja de cálculo: {file_name}")
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        effective_type = "excel"
    else:
        request = drive_service.files().get_media(fileId=file_id)
        effective_type = Path(file_name).suffix.lower()

    return io.BytesIO(request.execute()), effective_type, file_name

def unify_importe_format(x):
    if pd.isna(x): return x
    s = str(x).strip()
    # Lógica simple para importes europeos/americanos
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s: s = s.replace(",", ".")
    try: return float(s)
    except: return float("nan")

def transform_dataframe(file_bytes, file_type, file_name, config, bank, account_type) -> pd.DataFrame:
    try:
        if "csv" in file_type:
            df = pd.read_csv(file_bytes, skiprows=config.get("skip_rows", 0), skipfooter=config.get("skip_footer", 0), engine="python", encoding="utf-8")
        else:
            df = pd.read_excel(file_bytes, skiprows=config.get("skip_rows", 0), skipfooter=config.get("skip_footer", 0))
    except Exception as e:
        logging.error(f"Error leyendo {file_name}: {e}")
        return pd.DataFrame()

    # Tu lógica de mapeo
    if "column_mapping" not in config: return pd.DataFrame()
    
    # Filtrar columnas existentes
    cols_to_rename = {k: v for k, v in config["column_mapping"].items() if k in df.columns}
    df.rename(columns=cols_to_rename, inplace=True)
    
    required_cols = list(config["column_mapping"].values())
    if not all(col in df.columns for col in required_cols):
        logging.warning(f"Faltan columnas en {file_name}. Se encontraron: {df.columns.tolist()}")
        return pd.DataFrame()

    df = df[required_cols].copy()
    df["fecha"] = pd.to_datetime(df["fecha"], format=config.get("date_format"), errors="coerce").dt.strftime("%Y-%m-%d")
    df["importe"] = df["importe"].apply(unify_importe_format)
    df.dropna(subset=["fecha", "concepto", "importe"], inplace=True)
    
    if df.empty: return df

    df["entidad"] = bank.capitalize()
    df["origen"] = account_type.capitalize()
    df["hash_id"] = df.apply(generate_hash_id, axis=1)
    
    return df[["hash_id", "fecha", "concepto", "importe", "entidad", "origen"]]

def move_file_in_drive(drive_service, file_id, current_parent, new_parent):
    try:
        drive_service.files().update(fileId=file_id, addParents=new_parent, removeParents=current_parent, fields="id, parents").execute()
    except Exception as e:
        logging.error(f"Error moviendo archivo Drive: {e}")

def get_subfolder_ids(drive_service, parent_id) -> Dict[str, str]:
    folder_ids = {}
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folders = drive_service.files().list(q=q, fields="files(id, name)").execute().get("files", [])
    for f in folders: folder_ids[f["name"]] = f["id"]
    
    # Crear IN_PROGRESS si no existe
    if IN_PROGRESS_FOLDER not in folder_ids:
        m = {"name": IN_PROGRESS_FOLDER, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
        f = drive_service.files().create(body=m, fields="id").execute()
        folder_ids[IN_PROGRESS_FOLDER] = f.get("id")
    return folder_ids

def process_account_folder(drive_service, account_folder, bank_name, config) -> int:
    acc_name = account_folder["name"]
    acc_id = account_folder["id"]
    logging.info(f"--- Revisando carpeta: {bank_name}/{acc_name} ---")

    subs = get_subfolder_ids(drive_service, acc_id)
    pending, processed, progress = subs.get(PENDING_FOLDER), subs.get(PROCESSED_FOLDER), subs.get(IN_PROGRESS_FOLDER)

    if not pending or not processed:
        logging.warning(f"Faltan carpetas PENDING/PROCESSED en {acc_name}")
        return 0

    files = drive_service.files().list(q=f"'{pending}' in parents and trashed=false", fields="files(id, name)").execute().get("files", [])
    
    count = 0
    for f in files:
        fid, fname = f["id"], f["name"]
        logging.info(f"Procesando: {fname}")
        
        try:
            # 1. Mover a In Progress
            move_file_in_drive(drive_service, fid, pending, progress)
            
            # 2. Descargar y Transformar
            fbytes, ftype, fname = download_drive_file_as_bytes(drive_service, fid)
            df = transform_dataframe(fbytes, ftype, fname, config, bank_name, acc_name)
            
            if not df.empty:
                # 3. Subir a GCS (JSONL)
                json_data = df.to_json(orient="records", lines=True, date_format="iso")
                gcs_path = f"{bank_name}/{acc_name}/{Path(fname).stem}.jsonl"
                storage_client.bucket(GCS_BUCKET_NAME).blob(gcs_path).upload_from_string(json_data, "application/jsonl")
                logging.info(f"Subido a GCS: {gcs_path}")
                
                # 4. Mover a Processed
                move_file_in_drive(drive_service, fid, progress, processed)
                count += 1
            else:
                logging.warning(f"Archivo vacío o inválido: {fname}")
                move_file_in_drive(drive_service, fid, progress, pending) # Devolver

        except Exception as e:
            logging.error(f"Error en archivo {fname}: {e}")
            try: move_file_in_drive(drive_service, fid, progress, pending)
            except: pass

    return count

# --- MAIN ---
if __name__ == "__main__":
    logging.info("🚀 Iniciando Ingesta (GitHub Actions -> Drive -> GCS)")
    
    # Autenticación vía Variable de Entorno (GitHub Secret)
    creds = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/cloud-platform"]
    )
    
    drive = build("drive", "v3", credentials=creds)
    configs = load_configs()
    
    q_banks = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    banks = drive.files().list(q=q_banks, fields="files(id, name)").execute().get("files", [])

    total = 0
    for b in banks:
        bname = b["name"]
        q_acc = f"'{b['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        accs = drive.files().list(q=q_acc, fields="files(id, name)").execute().get("files", [])
        
        for acc in accs:
            aname = acc["name"]
            if bname in configs and aname in configs[bname]:
                total += process_account_folder(drive, acc, bname, configs[bname][aname])

    logging.info(f"✅ Proceso finalizado. Total archivos procesados: {total}")