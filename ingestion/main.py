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
from google.oauth2 import service_account
from dotenv import load_dotenv

# --- CONFIGURACIÓN INICIAL ---
# Carga variables del archivo .env que está en la raíz del proyecto
# (Subimos dos niveles desde ingestion/main.py para llegar a la raíz)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- RUTAS DE CONFIGURACIÓN ---
# El archivo JSON de config ahora vive en ingestion/config/
CONFIG_FILE = BASE_DIR / "ingestion" / "config" / "bank_configs.json"

PENDING_FOLDER = "PENDING"
PROCESSED_FOLDER = "PROCESSED"
IN_PROGRESS_FOLDER = "in_progress"

# Variables de entorno
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
DRIVE_PARENT_FOLDER_ID = os.environ.get("DRIVE_PARENT_FOLDER_ID")

# Cliente GCS
storage_client = storage.Client()

# --- FUNCIONES AUXILIARES ---


def load_configs() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        logging.critical(
            f"❌ No se encuentra el archivo de configuración en: {CONFIG_FILE}"
        )
        return {}

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.critical(f"❌ Error leyendo JSON de configuración: {e}")
        return {}


def generate_hash_id(row: pd.Series) -> str:
    # Usamos valores str() para evitar errores de tipos
    fecha_str = str(row["fecha"])
    concepto_str = str(row["concepto"]).strip().lower()
    importe_str = f"{float(row['importe']):.2f}"

    # Concatenamos para crear una firma única
    unique_string = f"{fecha_str}-{concepto_str}-{importe_str}"

    # SHA256
    return hashlib.sha256(unique_string.encode("utf-8")).hexdigest()


def download_drive_file_as_bytes(
    drive_service: Resource, file_id: str
) -> tuple[io.BytesIO, str, str]:
    file_metadata = (
        drive_service.files().get(fileId=file_id, fields="mimeType, name").execute()
    )
    mime_type = file_metadata.get("mimeType")
    file_name = file_metadata.get("name")

    if mime_type == "application/vnd.google-apps.spreadsheet":
        logging.info(f"📥 Exportando Google Sheet: {file_name}")
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        effective_type = "excel"
    else:
        logging.info(f"📥 Descargando archivo: {file_name}")
        request = drive_service.files().get_media(fileId=file_id)
        effective_type = Path(file_name).suffix.lower()

    return io.BytesIO(request.execute()), effective_type, file_name


def unify_importe_format(x):
    if pd.isna(x):
        return x
    s = str(x).strip()
    # Lógica simple para detectar formato europeo (1.000,00) vs americano (1,000.00)
    # Si tiene coma y punto, asumimos formato europeo o americano complejo.
    # Esta lógica es básica, ajústala según tus bancos.
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return float("nan")


def transform_dataframe(
    file_bytes, file_type, file_name, config, bank, account_type
) -> pd.DataFrame:
    try:
        if "csv" in file_type:
            df = pd.read_csv(
                file_bytes,
                skiprows=config.get("skip_rows", 0),
                skipfooter=config.get("skip_footer", 0),
                engine="python",
                encoding="utf-8",
            )
        else:
            df = pd.read_excel(
                file_bytes,
                skiprows=config.get("skip_rows", 0),
                skipfooter=config.get("skip_footer", 0),
            )
    except Exception as e:
        logging.error(f"⚠️ Error leyendo estructura de {file_name}: {e}")
        return pd.DataFrame()

    if "column_mapping" not in config:
        return pd.DataFrame()

    # Renombrar columnas según configuración
    cols_to_rename = {
        k: v for k, v in config["column_mapping"].items() if k in df.columns
    }
    df.rename(columns=cols_to_rename, inplace=True)

    required_cols = list(config["column_mapping"].values())

    # Verificar que existan las columnas destino
    if not all(col in df.columns for col in required_cols):
        logging.warning(
            f"⚠️ Faltan columnas requeridas en {file_name}. Se encontraron: {df.columns.tolist()}"
        )
        return pd.DataFrame()

    df = df[required_cols].copy()

    # Limpieza de datos
    df["fecha"] = pd.to_datetime(
        df["fecha"], format=config.get("date_format"), errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df["importe"] = df["importe"].apply(unify_importe_format)

    # Eliminar filas vacías críticas
    df.dropna(subset=["fecha", "concepto", "importe"], inplace=True)

    if df.empty:
        return df

    # Enriquecimiento
    df["entidad"] = bank.capitalize()
    df["origen"] = account_type.capitalize()
    df["hash_id"] = df.apply(generate_hash_id, axis=1)

    return df[["hash_id", "fecha", "concepto", "importe", "entidad", "origen"]]


def move_file_in_drive(drive_service, file_id, current_parent, new_parent):
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=new_parent,
            removeParents=current_parent,
            fields="id, parents",
        ).execute()
    except Exception as e:
        logging.error(f"⚠️ Error moviendo archivo en Drive: {e}")


def get_subfolder_ids(drive_service, parent_id) -> Dict[str, str]:
    folder_ids = {}
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folders = (
        drive_service.files()
        .list(q=q, fields="files(id, name)")
        .execute()
        .get("files", [])
    )
    for f in folders:
        folder_ids[f["name"]] = f["id"]

    # Auto-crear carpeta IN_PROGRESS si no existe
    if IN_PROGRESS_FOLDER not in folder_ids:
        m = {
            "name": IN_PROGRESS_FOLDER,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        f = drive_service.files().create(body=m, fields="id").execute()
        folder_ids[IN_PROGRESS_FOLDER] = f.get("id")
    return folder_ids


def process_account_folder(drive_service, account_folder, bank_name, config) -> int:
    acc_name = account_folder["name"]
    acc_id = account_folder["id"]
    logging.info(f"🔎 Revisando carpeta: {bank_name} / {acc_name}")

    subs = get_subfolder_ids(drive_service, acc_id)
    pending, processed, progress = (
        subs.get(PENDING_FOLDER),
        subs.get(PROCESSED_FOLDER),
        subs.get(IN_PROGRESS_FOLDER),
    )

    if not pending or not processed:
        logging.warning(
            f"⚠️ Estructura incompleta en {acc_name} (Falta PENDING o PROCESSED)"
        )
        return 0

    # Listar archivos en PENDING
    files = (
        drive_service.files()
        .list(q=f"'{pending}' in parents and trashed=false", fields="files(id, name)")
        .execute()
        .get("files", [])
    )

    count = 0
    for f in files:
        fid, fname = f["id"], f["name"]
        logging.info(f"🔄 Procesando archivo: {fname}")

        try:
            # 1. Mover a In Progress (Bloqueo lógico)
            move_file_in_drive(drive_service, fid, pending, progress)

            # 2. Descargar y Transformar
            fbytes, ftype, fname = download_drive_file_as_bytes(drive_service, fid)
            df = transform_dataframe(fbytes, ftype, fname, config, bank_name, acc_name)

            if not df.empty:
                # 3. Subir a GCS (Formato JSONL)
                json_data = df.to_json(orient="records", lines=True, date_format="iso")
                gcs_path = f"{bank_name}/{acc_name}/{Path(fname).stem}.jsonl"
                storage_client.bucket(GCS_BUCKET_NAME).blob(
                    gcs_path
                ).upload_from_string(json_data, "application/jsonl")
                logging.info(f"✅ Subido a GCS: {gcs_path}")

                # 4. Mover a Processed (Finalizado)
                move_file_in_drive(drive_service, fid, progress, processed)
                count += 1
            else:
                logging.warning(f"⚠️ Archivo vacío o datos inválidos: {fname}")
                # Devolver a PENDING para revisión manual
                move_file_in_drive(drive_service, fid, progress, pending)

        except Exception as e:
            logging.error(f"🔥 Error procesando {fname}: {e}")
            try:
                # Intentar devolver a PENDING si falla
                move_file_in_drive(drive_service, fid, progress, pending)
            except:
                pass

    return count


# --- MAIN ENTRY POINT ---
if __name__ == "__main__":
    print(
        """
    ========================================
       🚀 DWH FINANCIAL - INGESTION v2.0
    ========================================
    """
    )

    # Validación de entorno
    if not all([PROJECT_ID, GCS_BUCKET_NAME, DRIVE_PARENT_FOLDER_ID]):
        logging.error("❌ Faltan variables de entorno. Verifica tu archivo .env")
        exit(1)

    # Cargar configuración de mapeo de bancos
    configs = load_configs()
    if not configs:
        logging.error("❌ No hay configuraciones de bancos cargadas. Abortando.")
        exit(1)

    try:
        # Autenticación Google Drive
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            creds = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/cloud-platform",
                ],
            )
        else:
            import google.auth

            logging.info("🔑 Usando credenciales por defecto del sistema (ADC)")
            creds, _ = google.auth.default(
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/cloud-platform",
                ]
            )

        drive = build("drive", "v3", credentials=creds)

        # Iteración por carpetas de Bancos
        q_banks = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        banks = (
            drive.files()
            .list(q=q_banks, fields="files(id, name)")
            .execute()
            .get("files", [])
        )

        total_files = 0
        for b in banks:
            bname = b["name"]
            # Iteración por cuentas dentro del banco
            q_acc = f"'{b['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            accs = (
                drive.files()
                .list(q=q_acc, fields="files(id, name)")
                .execute()
                .get("files", [])
            )

            for acc in accs:
                aname = acc["name"]
                # Solo procesar si tenemos configuración para este banco/cuenta
                if bname in configs and aname in configs[bname]:
                    total_files += process_account_folder(
                        drive, acc, bname, configs[bname][aname]
                    )
                else:
                    logging.debug(f"ℹ️ Saltando carpeta no configurada: {bname}/{aname}")

        print(f"\n🎉 Proceso finalizado. Archivos procesados hoy: {total_files}")

    except Exception as e:
        logging.exception("🔥 Error crítico en el proceso de ingesta:")
