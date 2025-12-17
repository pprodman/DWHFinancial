import os
import logging
import pandas as pd
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth import default
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
SEEDS_DIR = BASE_DIR / "transformation" / "seeds"

load_dotenv(BASE_DIR / ".env")

# ID de la hoja de cálculo
SPREADSHEET_ID = os.environ.get("MAPPING_SHEET_ID")

# LISTA DE PESTAÑAS A SINCRONIZAR
SHEETS_TO_SYNC = [
    {"env_var": "MAPPING_SHEET_NAME", "file_name": "master_mapping.csv"},
    {"env_var": "BIZUM_SHEET_NAME", "file_name": "bizum_directory.csv"},
    {"env_var": "ADJUSTMENTS_SHEET_NAME", "file_name": "adjustments.csv"},
]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S"
)


def get_credentials():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        return service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
    else:
        creds, _ = default(
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        return creds


def download_and_save_sheet(service, spreadsheet_id, sheet_name, target_file):
    """
    Función auxiliar que descarga una pestaña y la guarda como CSV.
    Garantiza que celdas vacías sean NULL reales en el CSV.
    """

    logging.info(f"🔹 Procesando pestaña: '{sheet_name}' -> {target_file.name}")

    range_name = f"'{sheet_name}'!A:Z"

    try:
        # Descargar datos
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        rows = result.get("values", [])

        if not rows:
            logging.warning(f"⚠️ La pestaña '{sheet_name}' existe pero está vacía.")
            return

        # Normalización de columnas
        headers = rows[0]
        expected_cols = len(headers)
        raw_data = rows[1:]

        normalized_data = []
        for row in raw_data:
            if len(row) < expected_cols:
                # Rellenar con None de Python
                row = row + [None] * (expected_cols - len(row))
            elif len(row) > expected_cols:
                row = row[:expected_cols]
            normalized_data.append(row)

        df = pd.DataFrame(normalized_data, columns=headers)

        # --- 1. PROCESAMIENTO ESPECÍFICO (Primero tipos y limpieza de texto) ---

        # Priority: Convertir a número
        if "priority" in df.columns:
            df["priority"] = (
                pd.to_numeric(df["priority"], errors="coerce").fillna(50).astype(int)
            )

        # Keyword: Convertir a string para quitar espacios, PERO no rellenar con "" todavía
        if "keyword" in df.columns:
            # Convertimos a string, tratando 'nan' y 'None' como vacíos para evitar la palabra literal "None"
            df["keyword"] = df["keyword"].astype(str).replace({"nan": "", "None": ""})
            df["keyword"] = df["keyword"].str.strip()

        # Clean Name: Igual, asegurar limpieza de espacios
        if "clean_name" in df.columns:
            df["clean_name"] = (
                df["clean_name"].astype(str).replace({"nan": "", "None": ""})
            )
            df["clean_name"] = df["clean_name"].str.strip()

        # --- 2. LIMPIEZA GLOBAL DE NULLS (Al final para machacar todo) ---
        # Esto asegura el "Verdadero NULL":
        # - Cadenas vacías "" -> None
        # - Espacios "   " -> None
        # - Literales "None" o "nan" -> None

        # Regex para celdas vacías o con solo espacios
        df = df.replace(r"^\s*$", None, regex=True)
        # Reemplazo explícito de literales de texto por si acaso
        df = df.replace(["None", "nan", "NaN"], None)

        # --- 3. GUARDADO ---
        # na_rep='' hace que los None se escriban como ,, en el CSV (NULL para BigQuery)
        target_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(target_file, index=False, encoding="utf-8", na_rep="")

        logging.info(f"✅ Guardado correctamente: {target_file.name}")

    except Exception as e:
        logging.error(f"❌ Error procesando '{sheet_name}': {e}")


def sync_seeds():
    print(
        f"""
    ========================================
        🌱 DWH FINANCIAL - SEED SYNC MULTI
    ========================================
    """
    )

    if not SPREADSHEET_ID:
        logging.error("❌ Error: Falta la variable de entorno MAPPING_SHEET_ID en .env")
        exit(1)

    logging.info(f"🔄 Conectando a Google Sheets...")

    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
        spreadsheet = service.spreadsheets()

        # 1. Obtener lista de pestañas REALES en el documento
        meta = spreadsheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = meta.get("sheets", [])
        real_sheet_names = [s["properties"]["title"] for s in sheets]

        logging.info(f"🔎 Pestañas disponibles en el Excel: {real_sheet_names}")

        # 2. Iterar sobre la configuración de hojas definida arriba
        for config in SHEETS_TO_SYNC:
            sheet_name = os.environ.get(config["env_var"])
            target_path = SEEDS_DIR / config["file_name"]

            if sheet_name not in real_sheet_names:
                logging.warning(f"⚠️ La pestaña '{sheet_name}' NO existe.")
                continue

            download_and_save_sheet(service, SPREADSHEET_ID, sheet_name, target_path)

        print("\n✨ Sincronización completada con éxito.\n")

    except Exception as e:
        logging.error(f"🔥 Error crítico de conexión: {e}")


if __name__ == "__main__":
    sync_seeds()
