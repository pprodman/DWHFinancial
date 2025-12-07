import os
import logging
import pandas as pd
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth import default
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_CSV = BASE_DIR / "transformation" / "seeds" / "master_mapping.csv"

load_dotenv(BASE_DIR / ".env")

SHEET_ID = os.environ.get("MAPPING_SHEET_ID")
SHEET_NAME = os.environ.get("MAPPING_SHEET_NAME")

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
        logging.info("⚠️ Usando Application Default Credentials (ADC)")
        creds, _ = default(
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        return creds


def sync_seeds():
    print(
        f"""
    ========================================
       🌱 DWH FINANCIAL - SEED SYNC
    ========================================
    """
    )

    if not SHEET_ID:
        logging.error(
            "❌ Error: No se encontró la variable MAPPING_SHEET_ID en el archivo .env"
        )
        exit(1)

    logging.info(f"🔄 Conectando a Google Sheets...")

    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)

        range_name = f"'{SHEET_NAME}'!A:Z"

        logging.info(f"📄 Sheet ID: {SHEET_ID[:5]}...")
        logging.info(f"📑 Leyendo rango: {range_name}")

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SHEET_ID, range=range_name).execute()
        rows = result.get("values", [])

        if not rows:
            logging.warning("⚠️ La pestaña parece estar vacía o el rango es incorrecto.")
            return

        logging.info(f"📥 Descargadas {len(rows)} filas.")

        headers = rows[0]
        expected_cols = len(headers)
        raw_data = rows[1:]

        normalized_data = []
        for row in raw_data:
            if len(row) < expected_cols:
                row = row + [""] * (expected_cols - len(row))
            elif len(row) > expected_cols:
                row = row[:expected_cols]
            normalized_data.append(row)

        df = pd.DataFrame(normalized_data, columns=headers)

        # Limpieza de datos
        if "priority" in df.columns:
            df["priority"] = (
                pd.to_numeric(df["priority"], errors="coerce").fillna(50).astype(int)
            )

        if "keyword" in df.columns:
            df["keyword"] = df["keyword"].astype(str).str.strip()

        # Guardar
        TARGET_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(TARGET_CSV, index=False, encoding="utf-8")

        logging.info(f"✅ Archivo guardado en: {TARGET_CSV}")
        logging.info("🎉 Sincronización completada.")

    except Exception as e:
        logging.error(f"🔥 Error crítico: {e}")


if __name__ == "__main__":
    sync_seeds()
