import os
import logging
import pandas as pd
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth import default
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
# Rutas relativas
BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_CSV = BASE_DIR / "transformation" / "seeds" / "master_mapping.csv"

# Cargar variables de entorno
load_dotenv(BASE_DIR / ".env")

# Configuración del Sheet
# El ID debe estar en tu .env como MAPPING_SHEET_ID
SHEET_ID = os.environ.get("MAPPING_SHEET_ID")
SHEET_NAME = os.environ.get(
    "MAPPING_SHEET_NAME"
)  # Nombre exacto de la pestaña en tu Google Sheet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S"
)


def get_credentials():
    """Obtiene credenciales de Service Account o ADC."""
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if creds_path and os.path.exists(creds_path):
        return service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
    else:
        # Fallback a Application Default Credentials (útil en Cloud Shell o si usas gcloud auth)
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
        logging.info(
            "ℹ️  Abre tu .env y añade: MAPPING_SHEET_ID=tu_id_largo_de_google_sheets"
        )
        exit(1)

    logging.info(f"🔄 Conectando a Google Sheets...")
    logging.info(f"📄 Sheet ID: {SHEET_ID[:10]}...")
    logging.info(f"📑 Pestaña: {SHEET_NAME}")

    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)

        # Llamada a la API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SHEET_ID, range=SHEET_NAME).execute()
        rows = result.get("values", [])

        if not rows:
            logging.warning("⚠️ La pestaña parece estar vacía.")
            return

        logging.info(f"📥 Descargadas {len(rows)} filas.")

        # Convertir a DataFrame para facilitar el manejo CSV
        # La primera fila son las cabeceras
        headers = rows[0]
        data = rows[1:]

        df = pd.DataFrame(data, columns=headers)

        # --- LIMPIEZA Y VALIDACIÓN ---

        # 1. Asegurar que 'priority' sea numérico (rellenar nulos con 50 por defecto)
        if "priority" in df.columns:
            df["priority"] = (
                pd.to_numeric(df["priority"], errors="coerce").fillna(50).astype(int)
            )

        # 2. Limpiar espacios en blanco en columnas clave
        if "keyword" in df.columns:
            df["keyword"] = df["keyword"].astype(str).str.strip()

        # 3. Guardar como CSV
        # Aseguramos que la carpeta seeds existe
        TARGET_CSV.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(TARGET_CSV, index=False, encoding="utf-8")

        logging.info(f"✅ Archivo guardado correctamente en:")
        logging.info(f"   {TARGET_CSV}")
        logging.info("🎉 Sincronización completada.")

    except Exception as e:
        logging.error(f"🔥 Error crítico durante la sincronización: {e}")
        # Tip común de error
        if "403" in str(e):
            logging.error(
                "💡 Pista: ¿Has compartido el Google Sheet con el email de tu Service Account?"
            )


if __name__ == "__main__":
    sync_seeds()
