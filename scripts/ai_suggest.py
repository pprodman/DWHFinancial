import os
import pandas as pd
import google.generativeai as genai  # Librería gratuita
from google.cloud import bigquery
from pathlib import Path
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Credenciales para BigQuery (Sigue usando Service Account)
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Credenciales para IA (Nueva API Key Gratuita)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Rutas
SEEDS_DIR = BASE_DIR / "transformation" / "seeds"
MAPPING_FILE = SEEDS_DIR / "master_mapping.csv"
OUTPUT_FILE = BASE_DIR / "suggested_mappings.csv"


def configure_ai():
    if not GEMINI_API_KEY:
        print("❌ Error: Falta la variable GEMINI_API_KEY en el archivo .env")
        print("ℹ️  Consíguela gratis en: https://aistudio.google.com/app/apikey")
        return False

    genai.configure(api_key=GEMINI_API_KEY)
    return True


def get_uncategorized_concepts():
    """Consulta BigQuery para obtener conceptos que la macro no pudo clasificar."""
    client = bigquery.Client()

    # Buscamos conceptos donde el grupo sea 'Sin Clasificar'
    # Ajusta esta query según cómo se llame tu categoría por defecto en la macro
    query = """
    SELECT DISTINCT concepto
    FROM `dwhfinancial.silver.fct_transactions`
    WHERE grupo = 'Gastos Variables'
      AND categoria = 'Otros Gastos'
      AND subcategoria = 'Sin Clasificar'
    LIMIT 30
    """

    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            return []
        return df["concepto"].tolist()
    except Exception as e:
        print(f"⚠️ Error conectando a BigQuery: {e}")
        return []


def get_categories_context():
    """Lee el mapping actual para enseñarle a la IA tu estructura."""
    if not MAPPING_FILE.exists():
        return "No hay contexto previo."

    try:
        df = pd.read_csv(MAPPING_FILE)
        # Creamos una lista compacta de ejemplos únicos
        structure = (
            df[["grupo_categoria", "categoria", "subcategoria"]]
            .drop_duplicates()
            .to_string(index=False)
        )
        return structure
    except:
        return "Estructura estándar."


def generate_suggestions(concepts, context_structure):
    """Usa Gemini Pro (Versión Gratuita) para clasificar."""

    # Configuración del modelo gratuito
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = f"""
    Eres un experto Data Engineer. Clasifica estos movimientos bancarios siguiendo MI estructura existente.

    MIS CATEGORÍAS VÁLIDAS:
    {context_structure}

    INSTRUCCIONES:
    Para cada concepto de la lista, genera una línea CSV con este formato exacto:
    keyword,priority,grupo_categoria,categoria,subcategoria,entity_name

    REGLAS:
    1. keyword: La parte clave del concepto (MAYÚSCULAS).
    2. priority: Siempre 50.
    3. entity_name: Nombre limpio de la empresa (Title Case).
    4. Usa solo combinaciones válidas de mi lista. Si es duda, usa 'Gastos Variables,Otros,Varios'.
    5. NO uses bloques de código markdown (```). Solo devuelve el texto CSV.

    CONCEPTOS A CLASIFICAR:
    {", ".join(concepts)}
    """

    try:
        response = model.generate_content(prompt)
        # Limpieza de la respuesta por si la IA pone formato markdown
        clean_text = response.text.replace("```csv", "").replace("```", "").strip()
        return clean_text
    except Exception as e:
        print(f"🔥 Error en la llamada a la IA: {e}")
        return None


if __name__ == "__main__":
    print("--- 🧠 DWH Financial AI Assistant (Free Tier) ---")

    if not configure_ai():
        exit(1)

    # 1. Obtener conceptos
    print("🔍 Buscando transacciones sin clasificar en BigQuery...")
    concepts = get_uncategorized_concepts()

    if not concepts:
        print("✅ ¡Genial! No hay transacciones pendientes de clasificar.")
        exit()

    print(f"📝 Encontrados {len(concepts)} conceptos.")

    # 2. Obtener contexto
    context = get_categories_context()

    # 3. Llamar a la IA
    csv_content = generate_suggestions(concepts, context)

    if csv_content:
        # Guardar resultados
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            # Escribimos cabecera y contenido
            f.write(
                "keyword,priority,grupo_categoria,categoria,subcategoria,entity_name\n"
            )
            f.write(csv_content)

        print(f"\n✅ Sugerencias generadas en: {OUTPUT_FILE}")
        print(
            "👉 Abre el archivo, revisa y copia las líneas válidas a tu Google Sheet."
        )
