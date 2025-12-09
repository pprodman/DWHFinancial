import os
import pandas as pd
import google.generativeai as genai
from google.cloud import bigquery
from pathlib import Path
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Credenciales
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# Para BigQuery usamos las credenciales de servicio
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

# Archivos
SEEDS_DIR = BASE_DIR / "transformation" / "seeds"
MAPPING_FILE = SEEDS_DIR / "master_mapping.csv"
OUTPUT_FILE = BASE_DIR / "suggested_mappings.csv"


def configure_ai():
    if not GEMINI_API_KEY:
        print("❌ Error: Falta GEMINI_API_KEY en el .env")
        return False

    genai.configure(api_key=GEMINI_API_KEY)
    return True


def get_uncategorized_concepts():
    """Consulta BigQuery para obtener conceptos sin clasificar."""
    client = bigquery.Client()

    # Ajusta esta query según tus datos reales en Silver
    query = """
    SELECT DISTINCT concepto
    FROM `dwhfinancial.silver.fct_transactions`
    WHERE (grupo IS NULL OR grupo = 'Sin Clasificar' OR grupo = 'Gastos Variables')
      AND (categoria IS NULL OR categoria = 'Otros Gastos')
      AND (subcategoria IS NULL OR subcategoria = 'Sin Clasificar')
    LIMIT 20
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
    """Lee el mapping actual para dar contexto a la IA."""
    if not MAPPING_FILE.exists():
        return "Estructura estándar de finanzas personales."

    try:
        df = pd.read_csv(MAPPING_FILE)
        structure = (
            df[["grupo_categoria", "categoria", "subcategoria"]]
            .drop_duplicates()
            .to_string(index=False)
        )
        return structure
    except:
        return "Estructura estándar."


def try_generate(model_name, prompt):
    """Intenta generar contenido con un modelo específico."""
    print(f"🤖 Probando con modelo: {model_name}...")
    model = genai.GenerativeModel(model_name)
    response = model.generate_content(prompt)
    return response.text


def generate_suggestions(concepts, context_structure):

    prompt = f"""
    Eres un experto Data Engineer. Clasifica estos movimientos bancarios siguiendo MI estructura.

    MIS CATEGORÍAS VÁLIDAS:
    {context_structure}

    INSTRUCCIONES:
    Genera un CSV (sin markdown) con: keyword,priority,grupo_categoria,categoria,subcategoria,entity_name

    REGLAS:
    1. keyword: Texto clave del concepto en MAYÚSCULAS.
    2. priority: 50.
    3. entity_name: Nombre limpio (Title Case).
    4. Usa solo mi estructura. Si dudas: 'Gastos Variables,Otros,Varios'.

    CONCEPTOS:
    {", ".join(concepts)}
    """

    # --- CAMBIO IMPORTANTE: Lista de modelos basada en tu log ---
    models_to_try = [
        "gemini-2.0-flash",  # Tu modelo más potente disponible
        "gemini-flash-latest",  # El alias seguro
        "gemini-pro-latest",  # Fallback a Pro
    ]

    for model_name in models_to_try:
        try:
            return try_generate(model_name, prompt)
        except Exception as e:
            print(f"⚠️ Falló {model_name}: {e}")

    print("\n❌ TODOS LOS MODELOS FALLARON.")
    return None


if __name__ == "__main__":
    print("--- 🧠 DWH Financial AI Assistant ---")

    if not configure_ai():
        exit(1)

    print("🔍 Buscando transacciones...")
    concepts = get_uncategorized_concepts()

    if not concepts:
        print("✅ No hay nada pendiente de clasificar.")
        exit()

    print(f"📝 Encontrados {len(concepts)} conceptos.")
    context = get_categories_context()

    raw_response = generate_suggestions(concepts, context)

    if raw_response:
        # Limpieza de bloques de código markdown
        clean_csv = raw_response.replace("```csv", "").replace("```", "").strip()

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(
                "keyword,priority,grupo_categoria,categoria,subcategoria,entity_name\n"
            )
            f.write(clean_csv)

        print(f"\n✅ Sugerencias generadas en: {OUTPUT_FILE}")
        print(
            "👉 Abre el archivo, revisa las sugerencias y copia las filas válidas a tu Google Sheet 'dbt - mapping'."
        )
    else:
        print("\n🔥 No se pudo generar nada.")
