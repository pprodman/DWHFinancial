import os
import logging
import subprocess

logging.basicConfig(level=logging.INFO)

# --- Variables de Entorno ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
BQ_DATASET_DEV = 'dbt_dev' # O el nombre que prefieras para tu dataset de desarrollo/producción
DBT_PROFILES_DIR = "dbt_profiles"
DBT_PROJECT_DIR = "dbt_project" # Nuevo: La carpeta de tu proyecto dbt

def main(event, context):
    """
    Punto de entrada de la Cloud Function para ejecutar dbt.
    Esta función asume que el proyecto dbt está empaquetado y subido con la función.
    """
    logging.info("Starting scheduled dbt run job.")

    # Crea el directorio para el perfil de dbt si no existe
    # El path es relativo al directorio de trabajo actual de la función
    profiles_path = os.path.join(DBT_PROJECT_DIR, DBT_PROFILES_DIR)
    os.makedirs(profiles_path, exist_ok=True)

    # Crea el archivo profiles.yml dinámicamente
    profiles_content = f"""
dwhfinancial_profile:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account-json
      project: {PROJECT_ID}
      dataset: {BQ_DATASET_DEV}
      threads: 4
      timeout_seconds: 300
"""
    
    with open(os.path.join(profiles_path, "profiles.yml"), "w") as f:
        f.write(profiles_content)
    
    # Invocación del comando dbt
    try:
        # Usa subprocess para llamar al comando dbt CLI
        # El comando se ejecuta desde el directorio raíz de la función
        # Por eso, necesitamos especificar la ruta completa al proyecto dbt y al perfil.
        command = [
            "dbt",
            "--project-dir", DBT_PROJECT_DIR,  # Apunta al directorio del proyecto dbt
            "--profiles-dir", profiles_path,    # Apunta al directorio del perfil
            "run"
        ]
        
        logging.info(f"Executing dbt command: {' '.join(command)}")
        
        # Ejecutar el comando y capturar la salida
        process = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        
        logging.info("dbt run completed successfully.")
        logging.info("dbt stdout:\n" + process.stdout)
        
        return "OK", 200

    except subprocess.CalledProcessError as e:
        logging.critical(f"dbt command failed with exit code {e.returncode}")
        logging.critical("dbt stderr:\n" + e.stderr)
        logging.critical("dbt stdout:\n" + e.stdout)
        return "Error", 500
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
        return "Error", 500
