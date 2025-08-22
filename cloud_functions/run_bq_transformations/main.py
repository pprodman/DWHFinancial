import os
import logging
import subprocess

logging.basicConfig(level=logging.INFO)

# --- Variables de Entorno (definidas como constantes globales o se obtienen dentro de main) ---
BQ_DATASET_RAW = 'dwh_01_raw'
DBT_PROFILES_DIR_NAME = "dbt_profiles_tmp"
DBT_PROJECT_DIR_RELATIVE = "dbt_project"

def main(event, context):
    """
    Punto de entrada de la Cloud Function para ejecutar dbt.
    """
    logging.info("Starting scheduled dbt run job.")

    # --- Obtener y verificar variables de entorno dentro de main ---
    PROJECT_ID = os.environ.get("GCP_PROJECT")
    if not PROJECT_ID:
        logging.critical("Environment variable GCP_PROJECT is not set.")
        # Este return está correctamente dentro de la función main
        return "Error: GCP_PROJECT not set", 500

    # --- Verificaciones de estructura ---
    if not os.path.isdir(DBT_PROJECT_DIR_RELATIVE):
        logging.critical(f"DBT project directory '{DBT_PROJECT_DIR_RELATIVE}' not found in the deployment package.")
        return "Error: DBT project directory missing", 500

    dbt_project_file = os.path.join(DBT_PROJECT_DIR_RELATIVE, "dbt_project.yml")
    if not os.path.isfile(dbt_project_file):
        logging.critical(f"dbt_project.yml not found at '{dbt_project_file}'. Check deployment package.")
        return "Error: dbt_project.yml missing", 500

    # --- Configuración de Profiles ---
    # Ruta completa al directorio del proyecto
    full_dbt_project_path = os.path.abspath(DBT_PROJECT_DIR_RELATIVE)
    logging.info(f"Using DBT project directory: {full_dbt_project_path}")

    # Crea el directorio para el perfil de dbt dentro del proyecto
    profiles_path = os.path.join(full_dbt_project_path, DBT_PROFILES_DIR_NAME)
    os.makedirs(profiles_path, exist_ok=True)
    logging.info(f"Created/verified profiles directory: {profiles_path}")

    # Crea el archivo profiles.yml dinámicamente
    # Asegúrate de que el nombre del perfil en profiles.yml coincida con el de dbt_project.yml
    profiles_content = f"""
config:
  send_anonymous_usage_stats: False

dwhfinancial_profile:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{PROJECT_ID}"
      dataset: "{BQ_DATASET_RAW}"
      threads: 4
      timeout_seconds: 300
"""

    profile_file_path = os.path.join(profiles_path, "profiles.yml")
    try:
        with open(profile_file_path, "w") as f:
            f.write(profiles_content)
        logging.info(f"Created dbt profiles.yml at {profile_file_path}")
    except Exception as e:
        logging.critical(f"Failed to create profiles.yml: {e}")
        return "Error: Failed to create dbt profile", 500

    # --- Ejecución de dbt ---
    try:
        # 1. Ejecutar el comando de compilación primero
        logging.info("Executing dbt compile command.")
        compile_command = [
            "dbt",
            "compile",
            "--profiles-dir", DBT_PROFILES_DIR_NAME,
        ]

        compile_process = subprocess.run(
            compile_command,
            cwd=full_dbt_project_path,
            check=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        logging.info("dbt compile completed successfully.")
        if compile_process.stdout:
            logging.info("dbt compile stdout:\n" + compile_process.stdout)

        # 2. Ejecutar el comando de ejecución
        logging.info("Executing dbt run command.")
        run_command = [
            "dbt",
            "run",
            "--profiles-dir", DBT_PROFILES_DIR_NAME,
        ]

        process = subprocess.run(
            run_command,
            cwd=full_dbt_project_path,
            check=True,
            capture_output=True,
            text=True,
            timeout=600
        )

        logging.info("dbt run completed successfully.")
        if process.stdout:
            logging.info("dbt stdout:\n" + process.stdout)
        if process.stderr:
            logging.debug("dbt stderr (might contain warnings/logs):\n" + process.stderr)

        return "OK: dbt run finished", 200

    except subprocess.CalledProcessError as e:
        logging.critical(f"dbt command failed with exit code {e.returncode}")
        if e.stdout:
            logging.critical("dbt stdout:\n" + e.stdout)
        if e.stderr:
            logging.critical("dbt stderr:\n" + e.stderr)
        else:
            logging.critical("No stderr captured from dbt process.")
        return f"Error: dbt command failed (exit code {e.returncode})", 500

    except subprocess.TimeoutExpired as e:
        logging.critical(f"dbt command timed out after {e.timeout} seconds.")
        if e.stdout:
            logging.info("Partial dbt stdout before timeout:\n" + e.stdout.decode() if isinstance(e.stdout, bytes) else e.stdout)
        if e.stderr:
            logging.info("Partial dbt stderr before timeout:\n" + e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr)
        return "Error: dbt command timed out", 500

    except Exception as e:
        logging.critical(f"An unexpected error occurred during dbt execution: {e}", exc_info=True)
        return "Error: Unexpected failure", 500