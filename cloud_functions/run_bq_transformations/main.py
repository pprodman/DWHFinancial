import os
import logging
import subprocess

logging.basicConfig(level=logging.INFO)

# --- Variables de Entorno ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
if not PROJECT_ID:
    # Manejo del error si la variable no está definida
    logging.critical("Environment variable GCP_PROJECT is not set.")
    raise ValueError("GCP_PROJECT environment variable is required.")

BQ_DATASET_DEV = 'dbt_dev' # Asegúrate de que este dataset exista
DBT_PROFILES_DIR_NAME = "dbt_profiles_tmp" # Nombre de la carpeta para perfiles, dentro de dbt_project
DBT_PROJECT_DIR_RELATIVE = "dbt_project"  # Ruta relativa desde /workspace

def main(event, context):
    """
    Punto de entrada de la Cloud Run Function para ejecutar dbt.
    """
    logging.info("Starting scheduled dbt run job.")

    # --- Verificaciones de estructura ---
    # Verificar que el directorio del proyecto existe
    if not os.path.isdir(DBT_PROJECT_DIR_RELATIVE):
        logging.critical(f"DBT project directory '{DBT_PROJECT_DIR_RELATIVE}' not found in /workspace. Check deployment.")
        return "Error: DBT project directory missing", 500

    # Verificar que dbt_project.yml existe dentro del directorio del proyecto
    dbt_project_file = os.path.join(DBT_PROJECT_DIR_RELATIVE, "dbt_project.yml")
    if not os.path.isfile(dbt_project_file):
        logging.critical(f"dbt_project.yml not found at '{dbt_project_file}'. Check deployment.")
        return "Error: dbt_project.yml missing", 500

    # Ruta completa al directorio del proyecto
    full_dbt_project_path = os.path.abspath(DBT_PROJECT_DIR_RELATIVE)
    logging.info(f"Using DBT project directory: {full_dbt_project_path}")

    # --- Configuración de Profiles ---
    # Crea el directorio para el perfil de dbt dentro del proyecto
    profiles_path = os.path.join(full_dbt_project_path, DBT_PROFILES_DIR_NAME)
    os.makedirs(profiles_path, exist_ok=True)
    logging.info(f"Created/verified profiles directory: {profiles_path}")

    # Crea el archivo profiles.yml dinámicamente
    # Asegúrate de que el nombre del perfil en profiles.yml coincida con el de dbt_project.yml
    profiles_content = f"""
config:
  send_anonymous_usage_stats: False

dwhfinancial_profile:  # Este nombre debe coincidir con el profile: en tu dbt_project.yml
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{PROJECT_ID}"
      dataset: "{BQ_DATASET_DEV}"
      threads: 4
      timeout_seconds: 300
      # keyfile o keyfile_json se obtienen del service account asociado al Cloud Run
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
        # Comando dbt. No se necesita --project-dir porque el cwd será el directorio del proyecto.
        command = [
            "dbt",
            "run",
            "--profiles-dir", DBT_PROFILES_DIR_NAME, # Ruta relativa desde el cwd (full_dbt_project_path)
            # Puedes añadir más argumentos aquí si los necesitas, por ejemplo:
            # "--target", "dev",
            # "--full-refresh" # (Descomenta si necesitas refresco completo)
        ]

        logging.info(f"Executing dbt command from '{full_dbt_project_path}': {' '.join(command)}")

        # Ejecutar el comando, cambiando el directorio de trabajo (cwd) al del proyecto dbt
        process = subprocess.run(
            command,
            cwd=full_dbt_project_path, # <--- Cambio clave
            check=True, # Lanza CalledProcessError si el comando falla (exit code != 0)
            capture_output=True,
            text=True,
            timeout=600 # Timeout en segundos (ajusta según la duración esperada)
        )

        logging.info("dbt run completed successfully.")
        if process.stdout:
            logging.info("dbt stdout:\n" + process.stdout)
        # dbt a veces imprime información en stderr incluso si tiene éxito
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
        # Intentar obtener salida parcial si está disponible
        if e.stdout:
            logging.info("Partial dbt stdout before timeout:\n" + e.stdout.decode() if isinstance(e.stdout, bytes) else e.stdout)
        if e.stderr:
            logging.info("Partial dbt stderr before timeout:\n" + e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr)
        return "Error: dbt command timed out", 500

    except Exception as e:
        logging.critical(f"An unexpected error occurred during dbt execution: {e}", exc_info=True) # exc_info=True para stack trace
        return "Error: Unexpected failure", 500
