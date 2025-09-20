#!/bin/bash

# Salir inmediatamente si un comando falla
set -e
# Imprimir cada comando antes de ejecutarlo (excelente para depuración)
set -x

# --- DIRECTORIOS DE SALIDA ---
# Centralizar la ubicación de los logs y el target de dbt
# Esto es útil si quieres subir estos artefactos a GCS después.
LOG_DIR="logs"
DBT_TARGET_PATH="target"
mkdir -p "$LOG_DIR" "$DBT_TARGET_PATH"

# --- PASOS DE DEPURACIÓN (YA ESTÁN BIEN) ---
echo "=== DEBUG: Current user and environment ==="
whoami
env | sort

echo "=== DEBUG: Current directory ==="
pwd

echo "=== DEBUG: Files in directory ==="
ls -la

# --- EJECUCIÓN DE DBT ---
echo "--- Running dbt models... ---"
# Redirigir la salida de depuración (stderr) al log junto con la salida estándar (stdout)
dbt run --profiles-dir . --target prod --fail-fast --debug --target-path "$DBT_TARGET_PATH" 2>&1 | tee "$LOG_DIR/dbt_run.log"
RUN_EXIT_CODE=${PIPESTATUS[0]} # Capturar el código de salida de dbt, no de tee

if [ $RUN_EXIT_CODE -ne 0 ]; then
    echo "❌ dbt run failed with exit code $RUN_EXIT_CODE. Review logs above or in the log file."
    # No es necesario imprimir el log de nuevo, ya que `tee` lo mostró en la salida estándar
    exit $RUN_EXIT_CODE
fi

echo "--- Running dbt tests... ---"
dbt test --profiles-dir . --target prod --debug --target-path "$DBT_TARGET_PATH" 2>&1 | tee "$LOG_DIR/dbt_test.log"
TEST_EXIT_CODE=${PIPESTATUS[0]}

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "❌ dbt test failed with exit code $TEST_EXIT_CODE. Review logs above or in the log file."
    exit $TEST_EXIT_CODE
fi

# --- (Opcional) Subir artefactos a GCS ---
# Si configuras una cuenta de servicio con permisos para GCS, puedes descomentar esto.
# echo "--- Uploading dbt artifacts to GCS ---"
# gsutil cp -r "$DBT_TARGET_PATH" "gs://tu-bucket-de-artefactos/dbt-transform-job/$(date +%Y-%m-%d)/"
# gsutil cp -r "$LOG_DIR" "gs://tu-bucket-de-artefactos/dbt-transform-job/$(date +%Y-%m-%d)/"

echo "✅ dbt run and test completed successfully!"