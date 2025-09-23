#!/bin/bash

set -euo pipefail

LOG_DIR="logs"
DBT_TARGET_PATH="target"
mkdir -p "$LOG_DIR" "$DBT_TARGET_PATH"

echo "--- Iniciando dbt pipeline con 'dbt build'... ---"

# --- Ejecutar el pipeline completo con dbt build ---
# dbt build ejecuta seed, run y test en el orden correcto.
# El flag --fail-fast asegura que se detenga al primer error.
dbt build --profiles-dir . --target prod --fail-fast --target-path "$DBT_TARGET_PATH" 2>&1 | tee "$LOG_DIR/dbt_build.log"
BUILD_EXIT_CODE=${PIPESTATUS[0]}

if [ $BUILD_EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: dbt build falló con el código de salida $BUILD_EXIT_CODE. Revisa los logs de arriba."
    exit $BUILD_EXIT_CODE
fi

echo "✅ dbt build completado con éxito."
echo "🎉 ¡Pipeline de dbt finalizado exitosamente! 🎉"