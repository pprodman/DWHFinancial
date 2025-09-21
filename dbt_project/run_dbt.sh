#!/bin/bash

# Script de ejecución de dbt para producción
# Falla inmediatamente si cualquier comando falla (set -e)
# Falla si una variable no está definida (set -u)
# Falla si un comando en una tubería falla (set -o pipefail)
set -euo pipefail

# --- Directorios de Salida ---
LOG_DIR="logs"
DBT_TARGET_PATH="target"
mkdir -p "$LOG_DIR" "$DBT_TARGET_PATH"

echo "--- Iniciando dbt pipeline... ---"

# --- Paso 1: Ejecutar los modelos de dbt ---
echo "--- Ejecutando dbt run... ---"
# Se ejecuta dbt run, y toda la salida (stdout y stderr) se guarda en un log
# y se muestra en la consola al mismo tiempo gracias a 'tee'.
dbt run --profiles-dir . --target prod --fail-fast --target-path "$DBT_TARGET_PATH" 2>&1 | tee "$LOG_DIR/dbt_run.log"
# Se captura el código de salida de 'dbt', no de 'tee'.
RUN_EXIT_CODE=${PIPESTATUS[0]}

# Se verifica si el comando dbt run falló.
if [ $RUN_EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: dbt run falló con el código de salida $RUN_EXIT_CODE. Revisa los logs de arriba."
    # El script termina aquí con el código de error.
    exit $RUN_EXIT_CODE
fi

echo "✅ dbt run completado con éxito."

# --- Paso 2: Ejecutar los tests de calidad de datos ---
echo "--- Ejecutando dbt test... ---"
dbt test --profiles-dir . --target prod --target-path "$DBT_TARGET_PATH" 2>&1 | tee "$LOG_DIR/dbt_test.log"
TEST_EXIT_CODE=${PIPESTATUS[0]}

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: dbt test falló con el código de salida $TEST_EXIT_CODE. Revisa los logs de arriba."
    exit $TEST_EXIT_CODE
fi

echo "✅ dbt test completado con éxito."
echo "🎉 ¡Pipeline de dbt finalizado exitosamente! 🎉"