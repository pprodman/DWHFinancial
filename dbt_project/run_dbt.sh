#!/bin/sh

set -e
set -x

echo "=== DEBUG: Current directory ==="
pwd

echo "=== DEBUG: Files in directory ==="
ls -la

echo "=== DEBUG: profiles.yml content ==="
cat profiles.yml

echo "=== DEBUG: dbt_project.yml content ==="
cat dbt_project.yml

echo "=== DEBUG: Creating logs directory ==="
mkdir -p logs

echo "--- Running dbt models... ---"
# Ejecutar dbt y capturar TODO en un archivo, incluso si falla
if ! dbt run --profiles-dir . --target prod --fail-fast --debug 2>&1 | tee logs/dbt_run.log; then
    echo "❌ dbt run failed. Printing full logs for debugging:"
    cat logs/dbt_run.log
    exit 1
fi

echo "--- Running dbt tests... ---"
if ! dbt test --profiles-dir . --target prod --debug 2>&1 | tee logs/dbt_test.log; then
    echo "❌ dbt test failed. Printing full logs for debugging:"
    cat logs/dbt_test.log
    exit 1
fi

echo "--- dbt run and test completed successfully! ---"