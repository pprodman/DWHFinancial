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

echo "--- Running dbt models... ---"
# Ejecuta dbt y redirige stderr a stdout para ver todos los errores
dbt run --profiles-dir . --target prod --fail-fast --debug 2>&1

echo "--- Running dbt tests... ---"
dbt test --profiles-dir . --target prod --debug 2>&1

echo "--- dbt run and test completed successfully! ---"