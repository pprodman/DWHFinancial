#!/bin/sh

# Salir inmediatamente si un comando falla
set -e

# Imprimir los comandos que se ejecutan
set -x

# 1. Ejecutar los modelos de dbt
echo "--- Running dbt models... ---"
dbt run --profiles-dir . --fail-fast

# 2. Ejecutar los tests de dbt
echo "--- Running dbt tests... ---"
dbt test --profiles-dir .

echo "--- dbt run and test completed successfully! ---"