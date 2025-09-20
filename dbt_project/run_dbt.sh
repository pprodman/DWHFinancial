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
# Ejecutar dbt y capturar TODO en un archivo
dbt run --profiles-dir . --target prod --fail-fast --debug 2>&1 | tee logs/dbt_run.log
RUN_EXIT_CODE=${PIPESTATUS[0]}

if [ $RUN_EXIT_CODE -ne 0 ]; then
    echo "❌ dbt run failed with exit code $RUN_EXIT_CODE. Printing logs:"
    cat logs/dbt_run.log
    exit $RUN_EXIT_CODE
fi

echo "--- Running dbt tests... ---"
dbt test --profiles-dir . --target prod --debug 2>&1 | tee logs/dbt_test.log
TEST_EXIT_CODE=${PIPESTATUS[0]}

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "❌ dbt test failed with exit code $TEST_EXIT_CODE. Printing logs:"
    cat logs/dbt_test.log
    exit $TEST_EXIT_CODE
fi

echo "--- dbt run and test completed successfully! ---"