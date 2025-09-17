#!/bin/sh

set -e
set -x

echo "=== DEBUG: Current working directory ==="
pwd

echo "=== DEBUG: Listing all files in current directory ==="
ls -la

echo "=== DEBUG: Checking if profiles.yml exists ==="
if [ -f "profiles.yml" ]; then
    echo "✅ profiles.yml FOUND!"
    echo "=== Content of profiles.yml ==="
    cat profiles.yml
else
    echo "❌ ERROR: profiles.yml NOT FOUND in current directory!"
    exit 1
fi

echo "=== DEBUG: Checking if dbt_project.yml exists ==="
if [ -f "dbt_project.yml" ]; then
    echo "✅ dbt_project.yml FOUND!"
    echo "=== Content of dbt_project.yml ==="
    cat dbt_project.yml
else
    echo "❌ ERROR: dbt_project.yml NOT FOUND!"
    exit 1
fi

echo "--- Running dbt models... ---"
dbt run --profiles-dir . --target prod --fail-fast

echo "--- Running dbt tests... ---"
dbt test --profiles-dir . --target prod

echo "--- dbt run and test completed successfully! ---"