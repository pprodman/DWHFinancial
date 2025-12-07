# 🏦 DWH Financial - Local & GitHub Architecture

Pipeline ELT personal para finanzas. Ejecución local o vía GitHub Actions (Serverless).

## 🏗️ Arquitectura

1. Ingestión (Python): `ingestion/main.py`. Mueve Excels de Drive a GCS y genera CSVs de configuración desde Google Sheets.

2. Transformación (dbt Core): `transformation/`. Modelos SQL ejecutados por `dbt-bigquery`.

3. Orquestación: GitHub Actions (`.github/workflows/daily_pipeline.yml`).

## 🚀 Flujo de Trabajo

### A. Trabajo Diario (Automático)

El pipeline corre a las 06:00 AM UTC en GitHub Actions:

1. Ingesta nuevos archivos de Drive.

2. Actualiza reglas de categorización desde Google Sheets.

3. Ejecuta `dbt run` en BigQuery.

### B. Trabajo Manual (Local con VS Code)

Usamos `scripts/manage.ps1` como centro de mando.

1. Actualizar configuración (Si cambiaste el Excel de mapeo):
```
.\scripts\manage.ps1 update-seeds
git add .
git commit -m "update mapping"
git push
```

2. Probar ingesta manual:
```
.\scripts\manage.ps1 run-ingestion
```

3. Regenerar tablas dbt (Si cambiaste lógica SQL):
```
.\scripts\manage.ps1 dbt-refresh
```

## 🛠️ Configuración Local

### 1. Entorno:

- Python 3.11+

- Archivo .env en la raíz con:
```
GCP_PROJECT_ID=...
GCS_BUCKET_NAME=...
DRIVE_PARENT_FOLDER_ID=...
MAPPING_SHEET_ID=...
GOOGLE_APPLICATION_CREDENTIALS=C:\Ruta\Absoluta\a\keys\gcp_key.json
```

### 2. Instalación:
```
.\scripts\manage.ps1 install
```