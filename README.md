🏦 DWH Financial - Hybrid Architecture

Pipeline de datos personales para centralizar finanzas (Bancos, Tarjetas, Revolut) en Google BigQuery para su visualización en Looker Studio.

🏗️ Arquitectura Híbrida

Este proyecto utiliza un enfoque moderno Híbrido:

Ingestión (Local / Python): Scripts en Python que se ejecutan localmente (o en GitHub Actions) para mover datos de Google Drive a Google Cloud Storage.

Transformación (Cloud / dbt): Toda la lógica de negocio SQL se gestiona y ejecuta en dbt Cloud, conectado a BigQuery.

graph LR
    A[Google Drive\n(Excels)] -->|Python Script| B[Google Cloud Storage\n(JSONL)]
    B -->|BigQuery External Tables| C[BigQuery\n(Bronze)]
    C -->|dbt Cloud| D[BigQuery\n(Silver/Gold)]
    D -->|Conexión Directa| E[Looker Studio]


📂 Estructura del Proyecto

ingestion/: Código Python para la extracción y carga (EL).

config/: Mapeos de columnas para cada banco.

transformation/: Modelos SQL de dbt (Sincronizado con dbt Cloud).

scripts/: Utilidades para gestión local (PowerShell).

experiments/: (Ignorado por git) Notebooks para pruebas de datos sucios.

🚀 Cómo trabajar (Flujo Diario)

1. Ingesta de Datos (Local)

Coloca los archivos .xlsx en la carpeta PENDING de tu Google Drive.

# Desde VS Code
.\scripts\manage.ps1 run-ingestion


2. Desarrollo y Transformación (dbt Cloud)

Accede a dbt Cloud.

Desarrolla en la rama dev.

Ejecuta dbt run para actualizar tablas.

Haz Commit & Push en la web cuando termines.

3. Sincronizar Local (Opcional)

Si quieres tener una copia del código SQL actualizado en tu máquina:

git pull origin main


🛠️ Configuración Inicial

Python: Requiere Python 3.11+.

.\scripts\manage.ps1 install


Variables de Entorno (.env):
Crear un archivo .env en la raíz con:

GCP_PROJECT_ID=tu-proyecto-id
GCS_BUCKET_NAME=tu-bucket
DRIVE_PARENT_FOLDER_ID=tu-drive-folder-id
GOOGLE_APPLICATION_CREDENTIALS=./keys/gcp_key.json


📊 Stack Tecnológico

Cloud: Google Cloud Platform (BigQuery, Storage).

Lenguaje: Python 3.11.

Transformación: dbt Core (vía dbt Cloud).

BI: Looker Studio.