# Pipeline de Datos de Cuentas Bancarias en GCP

Este proyecto implementa un pipeline de datos para ingestar, procesar y almacenar movimientos de cuentas bancarias personales usando Google Cloud Platform.

## Arquitectura

1.  **Ingesta**: Una Cloud Function en Python se activa periódicamente por Cloud Scheduler.
2.  **Procesamiento**: La función lee archivos XLSX desde una estructura de carpetas en Google Drive, los limpia y estandariza usando Pandas y una configuración JSON.
3.  **Almacenamiento Crudo (Data Lake)**: Los datos limpios se guardan como archivos CSV en Google Cloud Storage.
4.  **Transformación**: Un Job de Cloud Run con dbt Core se activa para transformar los datos CSV y cargarlos en BigQuery.
5.  **Almacenamiento Estructurado (Data Warehouse)**: Los datos finales se materializan en una tabla en BigQuery.

## Cómo Desplegar

### 1. Ingestion Function

- Navega al directorio `ingestion_function`.
- Despliega la función usando el comando `gcloud functions deploy ...`.
- Asegúrate de configurar las variables de entorno: `GCP_PROJECT`, `GCS_BUCKET_NAME`, `DRIVE_PARENT_FOLDER_ID`, `DRIVE_SECRET_NAME`.

### 2. dbt Transformation Job

- Navega al directorio `dbt_project`.
- Construye y despliega la imagen en Cloud Run Jobs usando `gcloud run jobs deploy ...`.
- Configura un trigger de Pub/Sub o invócalo manualmente.

... (más detalles)