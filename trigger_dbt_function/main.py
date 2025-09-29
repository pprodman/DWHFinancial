# trigger_dbt_function/main.py

import functions_framework
from google.cloud import run_v2
import os

PROJECT_ID = os.environ.get('PROJECT_ID', 'dwhfinancial')
REGION = os.environ.get('REGION', 'us-central1')
JOB_NAME = os.environ.get('JOB_NAME', 'dbt-transform-job')

@functions_framework.cloud_event
def trigger_dbt_job(cloud_event):
    """Se ejecuta cuando un archivo se sube a GCS."""
    data = cloud_event.data
    file_name = data["name"]
    bucket_name = data["bucket"]
    
    print(f"Nuevo archivo detectado: gs://{bucket_name}/{file_name}")
    
    # Inicializar el cliente de Cloud Run Jobs
    client = run_v2.JobsClient()
    job_path = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
    # Crear una ejecución
    request = run_v2.RunJobRequest(name=job_path)
    
    try:
        operation = client.run_job(request=request)
        print(f"Job ejecutado. Operación: {operation.operation.name}")
        operation.result()  # Espera a que termine (opcional)
        print("✅ Job completado.")
    except Exception as e:
        print(f"❌ Error al ejecutar el job: {e}")
        raise