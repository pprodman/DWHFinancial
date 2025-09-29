# trigger_dbt_function/main.py

import functions_framework
from google.cloud import run_v2
import os

PROJECT_ID = os.environ.get('PROJECT_ID', 'dwhfinancial')
REGION = os.environ.get('REGION', 'us-central1')
JOB_NAME = os.environ.get('JOB_NAME', 'dbt-transform-job')

@functions_framework.cloud_event
def trigger_dbt_job(cloud_event):
    data = cloud_event.data
    file_name = data["name"]
    bucket_name = data["bucket"]
    print(f"Nuevo archivo detectado: gs://{bucket_name}/{file_name}")

    # Cliente para Cloud Run Jobs (v2)
    client = run_v2.JobsClient()
    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"

    try:
        operation = client.run_job(request=run_v2.RunJobRequest(name=name))
        print(f"Job iniciado. Operación: {operation.operation.name}")
        # Opcional: esperar a que termine (no recomendado en funciones por timeout)
        # operation.result()
        print("✅ Job ejecutado exitosamente.")
    except Exception as e:
        print(f"❌ Error al ejecutar el job: {e}")
        raise