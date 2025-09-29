import subprocess
import os

PROJECT_ID = os.environ.get('PROJECT_ID', 'dwhfinancial')
REGION = os.environ.get('REGION', 'us-central1')
JOB_NAME = os.environ.get('JOB_NAME', 'dbt-transform-job')

def trigger_dbt_job(event, context):
    """Se ejecuta cuando un archivo se sube a GCS."""
    file_name = event['name']
    bucket_name = event['bucket']
    
    print(f"Nuevo archivo detectado: gs://{bucket_name}/{file_name}")
    
    # Ejecuta el job de Cloud Run
    subprocess.run([
        'gcloud', 'run', 'jobs', 'execute', JOB_NAME,
        '--project', PROJECT_ID,
        '--region', REGION,
        '--wait'
    ], check=True)