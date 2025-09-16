# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /usr/app/dbt

# Instala dbt y el adaptador de BigQuery
RUN pip install dbt-bigquery

# Copia tu proyecto dbt al contenedor
# Copiamos primero los archivos de configuración para aprovechar el cache de Docker
COPY dbt_project.yml .
COPY packages.yml .

# Copia el resto del proyecto
COPY models/ ./models

# (Opcional) Instala dependencias si usas packages.yml
# RUN dbt deps --profiles-dir .

# Define el punto de entrada para que el job de Cloud Run pueda ejecutar comandos dbt
ENTRYPOINT ["dbt"]