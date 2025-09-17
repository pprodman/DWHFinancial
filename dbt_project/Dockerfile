# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /usr/app/dbt

# Instala dbt y el adaptador de BigQuery
RUN pip install dbt-bigquery

# Copia tu proyecto dbt al contenedor
COPY . .

# Asegurarse de que el script tiene permisos de ejecución
RUN chmod +x run_dbt.sh

# Define el script de shell como el punto de entrada
ENTRYPOINT ["./run_dbt.sh"]