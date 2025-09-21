# ---- Dockerfile Final y Optimizado ----

# --- ETAPA 1: "Builder" ---
# Instala las dependencias en un entorno aislado.
FROM python:3.9-slim as builder

WORKDIR /app

# Copiar solo el archivo de requerimientos para optimizar el caché de Docker
COPY requirements.txt .

# Instalar dependencias en un entorno virtual (venv)
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# --- ETAPA 2: "Runtime" ---
# Imagen final, limpia y ligera.
FROM python:3.9-slim

# Instalar 'bash' ya que nuestro script lo requiere
RUN apt-get update && apt-get install -y --no-install-recommends bash && \
    rm -rf /var/lib/apt/lists/*

# Crear el directorio de trabajo y el usuario no-root
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /home/appuser/dbt

# Copiar el entorno virtual con las dependencias ya instaladas
COPY --from=builder /opt/venv /opt/venv

# Copiar el código del proyecto dbt
COPY . .

# Cambiar la propiedad de todo el directorio de trabajo al nuevo usuario
RUN chown -R appuser:appuser /home/appuser/dbt
# Asegurar permisos de ejecución para el script de entrada
RUN chmod +x run_dbt.sh

# Cambiar al usuario no-root para la ejecución
USER appuser

# Añadir el venv al PATH del sistema
ENV PATH="/opt/venv/bin:$PATH"

# Definir el punto de entrada
ENTRYPOINT ["./run_dbt.sh"]