# ---- Dockerfile Optimizado ----

# --- ETAPA 1: "Builder" ---
# Usamos una imagen completa para compilar las dependencias de forma segura
FROM python:3.9-slim as builder

WORKDIR /app

# 1. Copiar SOLO el archivo de requerimientos
COPY requirements.txt .

# 2. Instalar dependencias en un entorno virtual aislado (venv)
#    --no-cache-dir reduce el tamaño de la capa
#    El venv se crea en /opt/venv para que sea fácil de encontrar y copiar
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# --- ETAPA 2: "Runtime" ---
# Empezamos de nuevo desde una imagen base limpia y ligera
FROM python:3.9-slim

# 3. Crear un usuario no-root para mejorar la seguridad
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /home/appuser/dbt

# 4. Copiar el entorno virtual con las dependencias ya instaladas desde la etapa "builder"
#    Esto es clave: no traemos herramientas de compilación ni caché, solo el resultado final.
COPY --from=builder /opt/venv /opt/venv

# 5. Copiar todo el código del proyecto dbt, estableciendo el propietario al nuevo usuario
COPY --chown=appuser:appuser . .

# 6. Cambiar al usuario no-root
USER appuser

# 7. Añadir el venv al PATH del sistema para que los comandos `dbt` funcionen directamente
ENV PATH="/opt/venv/bin:$PATH"

# 8. El script ya tiene permisos, pero si no, este es el lugar para asegurarlo
# RUN chmod +x run_dbt.sh

# 9. Definir el punto de entrada
ENTRYPOINT ["./run_dbt.sh"]