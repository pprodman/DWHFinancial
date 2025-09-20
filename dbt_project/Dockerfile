# ---- Dockerfile Optimizado ----

# --- ETAPA 1: "Builder" ---
FROM python:3.9-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# --- ETAPA 2: "Runtime" ---
FROM python:3.9-slim

# --- AÑADIR ESTE BLOQUE ---
# Instalar bash y limpiar el caché de apt en la misma capa
RUN apt-get update && apt-get install -y --no-install-recommends bash && \
    rm -rf /var/lib/apt/lists/*

# 3. Crear un usuario no-root para mejorar la seguridad
# (Ahora este comando encontrará /bin/bash y funcionará perfectamente)
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /home/appuser/dbt

# 4. Copiar el entorno virtual...
COPY --from=builder /opt/venv /opt/venv
COPY --chown=appuser:appuser . .
# --- AÑADIR O DESCOMENTAR ESTA LÍNEA ---
# Asegurarse de que el script tiene permisos de ejecución
RUN chmod +x run_dbt.sh
USER appuser
ENV PATH="/opt/venv/bin:$PATH"
ENTRYPOINT ["./run_dbt.sh"]