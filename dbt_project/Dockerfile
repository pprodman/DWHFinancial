# ---- Dockerfile Optimizado ----

# --- ETAPA 1: "Builder" ---
FROM python:3.9-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# --- ETAPA 2: "Runtime" ---
FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends bash && \
    rm -rf /var/lib/apt/lists/*

# 3. Crear un usuario no-root para mejorar la seguridad
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /home/appuser/dbt

# 4. Copiar el entorno virtual...
COPY --from=builder /opt/venv /opt/venv

# 5. Copiar todo el código del proyecto dbt
COPY . .

# Cambiar la propiedad de TODO el directorio de trabajo al nuevo usuario
RUN chown -R appuser:appuser /home/appuser/dbt

# Asegurarse de que el script tiene permisos de ejecución
RUN chmod +x run_dbt.sh
USER appuser
ENV PATH="/opt/venv/bin:$PATH"
ENTRYPOINT ["./run_dbt.sh"]