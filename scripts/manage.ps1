<#
.SYNOPSIS
    Gestor de Proyecto DWH Financial (Modo Híbrido)
.DESCRIPTION
    Script para gestionar las tareas locales de Python y dbt.
.EXAMPLE
    .\scripts\manage.ps1 run-ingestion
    .\scripts\manage.ps1 update-seeds
    .\scripts\manage.ps1 dbt-refresh
#>

# --- CARGA DE VARIABLES DE ENTORNO (.env) ---
# Esto se ejecuta siempre antes que nada.
$RootPath = Resolve-Path "$PSScriptRoot\.."
if (Test-Path "$RootPath\.env") {
    Write-Host "[INFO] Cargando variables desde .env..." -ForegroundColor DarkGray
    Get-Content "$RootPath\.env" | ForEach-Object {
        # Ignorar comentarios (#) y líneas vacías
        if ($_ -match '^([^#=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
}

param (
    [Parameter(Mandatory=$false)]
    # Añadimos 'dbt-refresh' a las opciones
    [ValidateSet("install", "run-ingestion", "clean", "help", "update-seeds", "dbt-refresh")]
    [string]$Command = "help"
)

$IngestionDir = Join-Path $RootPath "ingestion"

function Show-Help {
    Write-Host "--- DWH Financial Manager (Hybrid Edition) ---" -ForegroundColor Cyan
    Write-Host "  install         - Instala las dependencias de Python"
    Write-Host "  run-ingestion   - Ejecuta el pipeline de ingestión (Drive -> GCS)"
    Write-Host "  update-seeds    - Descarga el mapeo actualizado desde Google Sheets"
    Write-Host "  dbt-refresh     - Reconstruye todas las tablas dbt desde cero (Full Refresh)"
    Write-Host "  clean           - Limpia archivos temporales"
}

if ($Command -eq "help") { Show-Help; exit }

# --- INSTALL ---
if ($Command -eq "install") {
    Write-Host "[INFO] Instalando dependencias..." -ForegroundColor Green
    pip install -r "$IngestionDir\requirements.txt"
    if ($LASTEXITCODE -eq 0) { Write-Host "[OK] Instalacion completada." -ForegroundColor Green }
}

# --- RUN INGESTION ---
if ($Command -eq "run-ingestion") {
    Write-Host "[INFO] Iniciando Pipeline de Ingestion..." -ForegroundColor Green
    $ScriptPath = Join-Path $IngestionDir "main.py"
    python $ScriptPath
}

# --- UPDATE SEEDS ---
if ($Command -eq "update-seeds") {
    Write-Host "[INFO] Sincronizando Seeds desde Google Sheets..." -ForegroundColor Green
    $ScriptPath = Join-Path $IngestionDir "sync_seeds.py"
    python $ScriptPath
}

# --- DBT FULL REFRESH (NUEVO) ---
if ($Command -eq "dbt-refresh") {
    Write-Host "[INFO] Ejecutando dbt Full Refresh..." -ForegroundColor Cyan

    # Configuramos el directorio de perfiles automáticamente
    $env:DBT_PROFILES_DIR = Join-Path $RootPath "transformation"

    # 1. Actualizar Seeds primero (por si el Excel cambió)
    Write-Host "   > Cargando seeds (map_categories, master_mapping...)" -ForegroundColor Gray
    dbt seed --project-dir "$RootPath\transformation"

    # 2. Ejecutar Full Refresh
    Write-Host "   > Reconstruyendo tablas..." -ForegroundColor Gray
    dbt run --full-refresh --project-dir "$RootPath\transformation"

    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Tablas reconstruidas y categorizadas." -ForegroundColor Green
    } else {
        Write-Error "[ERROR] Algo fallo en dbt."
    }
}

# --- CLEAN ---
if ($Command -eq "clean") {
    Write-Host "[INFO] Limpiando archivos temporales..." -ForegroundColor Green
    Get-ChildItem -Path $RootPath -Recurse -Include "__pycache__", "*.pyc", ".pytest_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    if (Test-Path "$RootPath\transformation\logs") { Remove-Item "$RootPath\transformation\logs" -Recurse -Force }
    if (Test-Path "$RootPath\transformation\target") { Remove-Item "$RootPath\transformation\target" -Recurse -Force }
    Write-Host "[OK] Limpieza finalizada." -ForegroundColor Green
}