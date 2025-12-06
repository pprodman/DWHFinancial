<#
.SYNOPSIS
    Gestor de Proyecto DWH Financial (Modo Híbrido)
.DESCRIPTION
    Script para gestionar las tareas locales de Python.
    Las tareas de transformación (dbt) se gestionan ahora en dbt Cloud.
.EXAMPLE
    .\scripts\manage.ps1 install
    .\scripts\manage.ps1 run-ingestion
    .\scripts\manage.ps1 update-seeds
#>

param (
    [Parameter(Mandatory=$false)]
    [ValidateSet("install", "run-ingestion", "clean", "help", "update-seeds")]
    [string]$Command = "help"
)

# Definimos rutas relativas a la raíz del proyecto
$RootPath = Resolve-Path "$PSScriptRoot\.."
$IngestionDir = Join-Path $RootPath "ingestion"

function Show-Help {
    Write-Host "--- DWH Financial Manager (Hybrid Edition) ---" -ForegroundColor Cyan
    Write-Host "  install         - Instala las dependencias de Python"
    Write-Host "  run-ingestion   - Ejecuta el pipeline de ingestión (Drive -> GCS)"
    Write-Host "  update-seeds    - Descarga el mapeo actualizado desde Google Sheets -> CSV Local"
    Write-Host "  clean           - Limpia archivos temporales"
    Write-Host ""
    Write-Host "  Nota: Para ejecutar dbt, usa la interfaz web de dbt Cloud." -ForegroundColor DarkGray
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

    if (-not (Test-Path "$RootPath\.env")) {
        Write-Warning "[AVISO] No detecto el archivo .env en la raiz."
    }

    $ScriptPath = Join-Path $IngestionDir "main.py"
    python $ScriptPath
}

# --- UPDATE SEEDS ---
if ($Command -eq "update-seeds") {
    Write-Host "[INFO] Sincronizando Seeds desde Google Sheets..." -ForegroundColor Cyan

    if (-not (Test-Path "$RootPath\.env")) {
        Write-Warning "[AVISO] No detecto el archivo .env. El script fallara si no encuentra MAPPING_SHEET_ID."
    }

    $ScriptPath = Join-Path $IngestionDir "sync_seeds.py"
    python $ScriptPath
}

# --- CLEAN ---
if ($Command -eq "clean") {
    Write-Host "[INFO] Limpiando archivos temporales..." -ForegroundColor Green
    Get-ChildItem -Path $RootPath -Recurse -Include "__pycache__", "*.pyc", ".pytest_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    if (Test-Path "$RootPath\transformation\logs") { Remove-Item "$RootPath\transformation\logs" -Recurse -Force }
    if (Test-Path "$RootPath\transformation\target") { Remove-Item "$RootPath\transformation\target" -Recurse -Force }
    Write-Host "[OK] Limpieza finalizada." -ForegroundColor Green
}