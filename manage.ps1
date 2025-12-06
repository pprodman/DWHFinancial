<#
.SYNOPSIS
    Gestor de Proyecto DWH Financial (Modo Híbrido)
.DESCRIPTION
    Script para gestionar las tareas locales de Python.
    Las tareas de transformación (dbt) se gestionan ahora en dbt Cloud.
.EXAMPLE
    .\scripts\manage.ps1 install
    .\scripts\manage.ps1 run-ingestion
#>

param (
    [Parameter(Mandatory=$false)]
    [ValidateSet("install", "run-ingestion", "clean", "help")]
    [string]$Command = "help"
)

# Definimos rutas relativas a la raíz del proyecto
# PSScriptRoot es la carpeta 'scripts/', así que subimos un nivel.
$RootPath = Resolve-Path "$PSScriptRoot\.."
$IngestionDir = Join-Path $RootPath "ingestion"
$VenvPython = Join-Path $RootPath ".venv\Scripts\python.exe"
# Nota: Si usas Conda, el script usará el python activo en la terminal, lo cual es preferible.

function Show-Help {
    Write-Host "--- 🏦 DWH Financial Manager (Hybrid Edition) ---" -ForegroundColor Cyan
    Write-Host "  install         - Instala las dependencias de Python (pandas, google, etc.)"
    Write-Host "  run-ingestion   - Ejecuta el proceso de descarga de Drive y subida a GCS"
    Write-Host "  clean           - Limpia archivos temporales y cachés"
    Write-Host ""
    Write-Host "  Nota: Para ejecutar dbt, usa la interfaz web de dbt Cloud." -ForegroundColor DarkGray
}

if ($Command -eq "help") { Show-Help; exit }

# --- INSTALL ---
if ($Command -eq "install") {
    Write-Host "📦 Instalando dependencias de Ingestión..." -ForegroundColor Green
    # Usamos pip directamente, asumiendo que el entorno (Conda) ya está activo en VS Code
    pip install -r "$IngestionDir\requirements.txt"
    if ($LASTEXITCODE -eq 0) { Write-Host "✅ Instalación completada." -ForegroundColor Green }
}

# --- RUN INGESTION ---
if ($Command -eq "run-ingestion") {
    Write-Host "🚀 Iniciando Pipeline de Ingestión..." -ForegroundColor Green

    # Comprobar si existe .env
    if (-not (Test-Path "$RootPath\.env")) {
        Write-Warning "⚠️ No detecto el archivo .env en la raíz ($RootPath). El script podría fallar si no hay variables de entorno."
    }

    $ScriptPath = Join-Path $IngestionDir "main.py"

    # Ejecutamos Python
    python $ScriptPath
}

# --- CLEAN ---
if ($Command -eq "clean") {
    Write-Host "🧹 Limpiando archivos temporales..." -ForegroundColor Green

    # Borrar __pycache__ y .pyc recursivamente
    Get-ChildItem -Path $RootPath -Recurse -Include "__pycache__", "*.pyc", ".pytest_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

    # Borrar logs locales de dbt (si quedaron)
    if (Test-Path "$RootPath\transformation\logs") { Remove-Item "$RootPath\transformation\logs" -Recurse -Force }
    if (Test-Path "$RootPath\transformation\target") { Remove-Item "$RootPath\transformation\target" -Recurse -Force }

    Write-Host "✨ Limpieza finalizada." -ForegroundColor Green
}