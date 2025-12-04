<#
.SYNOPSIS
    Script de automatización para DWH Financial (Equivalente a Makefile para Windows)
.EXAMPLE
    .\manage.ps1 setup
    .\manage.ps1 run-dev
#>

param (
    [Parameter(Mandatory=$false)]
    [ValidateSet("setup", "install", "test-dbt", "run-dev", "docs", "clean", "help")]
    [string]$Command = "help"
)

$VenvDir = Join-Path $PSScriptRoot ".venv"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$DbtExe = Join-Path $VenvDir "Scripts\dbt.exe"

$IngestionDir = Join-Path $PSScriptRoot "ingestion"
$TransformationDir = Join-Path $PSScriptRoot "transformation"

function Show-Help {
    Write-Host "Comandos disponibles:" -ForegroundColor Cyan
    Write-Host "  .\manage.ps1 setup       - Setup inicial (crear venv e instalar todo)"
    Write-Host "  .\manage.ps1 install     - Reinstalar dependencias"
    Write-Host "  .\manage.ps1 test-dbt    - Ejecutar tests de dbt"
    Write-Host "  .\manage.ps1 run-dev     - Ejecutar pipeline completo (Ingesta + dbt)"
    Write-Host "  .\manage.ps1 docs        - Generar y servir documentación"
    Write-Host "  .\manage.ps1 clean       - Limpiar archivos temporales"
}

if ($Command -eq "help") { Show-Help; exit }

# --- SETUP ---
if ($Command -eq "setup") {
    Write-Host "🚀 Iniciando Setup..." -ForegroundColor Green
    if (-not (Test-Path $VenvDir)) {
        Write-Host "Creando entorno virtual..."
        python -m venv $VenvDir
    }
    # Actualizar pip
    & $PythonExe -m pip install --upgrade pip
    # Ir al paso de instalación
    & $MyInvocation.MyCommand.Path install
}

# --- INSTALL ---
if ($Command -eq "install") {
    Write-Host "📦 Instalando dependencias..." -ForegroundColor Green
    
    # Opción A: Si usas el requirements.txt unificado en la raíz (Recomendado en v2)
    if (Test-Path "requirements.txt") {
        & $PythonExe -m pip install -r requirements.txt
    }
    # Opción B: Si prefieres instalar por módulos (como en tu Makefile antiguo)
    else {
        if (Test-Path "$IngestionDir\requirements.txt") { & $PythonExe -m pip install -r "$IngestionDir\requirements.txt" }
        if (Test-Path "$TransformationDir\requirements.txt") { & $PythonExe -m pip install -r "$TransformationDir\requirements.txt" }
    }

    # Instalar dependencias de dbt
    Write-Host "Bajando paquetes de dbt..."
    Push-Location $TransformationDir
    try {
        & $DbtExe deps
    } finally {
        Pop-Location
    }
}

# --- TEST DBT ---
if ($Command -eq "test-dbt") {
    Write-Host "🧪 Ejecutando tests..." -ForegroundColor Green
    Push-Location $TransformationDir
    try {
        & $DbtExe test --target dev --profiles-dir .
    } finally {
        Pop-Location
    }
}

# --- RUN DEV ---
if ($Command -eq "run-dev") {
    Write-Host "▶️  Ejecutando Pipeline en DEV..." -ForegroundColor Green
    
    # 1. Ingesta
    Write-Host "[1/2] Ejecutando Ingesta..." -ForegroundColor Yellow
    # Aseguramos que las variables de entorno del .env estén cargadas para python
    & $PythonExe "$IngestionDir\main.py"
    
    if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en la ingesta"; exit 1 }

    # 2. Transformación
    Write-Host "[2/2] Ejecutando Transformación (dbt)..." -ForegroundColor Yellow
    Push-Location $TransformationDir
    try {
        & $DbtExe seed --target dev --profiles-dir .
        & $DbtExe run --target dev --profiles-dir .
    } finally {
        Pop-Location
    }
}

# --- DOCS ---
if ($Command -eq "docs") {
    Write-Host "📚 Generando documentación..." -ForegroundColor Green
    Push-Location $TransformationDir
    try {
        & $DbtExe docs generate --target dev --profiles-dir .
        & $DbtExe docs serve --profiles-dir .
    } finally {
        Pop-Location
    }
}

# --- CLEAN ---
if ($Command -eq "clean") {
    Write-Host "🧹 Limpiando..." -ForegroundColor Green
    # Borrar carpetas de dbt
    $PathsToRemove = @(
        "$TransformationDir\target",
        "$TransformationDir\dbt_packages",
        "$TransformationDir\logs"
    )
    foreach ($Path in $PathsToRemove) {
        if (Test-Path $Path) { Remove-Item -Path $Path -Recurse -Force; Write-Host "Borrado: $Path" }
    }
    
    # Borrar pycache
    Get-ChildItem -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
    Write-Host "Limpieza completada."
}