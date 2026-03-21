# DWH Financial - Personal Finance Project

## 1. Project Context

Este es un Data Warehouse de finanzas personales construido con **dbt Core** y **BigQuery**. El objetivo es consolidar movimientos de múltiples cuentas (Bankinter Personal, Bankinter Shared, Revolut, Efectivo) para analizar gastos, ingresos y ahorro.

## 2. Tech Stack

- **Database**: Google BigQuery.

- **Transformation**: dbt (data build tool).

- **Language**: SQL (StandardDialect) y Python (para scripts auxiliares).

- **Infrastructure**: Local development syncs with Cloud.

## 3. Data Architecture (Layers)

### A1_bronze (Raw)

- Ingesta directa de CSVs o APIs.

- Sin transformaciones de negocio, solo tipos de datos básicos.

### A2_silver (Transformation & Logic)

- Modelo principal: `fct_transactions`.

- Lógica de Categorización: Uso de macros `categorize_transaction`, `extract_bizum_name`.

- Lógica de "Operativa Interna":

    - Se detectan `Traspaso Interno`, `Aportación`, `Liquidación Tarjeta`.

    - Se limpian nombres de Bizum usando el directorio `bizum_directory`.

### A3_gold (Reporting)

- Vista principal: `fct_financial_user_view`.

- Aplica ajustes manuales finales y enriquecimiento de negocio para visualización.

## 4. Core Business Rules (CRITICAL)

### A. Cuentas Compartidas (Shared)

- Regla General: Todo gasto en `origen = 'Shared'` se considera compartido.

- Imputación: El usuario soporta el 50% del importe (`importe * 0.5`).

- Excepciones: Las "Aportaciones" (ingresos de dinero a la cuenta común) NO son gastos ni ingresos, son movimientos internos (`importe_personal = 0`).

### B. Lógica de Ajustes Manuales (Tabla `adjustments`)

- Se utiliza para corregir la imputación de gastos en la cuenta compartida.

- `adjustment_type = 'PARTNER_EXPENSE'`: El gasto es 100% de la pareja. Mi coste es 0.

- `adjustment_type = 'MY_EXPENSE'`: El gasto es 100% mío. Mi coste es el total.

- `adjustment_amount = X`: Ajuste parcial. Mi coste se calcula como `(ABS(Total) - ABS(Parte_Pareja)) / 2`.

### C. Contabilidad de Nóminas

- Para alinear la contabilidad, las nóminas (y ciertos ingresos recurrentes) se imputan al mes contable, no al natural.

- Fórmula: `DATE_TRUNC(DATE_SUB(fecha, INTERVAL 16 DAY), MONTH)`.

### D. Movimientos Reales vs. Técnicos

- `es_movimiento_real`: Flag booleano.

- `TRUE`: Gastos en supermercado, restaurantes, luz, nóminas.

- `FALSE`: Movimientos de dinero entre cuentas propias, pagos de tarjeta de crédito (la liquidación), aportaciones a la cuenta común.

## 5. Development Conventions

- SQL Style:

    - Comas al inicio (`SELECT \n , campo1 \n , campo2`).

    - Palabras clave en MAYÚSCULAS (`SELECT`, `FROM`, `WHERE`).

    - Indentación de 4 espacios.

- Macros:
    - `extract_bizum_name(column)`: Extrae nombres limpios de descripciones de Bizum.

    - `clean_bizum_name(column)`: Limpieza regex de respaldo.

- Seeds:

    - Los archivos CSV en `seeds/` deben sincronizarse usando `scripts/sync_seeds.py` que conecta con Google Sheets.

    - Los campos vacíos en CSV deben tratarse como `NULL`.