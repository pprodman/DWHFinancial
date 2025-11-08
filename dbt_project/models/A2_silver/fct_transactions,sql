-- dbt_project/models/A2_silver/fct_transactions.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Paso 1: Unificar todas las fuentes de la capa Bronce en un único CTE.
WITH all_sources_unioned AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_card') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_shared') }}
    UNION ALL
    SELECT * FROM {{ ref('revolut_account') }}
),

-- Paso 2: Clasificar cada transacción UNA SOLA VEZ.
transactions_classified AS (
    SELECT
        -- Campos originales
        hash_id,
        CAST(fecha AS DATE) AS fecha,
        concepto,
        importe,
        entidad,
        origen,

        -- Clasificación del tipo de movimiento
        CASE
            WHEN importe > 0 THEN 'Ingreso'
            WHEN importe < 0 THEN 'Gasto'
            ELSE 'Neutro'
        END AS tipo_movimiento,
        
        -- Clasificación del subtipo de transacción (LA LÓGICA VIVE AQUÍ Y SOLO AQUÍ)
        CASE
            -- 1. Reglas más específicas primero
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND ABS(importe) IN (500, 750) THEN 'Aportación Periódica'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND ABS(importe) IN (500, 750) THEN 'Aportación Periódica Lledó'
            
            -- 2. Reglas de traspasos y liquidaciones
            -- Recargas entre tus cuentas (Bankinter <-> Revolut)
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%REVOLUT%' THEN 'Recarga Revolut'
            WHEN entidad = 'Revolut' AND UPPER(concepto) LIKE '%RECARGA%' THEN 'Recarga Revolut'
            -- Pago del recibo de la tarjeta desde la cuenta
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta'
            -- Aportaciones a la cuenta común
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE 'TRANSF INTERNA%' THEN 'Traspaso Interno'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' THEN 'Traspaso Interno'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' THEN 'Traspaso Interno Lledó'

            -- 3. Reglas para gastos especiales
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%RECIBO VISA CLASICA%' THEN 'Gasto Compartido (Visa Clásica)'

            -- 4. Regla por defecto
            ELSE 'Gasto/Ingreso Regular'
        END AS subtipo_transaccion
        
    FROM all_sources_unioned
),

-- Paso 3: Calcular el importe personal BASADO EN la clasificación anterior.
transactions_with_personal_amount AS (
    SELECT
        *,
        -- Lógica de importe personal, ahora súper simple y legible
        CASE
            -- Los traspasos, aportaciones y reembolsos de otros no afectan a tu gasto/ingreso personal
            WHEN subtipo_transaccion NOT IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN 0
            -- Para gastos regulares en la cuenta común, es el 50%
            WHEN origen = 'Shared' AND subtipo_transaccion IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN importe * 0.5
            -- Para el resto de cuentas y movimientos, es el 100%
            ELSE importe
        END AS importe_personal
        
    FROM transactions_classified
)

-- Paso 4: Selección final y aplicación de macros
SELECT
    -- Columnas clave
    hash_id,
    fecha,
    concepto,
    importe,
    entidad,
    origen,
    tipo_movimiento,
    subtipo_transaccion,
    importe_personal,
    
    -- Categoría y Comercio (usando las clasificaciones ya hechas)
    CASE
        WHEN subtipo_transaccion NOT IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN 'Movimientos Internos'
        ELSE {{ categorize_transaction('concepto', 'importe') }}
    END AS categoria,
    
    {{ standardize_entity('concepto', 'NULL') }} AS comercio,
    
    -- Campos de fecha
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes

FROM transactions_with_personal_amount

{% if is_incremental() %}
  WHERE hash_id NOT IN (
    SELECT hash_id
    FROM {{ this }}
  )
{% endif %}