{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Paso 1: Unificar todas las fuentes de la capa Bronce (Perfecto)
WITH all_sources_unioned AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_card') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_shared') }}
    UNION ALL
    SELECT * FROM {{ ref('revolut_account') }}
),

-- Paso 2: Clasificar cada transacción UNA SOLA VEZ
transactions_classified AS (
    SELECT
        hash_id,
        CAST(fecha AS DATE) AS fecha_transaccion,
        concepto,
        importe,
        entidad,
        origen,

        CASE
            WHEN importe > 0 THEN 'Ingreso'
            WHEN importe < 0 THEN 'Gasto'
            ELSE 'Neutro'
        END AS tipo_movimiento,
        
        CASE
            -- 1. Reglas más específicas primero
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND ABS(importe) IN (500, 750) THEN 'Aportación Periódica'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND ABS(importe) IN (500, 750) THEN 'Aportación Periódica Lledó'
            
            -- 2. Reglas de traspasos y liquidaciones
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%REVOLUT%' THEN 'Recarga Revolut'
            WHEN entidad = 'Revolut' AND UPPER(concepto) LIKE '%RECARGA%' THEN 'Recarga Revolut'
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta'
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE 'TRANSF INTERNA%' THEN 'Traspaso Interno Propio'
            -- --- *** MEJORA DE NOMENCLATURA ***
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' THEN 'Liquidación / Reembolso'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' THEN 'Liquidación / Reembolso Lledó'

            -- 3. Reglas para gastos especiales
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%RECIBO VISA CLASICA%' THEN 'Gasto Compartido Especial'

            -- 4. Regla por defecto
            ELSE 'Gasto/Ingreso Regular'
        END AS subtipo_transaccion
        
    FROM all_sources_unioned
),

-- Paso 3: Calcular el importe personal BASADO EN la clasificación anterior
transactions_with_personal_amount AS (
    SELECT
        *,
        CASE
            -- Los movimientos internos (traspasos, aportaciones, liquidaciones de otros) tienen un impacto personal de 0.
            WHEN subtipo_transaccion NOT IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN 0
            
            -- --- *** CORRECCIÓN CRÍTICA *** ---
            -- Los gastos en la cuenta COMPARTIDA son al 50%.
            WHEN origen = 'Shared' AND subtipo_transaccion IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN importe * 0.5
            
            -- El resto de gastos/ingresos (en tus cuentas personales) son 100% tuyos.
            ELSE importe
        END AS importe_personal
        
    FROM transactions_classified
)

-- Paso 4: Selección final y aplicación de macros (Perfecto)
SELECT
    hash_id,
    fecha_transaccion,
    concepto,
    importe,
    entidad,
    origen,
    tipo_movimiento,
    subtipo_transaccion,
    importe_personal,
    
    CASE
        WHEN subtipo_transaccion NOT IN ('Gasto/Ingreso Regular', 'Gasto Compartido Especial') THEN 'Movimientos Internos'
        ELSE {{ categorize_transaction('concepto', 'importe') }}
    END AS categoria,
    
    {{ standardize_entity('concepto', 'NULL') }} AS comercio,
    
    EXTRACT(YEAR FROM fecha_transaccion) AS anio,
    EXTRACT(MONTH FROM fecha_transaccion) AS mes,
    FORMAT_DATE('%Y-%m', fecha_transaccion) AS anio_mes

FROM transactions_with_personal_amount

{% if is_incremental() %}
  WHERE hash_id NOT IN (
    SELECT hash_id
    FROM {{ this }}
  )
{% endif %}