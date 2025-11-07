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

-- Paso 2: Limpiar y enriquecer los datos. Aquí es donde aplicamos toda la lógica.
transactions_enriched AS (
    SELECT
        -- Campos originales
        hash_id,
        CAST(fecha AS DATE) AS fecha_transaccion,
        concepto,
        importe,
        entidad,
        origen,

        -- CLASIFICACIÓN 1: Tipo de movimiento (Ingreso/Gasto)
        CASE
            WHEN importe > 0 THEN 'Ingreso'
            WHEN importe < 0 THEN 'Gasto'
            ELSE 'Neutro'
        END AS tipo_movimiento,
        
        -- CLASIFICACIÓN 2: Lógica de transferencias y movimientos internos (¡LA CLAVE!)
        CASE
            -- Recargas entre tus cuentas (Bankinter <-> Revolut)
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%REVOLUT%' THEN 'Recarga Revolut'
            WHEN entidad = 'Revolut' AND UPPER(concepto) LIKE '%RECARGA%' THEN 'Recarga Revolut'
            
            -- Pago del recibo de la tarjeta desde la cuenta
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta' -- Asumiendo que 'PLATINUM' es tu tarjeta
            
            -- Aportaciones a la cuenta común
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE 'TRANSF INTERNA%' THEN 'Traspaso Interno'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' THEN 'Traspaso Interno'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' THEN 'Traspaso Interno Lledó'

            -- Aportaciones periódicas a la cuenta común
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND (ABS(importe) = 500 OR ABS(importe) = 750) THEN 'Aportación Periódica'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND (ABS(importe) = 500 OR ABS(importe) = 750) THEN 'Aportación Periódica Lledó'
               
            -- Por defecto, cualquier otra cosa es una transacción real con el exterior
            ELSE 'Gasto/Ingreso Regular'
        END AS subtipo_transaccion,
        
        -- CAMPO ANALÍTICO 1: Importe que te corresponde a ti
        CASE
            -- Para gastos/ingresos regulares en la cuenta común, es el 50%
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND ((UPPER(concepto) NOT LIKE '%PABLO%') OR (UPPER(concepto) NOT LIKE '%LLEDO%')) THEN importe * 0.5
            -- Para el resto de cuentas y movimientos, es el 100%
            ELSE importe
        END AS importe_personal

    FROM all_sources_unioned
)

-- Paso 3: Selección final, aplicando macros de categorización y añadiendo campos de fecha.
SELECT
    t.*,  -- Seleccionamos todas las columnas del CTE enriquecido

    -- CLASIFICACIÓN 3: Categoría y Comercio
    CASE 
        -- Los traspasos no se categorizan como un gasto, son una categoría en sí mismos.
        WHEN t.subtipo_transaccion IN ('Recarga Revolut', 'Liquidación Tarjeta', 'Traspaso Interno', 'Aportación Periódica') THEN 'Movimientos Internos'
        WHEN t.subtipo_transaccion IN ('Traspaso Interno Lledó', 'Aportación Periódica Lledó') THEN 'Movimientos Internos Lledó'
        -- Para el resto, usamos tu macro de categorización.
        ELSE {{ categorize_transaction('t.concepto', 't.importe') }}
    END AS categoria,
    
    {{ standardize_entity('t.concepto', 'NULL') }} AS comercio,
    
    -- Campos de fecha para facilitar el análisis en la capa Oro
    EXTRACT(YEAR FROM t.fecha_transaccion) AS anio,
    EXTRACT(MONTH FROM t.fecha_transaccion) AS mes,
    FORMAT_DATE('%Y-%m', t.fecha_transaccion) AS anio_mes

FROM transactions_enriched t

{% if is_incremental() %}
  WHERE t.hash_id NOT IN (
    SELECT hash_id
    FROM {{ this }}
  )
{% endif %}