{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Paso 1: Unificar fuentes
WITH all_sources_unioned AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_card') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_shared') }}
    UNION ALL
    SELECT * FROM {{ ref('revolut_account') }}
    UNION ALL
    SELECT * FROM {{ ref('cash') }}
),

-- Paso 2: Clasificación Lógica
transactions_classified AS (
    SELECT
        hash_id,
        CAST(fecha AS DATE) AS fecha,
        concepto,
        importe,
        entidad,
        origen,

        -- Tipo básico
        CASE
            WHEN importe > 0 THEN 'Ingreso'
            WHEN importe < 0 THEN 'Gasto'
            ELSE 'Neutro'
        END AS tipo_movimiento,

        -- Subtipo Operativo (Lógica de negocio crítica)
        CASE
            -- 1. Aportaciones (Ingresos o traspasos periódicos)
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación Periódica'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación Periódica Lledó'

            -- 2. Liquidaciones de Tarjeta (CRÍTICO: Restaurado para que funcione Gold)
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%RECIBO VISA CLASICA%' THEN 'Liquidación Tarjeta Compartida'

            -- 3. Recargas y Traspasos
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%REVOLUT%' THEN 'Recarga Revolut'
            WHEN entidad = 'Revolut' AND UPPER(concepto) LIKE '%RECARGA%' THEN 'Recarga Revolut'

            -- 4. Otros
            WHEN UPPER(concepto) LIKE '%BIZUM%' THEN 'Bizum'
            ELSE 'Transacción Regular'
        END AS subtipo_transaccion

    FROM all_sources_unioned
),

-- Paso 3: Aplicación de la Macro de Categorización Jerárquica
transactions_enriched AS (
    SELECT
        *,
        -- Llamamos a la macro UNA VEZ por fila
        {{ categorize_transaction('concepto') }} as _cat_string
    FROM transactions_classified
)

-- Paso 4: Desglose final
SELECT
    hash_id,
    fecha,
    concepto,
    importe,
    entidad,
    origen,
    tipo_movimiento,
    subtipo_transaccion,

    -- Lógica de importe personal (Corregida para incluir Bizums y Tarjetas compartidas)
    CASE
        -- Si sale de la cuenta compartida y es un gasto normal, Bizum o la tarjeta común -> 50%
        WHEN origen = 'Shared' AND subtipo_transaccion IN ('Transacción Regular', 'Bizum', 'Liquidación Tarjeta Compartida') THEN importe * 0.5
        -- Si son las aportaciones periódicas, no cuentan como gasto/ingreso personal directo en este punto
        WHEN subtipo_transaccion IN ('Aportación Periódica', 'Aportación Periódica Lledó') THEN 0
        ELSE importe
    END AS importe_personal,

    -- Desempaquetamos la cadena de la macro
    SPLIT(_cat_string, '|')[SAFE_OFFSET(0)] AS grupo,
    SPLIT(_cat_string, '|')[SAFE_OFFSET(1)] AS categoria,
    SPLIT(_cat_string, '|')[SAFE_OFFSET(2)] AS subcategoria,

    -- Fallback para comercio
    CASE
        WHEN SPLIT(_cat_string, '|')[SAFE_OFFSET(3)] = 'Desconocido' THEN INITCAP(concepto)
        ELSE SPLIT(_cat_string, '|')[SAFE_OFFSET(3)]
    END AS comercio,

    -- Fechas
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes

FROM transactions_enriched

{% if is_incremental() %}
  WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})
{% endif %}