{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

-- 1. Unificar fuentes
WITH all_sources AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_card') }}
    UNION ALL
    SELECT * FROM {{ ref('bankinter_shared') }}
    UNION ALL
    SELECT * FROM {{ ref('revolut_account') }}
    UNION ALL
    SELECT * FROM {{ ref('cash_expenses') }}
),

-- 2. Enriquecimiento con Macro y Lógica Operativa
enriched_transactions AS (
    SELECT
        hash_id,
        CAST(fecha AS DATE) AS fecha,
        concepto,
        importe,
        entidad,
        origen,

        -- Macro maestra
        {{ categorize_transaction('concepto') }} as _cat_string,

        -- Detectar operativa interna
        CASE
            -- Liquidaciones
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%RECIBO VISA CLASICA%' THEN 'Liquidación Tarjeta Compartida'
            -- Aportaciones
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación Lledó'
            -- Otros
            WHEN UPPER(concepto) LIKE '%TRASPASO%' OR UPPER(concepto) LIKE '%TRANSFERENCIA INTERNA%' THEN 'Traspaso Interno'
            WHEN UPPER(concepto) LIKE '%BIZUM%' THEN 'Bizum'
            ELSE 'Movimiento Regular'
        END AS operativa_interna

    FROM all_sources
)

-- 3. Proyección Final
SELECT
    hash_id,
    fecha,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes,
    EXTRACT(QUARTER FROM fecha) AS trimestre,

    concepto,
    importe, -- Importe TOTAL original
    entidad,
    origen,
    operativa_interna,

    -- Dimensiones Jerárquicas
    SPLIT(_cat_string, '|')[SAFE_OFFSET(0)] AS grupo,
    SPLIT(_cat_string, '|')[SAFE_OFFSET(1)] AS categoria,
    SPLIT(_cat_string, '|')[SAFE_OFFSET(2)] AS subcategoria,

    CASE
        WHEN SPLIT(_cat_string, '|')[SAFE_OFFSET(3)] = 'Desconocido' THEN INITCAP(concepto)
        ELSE SPLIT(_cat_string, '|')[SAFE_OFFSET(3)]
    END AS comercio,

    -- NUEVA COLUMNA BOOLEANA: ¿Es un gasto compartido?
    CASE
        -- Todo lo que salga de la cuenta 'Shared' (salvo aportaciones de capital) es compartido
        WHEN origen = 'Shared' AND operativa_interna NOT IN ('Aportación', 'Aportación Lledó') THEN TRUE
        ELSE FALSE
    END AS es_compartido,

    -- Métricas Calculadas (Convenience)
    -- Mantenemos importe_personal ya calculado por comodidad, pero usando el booleano
    CASE
        -- Si es compartido -> 50%
        WHEN origen = 'Shared' AND operativa_interna NOT IN ('Aportación', 'Aportación Lledó') THEN importe * 0.5
        -- Aportaciones -> 0 (Neutro)
        WHEN operativa_interna IN ('Aportación', 'Aportación Lledó') THEN 0
        -- Resto -> 100%
        ELSE importe
    END AS importe_personal,

    -- Flag de Movimiento Real (para filtros)
    CASE
        WHEN operativa_interna = 'Liquidación Tarjeta Compartida' THEN TRUE
        WHEN operativa_interna IN ('Liquidación Tarjeta', 'Traspaso Interno', 'Aportación', 'Aportación Lledó') THEN FALSE
        ELSE TRUE
    END AS es_movimiento_real

FROM enriched_transactions

{% if is_incremental() %}
  WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})
{% endif %}