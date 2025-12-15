-- depends_on: {{ ref('bizum_directory') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
) }}

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
    SELECT * FROM {{ ref('cash') }}
),

-- 2. Enriquecimiento y Cálculo de Operativa
enriched_stage AS (
    SELECT
        hash_id,
        CAST(fecha AS DATE) AS fecha,
        concepto,
        importe,
        entidad,
        origen,

        -- Llamada a la macro maestra (Devuelve string tipo: "Grupo|Cat|Subcat|Nombre")
        {{ categorize_transaction('concepto', 'COALESCE(importe, 0)') }} as _cat_string,

        -- Detectar operativa interna
        CASE
            -- Liquidaciones de tarjeta
            WHEN entidad = 'Bankinter' AND origen = 'Account' AND UPPER(concepto) LIKE '%RECIBO PLATINUM%' THEN 'Liquidación Tarjeta'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%RECIBO VISA CLASICA%' THEN 'Liquidación Tarjeta Compartida'

            -- Aportaciones (Reglas por importe exacto)
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%PABLO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación'
            WHEN entidad = 'Bankinter' AND origen = 'Shared' AND UPPER(concepto) LIKE '%LLEDO%' AND ABS(COALESCE(importe, 0)) IN (500, 750) THEN 'Aportación Lledó'

            -- Movimientos internos
            WHEN UPPER(concepto) LIKE '%TRASPASO%' OR UPPER(concepto) LIKE '%TRANSFERENCIA INTERNA%' THEN 'Traspaso Interno'
            WHEN UPPER(concepto) LIKE '%BIZUM%' THEN 'Bizum'
            ELSE 'Movimiento Regular'
        END AS operativa_interna
    FROM all_sources
),

-- 3. Desempaquetado (Split)
unpacked_stage AS (
    SELECT
        *,
        -- Usamos SAFE_OFFSET para evitar errores si la cadena no tiene suficientes partes
        SPLIT(_cat_string, '|')[SAFE_OFFSET(0)] AS cat_grupo,
        SPLIT(_cat_string, '|')[SAFE_OFFSET(1)] AS cat_categoria,
        SPLIT(_cat_string, '|')[SAFE_OFFSET(2)] AS cat_subcategoria,
        SPLIT(_cat_string, '|')[SAFE_OFFSET(3)] AS cat_entity_name
    FROM enriched_stage
)

-- 4. Proyección Final
SELECT
    hash_id,
    fecha,
    -- Dimensiones Temporales
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes,
    EXTRACT(QUARTER FROM fecha) AS trimestre,

    concepto,
    importe,
    entidad,
    origen,
    operativa_interna,

    -- Dimensiones Jerárquicas limpias
    COALESCE(cat_grupo, 'Sin Clasificar') AS grupo,
    COALESCE(cat_categoria, 'Sin Clasificar') AS categoria,
    COALESCE(cat_subcategoria, 'Sin Clasificar') AS subcategoria,

    -- NOMBRE LIMPIO DEL COMERCIO / ENTIDAD (Lógica COALESCE de Prioridad)
    COALESCE(
        -- 1. PRIMERA OPCIÓN: Si es Bizum, intentamos sacar el nombre del directorio
        CASE
            WHEN (operativa_interna = 'Bizum' OR UPPER(concepto) LIKE '%BIZUM%' OR UPPER(concepto) LIKE '%TRANSF%')
            THEN {{ extract_bizum_name('concepto') }}
            ELSE NULL
        END,

        -- 2. SEGUNDA OPCIÓN: Master Mapping, pero filtramos nombres genéricos "basura"
        --    Si cat_entity_name es "Bizum", "Transferencia" o "Desconocido", lo convertimos a NULL para seguir buscando
        NULLIF(NULLIF(NULLIF(NULLIF(cat_entity_name, 'Desconocido'), 'Bizum'), 'Bizum Enviado'), 'Transferencia'),

        -- 3. TERCERA OPCIÓN: Si es Bizum y falló el directorio, usamos limpieza automática
        CASE
            WHEN (operativa_interna = 'Bizum' OR UPPER(concepto) LIKE '%BIZUM%')
            THEN {{ clean_bizum_name('concepto') }}
            ELSE NULL
        END,

        -- 4. ÚLTIMA OPCIÓN: Nombre original limpio
        INITCAP(TRIM(concepto))
    ) AS comercio,

    -- Lógica de Gasto Compartido
    CASE
        WHEN origen = 'Shared' AND operativa_interna NOT IN ('Aportación', 'Aportación Lledó') THEN TRUE
        ELSE FALSE
    END AS es_compartido,

    -- Importe Personal (Regla del 50%)
    CASE
        WHEN origen = 'Shared' AND operativa_interna NOT IN ('Aportación', 'Aportación Lledó') THEN importe * 0.5
        WHEN operativa_interna IN ('Aportación', 'Aportación Lledó') THEN 0
        ELSE importe
    END AS importe_personal,

    -- Flag Movimiento Real
    CASE
        WHEN operativa_interna = 'Liquidación Tarjeta Compartida' THEN TRUE
        WHEN operativa_interna IN ('Liquidación Tarjeta', 'Traspaso Interno', 'Aportación', 'Aportación Lledó') THEN FALSE
        ELSE TRUE
    END AS es_movimiento_real

FROM unpacked_stage

{% if is_incremental() %}
  WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})
{% endif %}