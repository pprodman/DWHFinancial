{{
  config(
    materialized = 'table',
    tags = ['gold', 'financial_marts']
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

adjustments AS (
    SELECT * FROM {{ ref('manual_lledo_adjustments') }}
),

-- 1. Aplicación de Ajustes Manuales (Lledó)
logic_layer AS (
    SELECT
        b.*,
        -- Recálculo de importe personal por ajustes manuales
        CASE
            WHEN b.operativa_interna = 'Liquidación Tarjeta Compartida' AND a.hash_id IS NOT NULL
                THEN (ABS(b.importe) - a.adjustment_amount) * 0.5 * -1
            ELSE b.importe_personal
        END AS importe_personal_final
    FROM base b
    LEFT JOIN adjustments a ON b.hash_id = a.hash_id
),

-- 2. Enriquecimiento de Negocio (Lo que antes hacía la vista)
business_enrichment AS (
    SELECT
        *,
        -- A. Tipo de Movimiento (Basado en signo)
        CASE
            WHEN importe >= 0 THEN 'Ingreso'
            ELSE 'Gasto'
        END AS tipo_movimiento,

        -- B. Clasificación Operativa (Simplificación para humanos)
        CASE
            WHEN operativa_interna = 'Movimiento Regular' THEN 'Operación Normal'
            WHEN operativa_interna = 'Traspaso Interno' THEN 'Movimiento Interno'
            WHEN operativa_interna IN ('Liquidación Tarjeta', 'Liquidación Tarjeta Compartida') THEN 'Liquidación Tarjeta'
            WHEN operativa_interna LIKE '%Recarga%' THEN 'Recarga'
            WHEN operativa_interna = 'Bizum' THEN 'Bizum'
            ELSE 'Otro'
        END AS tipo_operacion,

        -- C. Rangos de Importe (Para segmentación)
        CASE
            WHEN ABS(importe) < 10 THEN '< 10€'
            WHEN ABS(importe) BETWEEN 10 AND 50 THEN '10€ - 50€'
            WHEN ABS(importe) BETWEEN 50 AND 100 THEN '50€ - 100€'
            WHEN ABS(importe) BETWEEN 100 AND 200 THEN '100€ - 200€'
            WHEN ABS(importe) BETWEEN 200 AND 500 THEN '200€ - 500€'
            ELSE '> 500€'
        END AS rango_importe,

        -- D. Lógica de Fecha de Imputación (Nóminas y Ajustes Contables)
        CASE
            -- Regla: Nóminas a final de mes -> Imputar contablemente al mes
            WHEN categoria = 'Nómina'
            THEN DATE_TRUNC(DATE_SUB(fecha, INTERVAL 16 DAY), MONTH)

            -- Regla: Ajuste Gastos Compartidos (Lledó)
            WHEN (
                ABS(importe) > 35
                AND origen = 'Account'
                AND UPPER(concepto) LIKE '%LLEDO%'
                AND importe < 0
                AND categoria = 'Transferencias Bizum'
            )
            THEN DATE_TRUNC(DATE_SUB(fecha, INTERVAL 16 DAY), MONTH)

            -- Default: Mes natural
            ELSE DATE_TRUNC(fecha, MONTH)
        END AS fecha_imputacion
    FROM logic_layer
)

-- 3. Selección Final
SELECT
    -- Identificadores
    --hash_id,

    -- Fechas
    fecha AS fecha_transaccion,
    fecha_imputacion, -- Fecha contable calculada

    -- Dimensiones Temporales Derivadas (Útiles para no depender de joins)
    --EXTRACT(YEAR FROM fecha_imputacion) AS anio_imputado,
    --EXTRACT(MONTH FROM fecha_imputacion) AS mes_imputado,
    FORMAT_DATE('%Y-%m', fecha_imputacion) AS periodo,

    concepto,
    tipo_movimiento,

    -- Métricas
    importe,
    --ABS(importe) AS importe_abs,
    es_compartido,
    importe_personal_final AS importe_personal,
    --ABS(importe_personal_final) AS importe_personal_abs,

    -- Dimensiones de Negocio
    comercio,
    grupo,
    categoria,
    subcategoria,

    -- Metadatos Operativos
    entidad,
    origen,
    operativa_interna,  -- REVISAR
    --tipo_movimiento,
    tipo_operacion, -- REVISAR
    rango_importe,

    -- Flags
    es_movimiento_real,
    --es_compartido

FROM business_enrichment