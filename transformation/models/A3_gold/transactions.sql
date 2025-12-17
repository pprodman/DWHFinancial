{{ config(
    materialized = 'view',
    tags = ['gold', 'financial_marts']
) }}

WITH base AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

adjustments AS (
    SELECT * FROM {{ ref('adjustments') }}
),

-- 1. CAPA LÓGICA: Aplicación de Ajustes Manuales
-- Preferimos ajustar la transacción original en lugar de la liquidación total
logic_layer AS (
    SELECT
        b.*,

        -- A. Ajuste de Importe Personal (Granularidad Transaccional)
        CASE
            -- Caso: Gasto de tu pareja pagado con cuenta común -> Tu coste es 0
            WHEN a.adjustment_type = 'PARTNER_EXPENSE' THEN 0

            -- Caso: Gasto tuyo pagado con cuenta común -> Tu coste es 100%
            WHEN a.adjustment_type = 'MY_EXPENSE' THEN b.importe

            -- Caso 3: Ajuste manual PARCIAL (Cálculo del Remanente Compartido)
            -- Lógica: En el Excel indicas la parte EXCLUSIVA de Lledó (adjustment_amount).
            -- Tu parte es la mitad de lo que sobra (la diferencia).
            -- Fórmula: (Total - Parte de Lledó) / 2
            WHEN a.adjustment_amount IS NOT NULL
                THEN (ABS(b.importe) - ABS(a.adjustment_amount)) * 0.5 * (CASE WHEN b.importe < 0 THEN -1 ELSE 1 END)

            -- Default: Lógica estándar calculada en Silver
            ELSE b.importe_personal
        END AS importe_personal_final,

        -- B. Reclasificación de flag compartido
        CASE
            WHEN a.adjustment_type IN ('PARTNER_EXPENSE', 'MY_EXPENSE') THEN FALSE
            ELSE b.es_compartido
        END AS es_compartido_final

    FROM base b
    LEFT JOIN adjustments a ON b.hash_id = a.hash_id
),

-- 2. CAPA DE ENRIQUECIMIENTO DE NEGOCIO (Tu aportación clave)
business_enrichment AS (
    SELECT
        *,
        -- A. Tipo de Movimiento (Visual)
        CASE
            WHEN importe >= 0 THEN 'Ingreso'
            ELSE 'Gasto'
        END AS tipo_movimiento,

        -- B. Clasificación Operativa Simplificada
        CASE
            WHEN operativa_interna = 'Movimiento Regular' THEN 'Operación Normal'
            WHEN operativa_interna IN ('Traspaso Interno', 'Aportación Mensual', 'Aportación Extra') THEN 'Movimiento Interno'
            WHEN operativa_interna IN ('Liquidación Tarjeta', 'Liquidación Tarjeta Compartida') THEN 'Liquidación Tarjeta'
            WHEN operativa_interna = 'Bizum' THEN 'Bizum'
            ELSE 'Otro'
        END AS tipo_operacion,

        -- C. Rangos de Importe (Para histogramas)
        CASE
            WHEN ABS(importe) < 10 THEN '< 10€'
            WHEN ABS(importe) BETWEEN 10 AND 50 THEN '10€ - 50€'
            WHEN ABS(importe) BETWEEN 50 AND 100 THEN '50€ - 100€'
            WHEN ABS(importe) BETWEEN 100 AND 200 THEN '100€ - 200€'
            WHEN ABS(importe) BETWEEN 200 AND 500 THEN '200€ - 500€'
            ELSE '> 500€'
        END AS rango_importe,

        -- D. Lógica de Fecha de Imputación (Contabilidad de Hogar)
        CASE
            -- 1. Nóminas: Si entran a fin de mes (ej: día 28), cuentan para ese mes.
            --    Si entran a principio (ej: día 2), cuentan para el mes anterior.
            --    El truco DATE_SUB(..., 16 DAY) centra la fecha a mitad del mes contable.
            WHEN categoria = 'Nómina'
            THEN DATE_TRUNC(DATE_SUB(fecha, INTERVAL 16 DAY), MONTH)

            -- 2. Ajustes Lledó (Gastos compartidos cobrados tarde)
            WHEN (
                ABS(importe) > 35
                AND origen = 'Account'
                AND UPPER(concepto) LIKE '%LLEDO%'
                AND importe < 0
                AND categoria = 'Transferencias Bizum'
            )
            THEN DATE_TRUNC(DATE_SUB(fecha, INTERVAL 16 DAY), MONTH)

            -- 3. Default: Mes natural de la transacción
            ELSE DATE_TRUNC(fecha, MONTH)
        END AS fecha_imputacion

    FROM logic_layer
)

-- 3. SELECCIÓN FINAL
SELECT
    -- Identificadores
    hash_id,

    -- Fechas
    fecha AS fecha_transaccion,
    fecha_imputacion,
    FORMAT_DATE('%Y-%m', fecha_imputacion) AS periodo,
    EXTRACT(YEAR FROM fecha_imputacion) AS anio,

    -- Métricas Principales
    concepto,
    importe,
    importe_personal_final AS importe_personal,

    -- Flags y Estados
    es_compartido_final AS es_compartido,
    es_movimiento_real,

    -- Dimensiones de Negocio
    comercio,
    grupo,
    categoria,
    subcategoria,

    -- Dimensiones Operativas
    entidad,
    origen,
    operativa_interna,
    tipo_movimiento,
    tipo_operacion,
    rango_importe

FROM business_enrichment