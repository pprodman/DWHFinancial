{{ config(
    materialized = 'view',
    tags = ['marts', 'hr']
) }}

WITH base_nominas AS (
    SELECT
        fecha_imputacion,
        periodo,
        anio,
        comercio AS empresa,
        SUM(importe) AS importe_neto
    FROM {{ ref('transactions') }}
    -- Filtramos por categoría. Asegúrate que coincide con tu seeds/master_mapping
    WHERE subcategoria = 'Salario Base'
    GROUP BY 1, 2, 3, 4
),

-- 1. Generamos el "Spine" usando la tabla maestra de fechas (dim_dates)
date_spine AS (
    SELECT
        fecha AS mes_fecha
    FROM {{ ref('dim_date') }}
    WHERE
        -- Filtramos solo el primer día de cada mes para tener granularidad mensual
        EXTRACT(DAY FROM fecha) = 1
        -- Acotamos el inicio: Desde la primera nómina (o 2020 por defecto)
        AND fecha >= COALESCE(
            (SELECT MIN(fecha_imputacion) FROM base_nominas),
            DATE('2020-01-01')
        )
        -- Acotamos el fin: Hasta el MES ANTERIOR al actual para evitar caídas de media por mes incompleto
        AND fecha <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
),

-- 2. Densificación: Unimos calendario con ingresos
-- Agrupamos por mes sin importar la empresa para calcular la capacidad de ingreso total
monthly_global_income AS (
    SELECT
        ds.mes_fecha,
        -- Si no hay nómina ese mes, ponemos 0
        COALESCE(SUM(bn.importe_neto), 0) as total_mes_global
    FROM date_spine ds
    LEFT JOIN base_nominas bn ON ds.mes_fecha = bn.fecha_imputacion
    GROUP BY 1
),

-- 3. Cálculo de Métricas Globales (Incluyendo los ceros)
metrics_global AS (
    SELECT
        mes_fecha,
        total_mes_global,
        -- La ventana de 12 meses incluye los meses con 0, corrigiendo el sesgo
        AVG(total_mes_global) OVER (
            ORDER BY mes_fecha
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS media_movil_12m_real
    FROM monthly_global_income
)

-- 4. Unión Final (INVERTIDA para mostrar meses vacíos)
-- Usamos metrics_global como base para que aparezcan los meses donde no cobraste
SELECT
    mg.mes_fecha AS fecha,
    FORMAT_DATE('%Y-%m', mg.mes_fecha) AS periodo,
    EXTRACT(YEAR FROM mg.mes_fecha) AS anio,

    -- Si no hubo nómina, mostramos etiqueta informativa
    COALESCE(bn.empresa, 'Sin Ingresos') AS empresa,

    -- Importe del mes (0 si no hubo cobro)
    COALESCE(bn.importe_neto, 0) AS importe_neto,

    -- Acumulado anual (Running Total)
    -- Calculado sobre la vista densa para que sume correctamente a lo largo del año
    SUM(COALESCE(bn.importe_neto, 0)) OVER (
        PARTITION BY EXTRACT(YEAR FROM mg.mes_fecha)
        ORDER BY mg.mes_fecha
    ) AS acumulado_anual,

    -- Media Móvil Densificada (Ya calculada correctamente con los ceros)
    ROUND(mg.media_movil_12m_real, 2) AS media_movil_12m

FROM metrics_global mg
LEFT JOIN base_nominas bn ON mg.mes_fecha = bn.fecha_imputacion

ORDER BY mg.mes_fecha DESC