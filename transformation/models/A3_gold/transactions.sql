{{
  config(
    materialized = 'table'
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

adjustments AS (
    SELECT * FROM {{ ref('manual_lledo_adjustments') }}
),

final_enrichment AS (
    SELECT
        b.*,
        -- Recalculamos el importe personal SOLO si hay un ajuste manual específico (seed).
        -- Si no hay ajuste, confiamos en el cálculo estándar que ya viene de Silver.
        CASE
            -- Caso especial: Ajuste manual en la liquidación de tarjeta compartida
            -- Fórmula: (Importe Total Absoluto - Ajuste) / 2 * (-1 para que sea gasto)
            WHEN b.operativa_interna = 'Liquidación Tarjeta Compartida' AND a.hash_id IS NOT NULL
                THEN (ABS(b.importe) - a.adjustment_amount) * 0.5 * -1

            -- Por defecto, nos quedamos con lo que calculó Silver
            ELSE b.importe_personal
        END AS importe_personal_final
    FROM base b
    LEFT JOIN adjustments a ON b.hash_id = a.hash_id
)

SELECT
    -- Identificadores
    hash_id,

    -- Dimensiones Temporales (Clave para Time Series)
    fecha,
    anio,
    mes,
    anio_mes,
    trimestre,

    -- Detalles de la Transacción
    concepto,
    importe,        -- Importe TOTAL original
    comercio,       -- Nombre limpio (ej: Mercadona)

    -- Categorización Jerárquica (Tu "Santo Grial" para el Dashboard)
    grupo,          -- Nivel 1: Gastos Fijos, Variables...
    categoria,      -- Nivel 2: Supermercado, Vivienda...
    subcategoria,   -- Nivel 3: Luz, Agua, Hipoteca...

    -- Dimensiones de Origen
    entidad,        -- Banco (ej: Bankinter)
    origen,         -- Producto (ej: Card, Shared)

    -- Lógica Operativa
    operativa_interna,  -- Para saber si es Liquidación, Bizum, etc.
    es_movimiento_real, -- ¡IMPORTANTE! Usar esto como filtro en Looker (WHERE es_movimiento_real = true)
    es_compartido,     -- ¿Es un gasto compartido?
    importe_personal_final AS importe_personal -- Tu gasto real (Esta es la columna que sumarás en los gráficos)

FROM final_enrichment