{{
  config(
    materialized = 'table',  -- O 'incremental' si prefieres (ver nota abajo)
    partition_by = {'field': 'fecha', 'data_type': 'date'},
    cluster_by = ['entidad', 'categoria_movimiento', 'tipo_movimiento']
  )
}}

WITH bankinter AS (
    SELECT * FROM {{ ref('bankinter') }}
),

--sabadell AS (
--    SELECT * FROM {{ ref('slv_sabadell__global') }}
--),


bancos AS (
    SELECT * FROM bankinter
    --UNION ALL
    --SELECT * FROM sabadell
)

SELECT
    transaccion_id,
    fecha,
    concepto,
    importe,
    entidad,           -- Banco (Bankinter, Sabadell, etc.)
    origen,            -- Fuente original (cuenta, tarjeta)
    tipo_movimiento,   -- Ingreso/Gasto (calculado en Silver)

    -- ✅ Campo nuevo: Categoría inteligente
    {{ categorizar_movimiento('concepto', 'importe') }} AS categoria,

    -- ✅ Campos de análisis
    ABS(importe) AS importe_absoluto,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS periodo

FROM bancos