{{
  config(
    materialized = 'table'
  )
}}

WITH bankinter AS (
    SELECT * FROM {{ ref('bankinter') }}
),

todos AS (
    SELECT * FROM bankinter
)

SELECT
    fecha,
    concepto,
    importe,
    entidad,
    origen,
    tipo_movimiento,
    {{ categorizar_movimiento('concepto', 'importe') }} AS categoria,
    ABS(importe) AS importe_absoluto,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes
FROM todos