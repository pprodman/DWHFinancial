{{
  config(
    materialized = 'table',
    partition_by = {'field': 'fecha', 'data_type': 'date'},
    cluster_by = ['entidad', 'categoria']
  )
}}

WITH bankinter AS (
    SELECT * FROM {{ ref('bankinter') }}
),

todos AS (
    SELECT * FROM bankinter
)

SELECT
    transaccion_id,
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