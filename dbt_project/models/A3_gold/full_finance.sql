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
    CASE
      WHEN origen = 'Card' THEN 'Tarjeta'
      WHEN origen = 'Account' THEN 'Cuenta'
    END AS origen,
    tipo_movimiento,
    {{ categorize_transaction('concepto', 'importe') }} AS categoria,
    {{ standardize_entity('concepto', 'NULL') }} AS comercio,
    ABS(importe) AS importe_absoluto,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes
FROM todos