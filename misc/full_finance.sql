-- dbt_project/models/A3_gold/full_finance.sql

{{
  config(
    materialized = 'table'
  )
}}

WITH
bankinter AS (
    SELECT * FROM {{ ref('bankinter') }}
    WHERE UPPER(concepto) NOT LIKE '%REVOLUT%' -- excluir recargas de Revolut
),

revolut AS (
    SELECT * FROM {{ ref('revolut') }}
    WHERE UPPER(concepto) NOT LIKE '%RECARGA%'
      AND UPPER(concepto) NOT LIKE '%PAGO DE PABLO%'
),

todos AS (
    SELECT * FROM bankinter
    UNION ALL
    SELECT * FROM revolut
)

SELECT
    fecha,
    concepto,
    importe,
    entidad,
    CASE
      WHEN origen = 'Card' THEN 'Tarjeta'
      WHEN origen = 'Account' THEN 'Cuenta'
      WHEN origen = 'Shared' THEN 'Compartida'
      ELSE 'Otro'
    END AS origen,
    tipo_movimiento,
    {{ categorize_transaction('concepto', 'importe') }} AS categoria,
    {{ standardize_entity('concepto', 'NULL') }} AS comercio,
    ABS(importe) AS importe_absoluto,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes
FROM todos