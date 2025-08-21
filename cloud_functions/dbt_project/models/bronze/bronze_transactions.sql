{{
  config(
    materialized='incremental',
    unique_key='transaccion_id'
  )
}}

SELECT
  transaccion_id,
  fecha,
  concepto,
  importe,
  origen,
  entidad,
  CASE WHEN importe < 0 THEN 'Gasto' ELSE 'Ingreso' END AS tipo_movimiento,
  CASE
    WHEN entidad = 'revolut' AND LOWER(concepto) LIKE 'top-up by%' THEN 'Recarga de Cuenta'
    WHEN entidad = 'revolut' AND LOWER(concepto) LIKE 'exchange%' THEN 'Cambio de Divisa'
    WHEN LOWER(concepto) LIKE '%nomina%' THEN 'Nómina'
    WHEN LOWER(concepto) LIKE '%bizum%' AND importe > 0 THEN 'Ingresos Bizum'
    WHEN LOWER(concepto) LIKE '%alquiler%' OR LOWER(concepto) LIKE '%hipoteca%' THEN 'Vivienda'
    WHEN LOWER(concepto) LIKE '%recibo%digi%' OR LOWER(concepto) LIKE '%recibo%movistar%' THEN 'Internet y Móvil'
    WHEN LOWER(concepto) LIKE '%mercadona%' OR LOWER(concepto) LIKE '%consum%' THEN 'Supermercado'
    WHEN LOWER(concepto) LIKE '%amazon%' OR LOWER(concepto) LIKE '%amzn%' THEN 'Compras Online'
    WHEN LOWER(concepto) LIKE '%restaurante%' OR LOWER(concepto) LIKE '%glovo%' THEN 'Restaurantes y Comida'
    WHEN LOWER(concepto) LIKE '%bizum%' AND importe < 0 THEN 'Pagos Bizum'
    ELSE 'Otros'
  END AS categoria
FROM (
  SELECT * FROM {{ source('financial_raw', 'bankinter_account') }}
  UNION ALL
  SELECT * FROM {{ source('financial_raw', 'revolut_account') }}
) AS raw_data

{% if is_incremental() %}
  -- Este filtro incremental evita el reprocesamiento de datos ya cargados
  WHERE fecha > (SELECT MAX(fecha) FROM {{ this }})
{% endif %}
