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
  tipo_movimiento,
  categoria,
  EXTRACT(YEAR FROM fecha) AS anio,
  EXTRACT(MONTH FROM fecha) AS mes,
  FORMAT_DATE('%Y-%m', fecha) AS anio_mes
FROM
  {{ ref('bronze_transactions') }}
WHERE
  LOWER(concepto) NOT LIKE '%tarjeta diamond%'

{% if is_incremental() %}
  -- Filtra solo los datos nuevos de la capa bronze
  WHERE fecha > (SELECT MAX(fecha) FROM {{ this }})
{% endif %}
