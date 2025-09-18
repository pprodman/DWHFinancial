{{
  config(
    materialized='incremental',
    unique_key='transaccion_id'
  )
}}

SELECT
    transaccion_id,
    CAST(fecha AS DATE) AS fecha,
    concepto,
    CAST(importe AS NUMERIC) AS importe,
    entidad AS banco,
    origen AS tipo_cuenta,
    _FILE_NAME AS archivo_origen
FROM {{ source('gcs_raw_source', 'movimientos_raw_jsonl') }}

{% if is_incremental() %}
  WHERE _FILE_NAME NOT IN (
    SELECT DISTINCT archivo_origen FROM {{ this }} WHERE archivo_origen IS NOT NULL
  )
{% endif %}