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
    entidad,
    origen,
    _FILE_NAME AS archivo_origen -- ✅ Para lógica incremental
FROM {{ source('gcs_raw_source', 'movimientos_raw_jsonl') }}

{% if is_incremental() %}
  WHERE _FILE_NAME NOT IN (
    SELECT DISTINCT archivo_origen FROM {{ this }}
  )
{% endif %}