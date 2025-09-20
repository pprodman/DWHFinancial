{{
  config(
    materialized = 'incremental',
    unique_key = 'transaccion_id',
    on_schema_change = 'sync_all_columns'
  )
}}

SELECT
    transaccion_id,
    fecha,
    concepto,
    importe,
    entidad,
    origen
FROM {{ source('bronze_raw', 'bankinter_account') }}

{% if is_incremental() %}
  WHERE transaccion_id NOT IN (
    SELECT transaccion_id
    FROM {{ this }}
  )
{% endif %}