{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

SELECT
    hash_id,
    fecha,
    concepto,
    importe,
    entidad,
    origen
FROM {{ source('bronze_raw', 'bankinter_shared') }}

{% if is_incremental() %}
  WHERE hash_id NOT IN (
    SELECT hash_id
    FROM {{ this }}
  )
{% endif %}