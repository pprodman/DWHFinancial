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
    origen,
    FILE_NAME as uri_file
FROM {{ source('bronze_raw', 'bankinter_account') }}

{% if is_incremental() %}
  WHERE _FILE_NAME NOT IN (
    SELECT DISTINCT uri_file
    FROM {{ this }}
    WHERE uri_file IS NOT NULL
  )
{% endif %}