{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH raw_source AS (
    SELECT
        SAFE_CAST(fecha AS DATE) as fecha,
        TRIM(concepto) as concepto,
        SAFE_CAST(importe AS FLOAT64) as importe
    FROM {{ source('bronze_raw', 'cash') }}
    WHERE fecha IS NOT NULL AND importe IS NOT NULL
),

final AS (
    SELECT
        TO_HEX(SHA256(CONCAT(
            CAST(fecha AS STRING), '-',
            LOWER(TRIM(concepto)), '-',
            FORMAT('%.2f', importe)
        ))) as hash_id,
        fecha,
        concepto,
        importe,
        'Caja' as entidad,
        'Cash' as origen
    FROM raw_source
)

SELECT * FROM final

{% if is_incremental() %}
  WHERE hash_id NOT IN (
    SELECT hash_id FROM {{ this }}
  )
{% endif %}