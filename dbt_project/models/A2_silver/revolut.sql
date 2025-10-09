{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH
cuenta AS (
    SELECT * FROM {{ ref('revolut_account') }}
),

unificado AS (
    SELECT * FROM cuenta
)

SELECT
    hash_id,
    CAST(fecha AS DATE) AS fecha,
    concepto,
    importe,
    entidad,
    origen,
    CASE
        WHEN importe > 0 THEN 'Ingreso'
        WHEN importe < 0 THEN 'Gasto'
        ELSE 'Neutro'
    END AS tipo_movimiento
FROM unificado

{% if is_incremental() %}
  WHERE hash_id NOT IN (
    SELECT hash_id
    FROM {{ this }}
  )
{% endif %}