{{
  config(
    materialized = 'incremental',
    unique_key = 'transaccion_id',
    partition_by = {'field': 'fecha', 'data_type': 'date'},
    cluster_by = ['entidad', 'origen', 'tipo_movimiento'],
    on_schema_change = 'sync_all_columns'
  )
}}

WITH cuenta AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    WHERE concepto NOT LIKE '%RECIBO PLATINUM%'
),

tarjeta AS (
    SELECT * FROM {{ ref('bankinter_card') }}
),

unificado AS (
    SELECT * FROM cuenta
    UNION ALL
    SELECT * FROM tarjeta
)

SELECT
    transaccion_id,
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
  WHERE transaccion_id NOT IN (
    SELECT transaccion_id
    FROM {{ this }}
  )
{% endif %}