{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH
cuenta AS (
    SELECT * FROM {{ ref('bankinter_account') }}
    WHERE
        NOT (
            fecha >= '2024-05-01'
            AND (UPPER(concepto) LIKE 'TRANSF INTERNA%') -- excluir transferencias internas
            )
        AND UPPER(concepto) NOT LIKE '%RECIBO PLATINUM%' -- excluir recibos de tarjeta platinum
),

tarjeta AS (
    SELECT * FROM {{ ref('bankinter_card') }}
),

cuenta_comun AS (
    SELECT
        hash_id,
        fecha,
        concepto,
        importe * 0.5 AS importe, -- mi parte del gasto
        entidad,
        origen
    FROM {{ ref('bankinter_shared') }}
    WHERE
        NOT (
            ABS(importe) BETWEEN 490 AND 510
            AND UPPER(concepto) LIKE 'TRANS%'
        )
),

unificado AS (
    SELECT * FROM cuenta
    UNION ALL
    SELECT * FROM tarjeta
    UNION ALL
    SELECT * FROM cuenta_comun
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