WITH base AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

adjustments AS (
    SELECT * FROM {{ ref('manual_lledo_adjustments') }}
),

final AS (
    SELECT
        b.*,
        CASE
            WHEN a.hash_id IS NOT NULL THEN b.importe_personal - (a.adjustment_amount * 0.5)
            ELSE b.importe_personal
        END AS importe_personal_ajustado
    FROM base b
    LEFT JOIN adjustments a ON b.hash_id = a.hash_id
)

SELECT
    fecha,
    concepto,
    importe,
    entidad,
    origen,
    tipo_movimiento,
    subtipo_transaccion,
    importe_personal_ajustado AS importe_personal,
    categoria,
    comercio,
    anio,
    mes,
    anio_mes
FROM final