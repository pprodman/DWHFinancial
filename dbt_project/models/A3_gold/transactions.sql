{{
  config(
    materialized = 'incremental',
    unique_key = 'hash_id',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Modelo: fct_transactions_adjusted
-- Propósito: Ajustar importe_personal considerando reembolsos posteriores de Lledó

WITH base_transactions AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

-- Separamos gastos compartidos (candidatos a ser compensados)
shared_expenses AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entidad ORDER BY fecha, hash_id) AS expense_rn
    FROM base_transactions
    WHERE subtipo_transaccion = 'Gasto Compartido (Visa Clásica)'
),

-- Separamos reembolsos de Lledó (los que compensan)
lldo_refunds AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entidad ORDER BY fecha, hash_id) AS refund_rn
    FROM base_transactions
    WHERE subtipo_transaccion = 'Traspaso Interno Lledó' 
      AND tipo_movimiento = 'Ingreso'
),

-- Creamos una tabla que empareje cada gasto con reembolsos posteriores
expense_refund_matches AS (
    SELECT 
        e.*,
        r.importe AS refund_amount,
        r.fecha AS refund_fecha,
        r.hash_id AS refund_hash_id
    FROM shared_expenses e
    LEFT JOIN lldo_refunds r
        ON e.entidad = r.entidad
        AND r.fecha >= e.fecha  -- Reembolso después del gasto
        AND r.importe <= ABS(e.importe)  -- Reembolso menor o igual al gasto
),

-- Para cada gasto, calculamos el total compensado
compensation_per_expense AS (
    SELECT 
        hash_id,
        entidad,
        importe AS original_expense,
        SUM(COALESCE(refund_amount, 0)) AS total_compensated
    FROM expense_refund_matches
    GROUP BY hash_id, entidad, importe
),

-- Combinamos con la tabla original para ajustar solo los gastos compartidos
adjusted_shared_expenses AS (
    SELECT 
        b.*,
        CASE 
            WHEN b.subtipo_transaccion = 'Gasto Compartido (Visa Clásica)' THEN
                CASE 
                    WHEN ABS(b.importe) <= COALESCE(c.total_compensated, 0) THEN 0  -- Totalmente compensado
                    ELSE (ABS(b.importe) - COALESCE(c.total_compensated, 0)) * 0.5  -- Parcialmente compensado
                END * -1  -- Negativo porque es gasto
            ELSE b.importe_personal  -- Para otros casos, mantener el valor original
        END AS importe_personal_ajustado
    FROM base_transactions b
    LEFT JOIN compensation_per_expense c
        ON b.hash_id = c.hash_id
),

-- Selección final
final_transactions AS (
    SELECT 
        --hash_id,
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
    FROM adjusted_shared_expenses
)

SELECT * FROM final_transactions

{% if is_incremental() %}
WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})
{% endif %}