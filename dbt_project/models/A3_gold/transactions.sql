-- dbt_project/models/A3_gold/transactions.sql

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

-- Emparejamos cada reembolso con el gasto más reciente que no se ha compensado completamente
-- Usamos una lógica de "acumulado" para hacer el emparejamiento
expenses_with_running_total AS (
    SELECT 
        *,
        SUM(ABS(importe)) OVER (PARTITION BY entidad ORDER BY fecha, hash_id ROWS UNBOUNDED PRECEDING) AS running_expense_total
    FROM shared_expenses
),

refunds_with_running_total AS (
    SELECT 
        *,
        SUM(importe) OVER (PARTITION BY entidad ORDER BY fecha, hash_id ROWS UNBOUNDED PRECEDING) AS running_refund_total
    FROM lldo_refunds
),

-- Para cada gasto, calculamos cuánto ha sido compensado
expenses_with_compensation AS (
    SELECT 
        e.*,
        COALESCE(
            (SELECT SUM(r.importe) 
             FROM refunds_with_running_total r 
             WHERE r.entidad = e.entidad 
               AND r.fecha >= e.fecha 
               AND r.running_refund_total <= e.running_expense_total + (SELECT COALESCE(MAX(running_refund_total), 0) FROM refunds_with_running_total r2 WHERE r2.entidad = e.entidad AND r2.fecha < e.fecha)
            ), 
            0
        ) AS total_compensated
    FROM expenses_with_running_total e
),

-- Ajustamos el importe_personal para gastos compartidos
adjusted_shared_expenses AS (
    SELECT 
        *,
        CASE 
            WHEN ABS(importe) <= total_compensated THEN 0  -- Totalmente compensado
            ELSE (ABS(importe) - total_compensated) * 0.5  -- Parcialmente compensado
        END * -1 AS importe_personal_ajustado  -- Negativo porque es gasto
    FROM expenses_with_compensation
),

-- Para todos los demás registros, mantenemos el importe_personal original
other_transactions AS (
    SELECT 
        *,
        importe_personal AS importe_personal_ajustado
    FROM base_transactions
    WHERE subtipo_transaccion != 'Gasto Compartido (Visa Clásica)'
),

-- Combinamos todos los registros
final_transactions AS (
    SELECT 
        hash_id,
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
    
    UNION ALL
    
    SELECT 
        hash_id,
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
    FROM other_transactions
)

SELECT * FROM final_transactions

{% if is_incremental() %}
WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})
{% endif %}