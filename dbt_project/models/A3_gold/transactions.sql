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
    SELECT * FROM {{ ref('fct_transactions') }}  -- Tu modelo actual
),

-- Si es incremental, solo procesamos nuevos registros
filtered_base AS (
    {% if is_incremental() %}
    SELECT *
    FROM base_transactions
    WHERE hash_id NOT IN (
        SELECT hash_id FROM {{ this }}
    )
    {% else %}
    SELECT *
    FROM base_transactions
    {% endif %}
),

-- Ordenamos TODAS las transacciones por fecha y hash_id para secuenciación
ordered_all AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY entidad ORDER BY fecha, hash_id) AS rn
    FROM filtered_base
),

-- Para cada transacción, miramos los próximos 30 días en busca de reembolsos de Lledó
-- Incluimos también los reembolsos existentes en la tabla destino si es incremental
with_possible_refunds AS (
    {% if is_incremental() %}
    -- Si es incremental, incluimos también los reembolsos que ya están en la tabla destino
    -- para que puedan ser usados para ajustar nuevos gastos
    WITH all_relevant_data AS (
        SELECT *, FALSE AS is_new
        FROM {{ this }}
        WHERE UPPER(concepto) LIKE '%LLEDO%' AND importe > 0 AND tipo_movimiento = 'Ingreso'
          AND fecha >= (SELECT DATE_SUB(MAX(fecha), INTERVAL 31 DAY) FROM {{ this }})
        UNION ALL
        SELECT *, TRUE AS is_new
        FROM ordered_all
    ),
    -- Ahora aplicamos la lógica de búsqueda de reembolsos
    SELECT
        t.*,
        ARRAY_AGG(
            CASE 
                WHEN UPPER(concepto) LIKE '%LLEDO%' AND importe > 0 AND tipo_movimiento = 'Ingreso'
                THEN STRUCT(importe AS refund_amount, fecha AS refund_date, hash_id AS refund_hash)
            END
        ) OVER (
            PARTITION BY entidad 
            ORDER BY fecha
            RANGE BETWEEN CURRENT ROW AND INTERVAL 30 DAY FOLLOWING
        ) AS future_refunds_array
    FROM all_relevant_data t
    WHERE t.is_new  -- Solo procesamos los nuevos registros
    {% else %}
    -- Si no es incremental, solo usamos los datos de la fuente
    SELECT
        t.*,
        ARRAY_AGG(
            CASE 
                WHEN UPPER(concepto) LIKE '%LLEDO%' AND importe > 0 AND tipo_movimiento = 'Ingreso'
                THEN STRUCT(importe AS refund_amount, fecha AS refund_date, hash_id AS refund_hash)
            END
        ) OVER (
            PARTITION BY entidad 
            ORDER BY fecha
            RANGE BETWEEN CURRENT ROW AND INTERVAL 30 DAY FOLLOWING
        ) AS future_refunds_array
    FROM ordered_all t
    {% endif %}
),

-- Ajustamos el importe_personal SOLO para gastos compartidos
adjusted_transactions AS (
    SELECT
        *,
        CASE
            -- Solo ajustamos gastos compartidos (Visa Clásica o Shared genéricos)
            WHEN subtipo_transaccion = 'Gasto Compartido (Visa Clásica)' 
                 OR (origen = 'Shared' AND tipo_movimiento = 'Gasto' AND subtipo_transaccion = 'Gasto/Ingreso Regular')
            THEN (
                -- Importe base compartido (50%)
                importe * 0.5 +
                -- Sumamos la parte del reembolso que corresponde a tu mitad
                (
                    SELECT COALESCE(SUM(refund.refund_amount * 0.5), 0)
                    FROM UNNEST(future_refunds_array) AS refund
                    WHERE refund.refund_amount IS NOT NULL
                      -- Opcional: solo reembolsos razonables (ej. < 90% del gasto original)
                      AND refund.refund_amount <= ABS(importe) * 0.9
                )
            )
            ELSE importe_personal
        END AS importe_personal_ajustado
    FROM with_possible_refunds
)

SELECT
    -- Seleccionamos todas las columnas excepto hash_id y importe_personal original
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
FROM adjusted_transactions