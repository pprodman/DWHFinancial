{{ config(
    materialized = 'view',
    tags = ['marts', 'capex']
) }}


SELECT
    fecha_transaccion AS fecha,
    periodo,
    concepto,
    importe,
    ABS(importe) AS importe_abs,
    comercio,
    categoria,
    subcategoria,
    FROM {{ ref('transactions') }}
    WHERE grupo = 'CAPEX' OR subcategoria = 'Hipoteca'