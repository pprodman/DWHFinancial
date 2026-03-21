{{ config(
    materialized='view',
    description='Capa de presentación sobre la tabla Gold.',
    tags=['reporting']
) }}

SELECT
    * FROM {{ ref('transactions') }}
WHERE es_movimiento_real = TRUE