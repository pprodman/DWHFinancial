{{
  config(
    materialized='incremental',
    unique_key='transaction_id'  -- ¡Mucho más limpio y seguro!
  )
}}

SELECT
    -- Los datos ya vienen limpios y con los tipos correctos desde la ingesta.
    -- El SQL solo se encarga de seleccionar y añadir metadatos de carga.
    transaction_id,
    fecha,
    concepto,
    importe,
    banco,
    tipo_cuenta,
    _FILE_NAME AS archivo_origen,
    CURRENT_TIMESTAMP() AS fecha_carga
    
FROM {{ source('gcs_raw_source', 'movimientos_raw_csv') }}

{% if is_incremental() %}
  -- Filtra para procesar solo los registros de archivos nuevos
  WHERE _FILE_NAME NOT IN (
    SELECT DISTINCT archivo_origen FROM {{ this }}
  )
{% endif %}