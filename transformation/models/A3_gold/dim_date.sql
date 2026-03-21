{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

WITH date_spine AS (
  -- Generamos fechas desde 2019 hasta 2030 (cubre histórico y futuro previsible)
  SELECT day
  FROM UNNEST(
      GENERATE_DATE_ARRAY(DATE('2019-01-01'), DATE('2030-12-31'), INTERVAL 1 DAY)
  ) AS day
)

SELECT
  -- Identificadores
  FORMAT_DATE('%Y%m%d', day) AS date_id,
  day AS fecha,

  -- Año y Mes
  EXTRACT(YEAR FROM day) AS anio,
  EXTRACT(MONTH FROM day) AS mes_num,

  -- Nombres en Español (Hardcoded para evitar problemas de configuración regional en BQ)
  CASE EXTRACT(MONTH FROM day)
    WHEN 1 THEN 'Enero'
    WHEN 2 THEN 'Febrero'
    WHEN 3 THEN 'Marzo'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Mayo'
    WHEN 6 THEN 'Junio'
    WHEN 7 THEN 'Julio'
    WHEN 8 THEN 'Agosto'
    WHEN 9 THEN 'Septiembre'
    WHEN 10 THEN 'Octubre'
    WHEN 11 THEN 'Noviembre'
    WHEN 12 THEN 'Diciembre'
  END AS mes_nombre,

  FORMAT_DATE('%Y-%m', day) AS period, -- Útil para ejes de gráficos (2024-01)

  -- Trimestres
  EXTRACT(QUARTER FROM day) AS trimestre,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM day) AS STRING), '-', CAST(EXTRACT(YEAR FROM day) AS STRING)) as trimestre_anio,

  -- Semanas
  EXTRACT(ISOWEEK FROM day) AS semana_anio,

  -- Días
  EXTRACT(DAYOFWEEK FROM day) AS dia_semana_num, -- 1=Domingo, 2=Lunes... en BigQuery
  CASE EXTRACT(DAYOFWEEK FROM day)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Lunes'
    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles'
    WHEN 5 THEN 'Jueves'
    WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
  END AS dia_semana_nombre,

  -- Banderas Booleanas
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM day) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS es_fin_de_semana,

  -- Día actual (útil para filtros dinámicos: "fecha <= hoy")
  CASE
    WHEN day = CURRENT_DATE() THEN TRUE
    ELSE FALSE
  END AS es_hoy

FROM date_spine