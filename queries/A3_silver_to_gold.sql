-- Paso 3: De Silver a Gold
CREATE OR REPLACE TABLE `@project_id.@gold_dataset.@gold_table` AS
SELECT
  anio_mes,
  categoria,
  SUM(IF(tipo_movimiento = 'Ingreso', importe, 0)) AS total_ingresos,
  SUM(IF(tipo_movimiento = 'Gasto', importe, 0)) AS total_gastos,
  SUM(importe) AS balance,
  COUNT(transaccion_id) AS numero_transacciones
FROM
  `@project_id.@silver_dataset.@silver_table`
GROUP BY
  anio_mes,
  categoria
ORDER BY
  anio_mes DESC,
  balance DESC;