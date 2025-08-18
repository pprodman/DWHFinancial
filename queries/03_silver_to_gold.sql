CREATE OR REPLACE TABLE `dwhfinancial.dwh_04_gold.monthly_summary` AS
SELECT
  anio_mes,
  categoria,
  SUM(IF(tipo_movimiento = 'Ingreso', importe, 0)) AS total_ingresos,
  SUM(IF(tipo_movimiento = 'Gasto', importe, 0)) AS total_gastos,
  SUM(importe) AS balance,
  COUNT(transaccion_id) AS numero_transacciones
FROM
  `dwhfinancial.dwh_03_silver.transactions`
GROUP BY
  anio_mes,
  categoria
ORDER BY
  anio_mes DESC,
  balance DESC;