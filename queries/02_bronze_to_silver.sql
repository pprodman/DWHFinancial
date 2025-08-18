MERGE `dwhfinancial.dwh_03_silver.transactions` AS T
USING (
  SELECT
    transaccion_id,
    fecha,
    concepto,
    importe,
    origen,
    entidad,
    tipo_movimiento,
    categoria,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes
  FROM
    `dwhfinancial.dwh_02_bronze.transactions`
  WHERE
    LOWER(concepto) NOT LIKE '%tarjeta diamond%'
) AS S
ON T.transaccion_id = S.transaccion_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (transaccion_id, fecha, concepto, importe, origen, entidad, tipo_movimiento, categoria, anio, mes, anio_mes)
  VALUES (transaccion_id, fecha, concepto, importe, origen, entidad, tipo_movimiento, categoria, anio, mes, anio_mes);