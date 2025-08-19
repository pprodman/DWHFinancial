MERGE `@project_id.@bronze_dataset.@bronze_table` AS T
USING (
  -- Unimos los datos de todas las tablas externas que hemos creado
  SELECT * FROM `@project_id.@raw_dataset.bankinter_account`
  UNION ALL
  SELECT * FROM `@project_id.@raw_dataset.revolut_account`
  -- Añadir aquí futuras fuentes con UNION ALL
) AS S
ON T.transaccion_id = S.transaccion_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (transaccion_id, fecha, concepto, importe, origen, entidad, tipo_movimiento, categoria)
  VALUES (
    transaccion_id,
    fecha,
    concepto,
    importe,
    origen,
    entidad,
    CASE WHEN importe < 0 THEN 'Gasto' ELSE 'Ingreso' END,
    CASE
      WHEN entidad = 'revolut' AND LOWER(concepto) LIKE 'top-up by%' THEN 'Recarga de Cuenta'
      WHEN entidad = 'revolut' AND LOWER(concepto) LIKE 'exchange%' THEN 'Cambio de Divisa'
      WHEN LOWER(concepto) LIKE '%nomina%' THEN 'Nómina'
      WHEN LOWER(concepto) LIKE '%bizum%' AND importe > 0 THEN 'Ingresos Bizum'
      WHEN LOWER(concepto) LIKE '%alquiler%' OR LOWER(concepto) LIKE '%hipoteca%' THEN 'Vivienda'
      WHEN LOWER(concepto) LIKE '%recibo%digi%' OR LOWER(concepto) LIKE '%recibo%movistar%' THEN 'Internet y Móvil'
      WHEN LOWER(concepto) LIKE '%mercadona%' OR LOWER(concepto) LIKE '%consum%' THEN 'Supermercado'
      WHEN LOWER(concepto) LIKE '%amazon%' OR LOWER(concepto) LIKE '%amzn%' THEN 'Compras Online'
      WHEN LOWER(concepto) LIKE '%restaurante%' OR LOWER(concepto) LIKE '%glovo%' THEN 'Restaurantes y Comida'
      WHEN LOWER(concepto) LIKE '%netflix%' OR LOWER(concepto) LIKE '%spotify%' THEN 'Suscripciones'
      WHEN LOWER(concepto) LIKE '%bizum%' AND importe < 0 THEN 'Pagos Bizum'
      ELSE 'Otros'
    END
  );