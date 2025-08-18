-- Paso 0: Mover datos de Staging a Raw, evitando duplicados
MERGE `@project_id.@raw_dataset.@raw_table` AS T
USING `@project_id.@raw_dataset.@staging_table` AS S
ON T.transaccion_id = S.transaccion_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (transaccion_id, fecha, concepto, importe, origen, entidad)
  VALUES(transaccion_id, fecha, concepto, importe, origen, entidad);
