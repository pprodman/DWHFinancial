-- Sincroniza la tabla enriquecida con los datos de la tabla cruda.
-- Es idempotente: solo inserta las filas nuevas que no existen.
MERGE `dwhfinancial.financial_data.data_standard` AS T  -- T es el alias para el Target (Destino)
USING (
  -- La subconsulta "USING" define la FUENTE de datos.
  -- Aquí es donde hacemos toda la transformación y el enriquecimiento, igual que antes.
  SELECT
    id_transaccion,
    fecha,
    concepto,
    importe,
    origen,
    CASE WHEN importe < 0 THEN 'Gasto' ELSE 'Ingreso' END AS tipo_movimiento,
    CASE
      WHEN origen = 'cuenta' AND LOWER(concepto) LIKE '%nomina%' THEN 'Nómina'
      WHEN origen = 'tarjeta' AND (LOWER(concepto) LIKE '%restaurante%' OR LOWER(concepto) LIKE '%glovo%') THEN 'Restaurantes y Comida'
      WHEN LOWER(concepto) LIKE '%mercadona%' THEN 'Supermercado'
      WHEN LOWER(concepto) LIKE '%amazon%' THEN 'Compras Online'
      ELSE 'Otros'
    END AS categoria,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    FORMAT_DATE('%Y-%m', fecha) AS anio_mes
  FROM
    `dwhfinancial.financial_data.data_raw`
  WHERE
    -- Filtramos los datos no deseados de la fuente
    LOWER(concepto) NOT LIKE '%tarjeta diamond%'
) AS S -- S es el alias para la Source (Fuente)

-- La condición de unión: cómo sabe BigQuery qué filas coinciden
ON T.id_transaccion = S.id_transaccion

-- La acción a realizar: ¿Qué hacemos cuando una fila de la Fuente NO está en el Destino?
WHEN NOT MATCHED BY TARGET THEN
  -- La insertamos.
  INSERT (id_transaccion, fecha, concepto, importe, origen, tipo_movimiento, categoria, anio, mes, anio_mes)
  VALUES (S.id_transaccion, S.fecha, S.concepto, S.importe, S.origen, S.tipo_movimiento, S.categoria, S.anio, S.mes, S.anio_mes);

-- ¿Qué hacemos si una fila YA existe? No hacemos nada.
-- Podríamos añadir una cláusula "WHEN MATCHED THEN UPDATE SET ...", pero para este caso no es necesario.
-- Al no tenerla, si subes un archivo duplicado, las filas que ya existen simplemente se ignorarán.