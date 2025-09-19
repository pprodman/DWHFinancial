{% macro categorizar_movimiento(concepto_col, importe_col) %}
    CASE
        WHEN {{ importe_col }} > 0 THEN
            CASE
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(NÓMINA|SALARIO|PAGO.*EMPRESA|INGRESO.*TRANSFERENCIA)') THEN 'Ingreso - Nómina'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(DEVOLUCIÓN|REEMBOLSO|REINTEGRO)') THEN 'Ingreso - Devolución'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(INTERESES|RENDIMIENTOS)') THEN 'Ingreso - Intereses'
                ELSE 'Ingreso - Otros'
            END
        WHEN {{ importe_col }} < 0 THEN
            CASE
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(SUPERMERCADO|MERCADONA|CARREFOUR|DIA|LIDL|ALDI)') THEN 'Gasto - Alimentación'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(AMAZON|EBAY|ALIEXPRESS|WORTEN|MEDIA MARKT)') THEN 'Gasto - Compras Online'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(GASOLINA|REPSOL|CEPSA|SHELL|AUTO|TALLER)') THEN 'Gasto - Transporte'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(RESTAURANTE|CAFÉ|STARBUCKS|MCDONALDS|Burger King|CENA|COMIDA)') THEN 'Gasto - Ocio y Restaurantes'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(RECIBO|LUZ|AGUA|GAS|INTERNET|TELÉFONO|MÓVIL)') THEN 'Gasto - Servicios'
                WHEN REGEXP_CONTAINS(UPPER({{ concepto_col }}), r'(SEGURO|HIPOTECA|PRÉSTAMO|FINANCIACIÓN)') THEN 'Gasto - Financiero'
                ELSE 'Gasto - Otros'
            END
        ELSE 'Neutro'
    END
{% endmacro %}