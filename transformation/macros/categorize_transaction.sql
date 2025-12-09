{% macro categorize_transaction(concepto_column, importe_column) %}

    CASE
    {% set mapping_query %}
        SELECT
            REGEXP_REPLACE(NORMALIZE(UPPER(keyword), NFD), r'\pM', '') as clean_keyword,
            grupo_categoria,
            categoria,
            subcategoria,
            entity_name,
            priority
        FROM {{ ref('master_mapping') }}
        ORDER BY priority ASC
    {% endset %}

    {% set mappings = run_query(mapping_query) %}

    {% if execute %}
        {% for row in mappings %}
            -- APLICACIÓN:
            -- 1. Coincidencia de texto (limpio)
            -- 2. Lógica de Signo:
            --    Si la regla es de 'Ingresos', solo aplica si el importe es > 0.
            --    Si la regla es de 'Gastos...', solo aplica si el importe es < 0.
            --    Si no especificamos, aplicamos por defecto.
            WHEN
                REGEXP_REPLACE(NORMALIZE(UPPER({{ concepto_column }}), NFD), r'\pM', '') LIKE '%{{ row['clean_keyword'] }}%'
                AND (
                    ('{{ row['grupo_categoria'] }}' = 'Ingresos' AND {{ importe_column }} > 0) OR
                    ('{{ row['grupo_categoria'] }}' != 'Ingresos' AND {{ importe_column }} < 0) OR
                    -- Caso borde: Importe 0 o reglas que apliquen a ambos (raro pero posible)
                    ({{ importe_column }} = 0)
                )
            THEN '{{ row['grupo_categoria'] }}|{{ row['categoria'] }}|{{ row['subcategoria'] }}|{{ row['entity_name'] }}'
        {% endfor %}
    {% endif %}

    -- FALLBACK INTELIGENTE POR SIGNO
    -- Si no encontramos regla, miramos el signo para dar un "Desconocido" más preciso.
    ELSE
        CASE
            WHEN {{ importe_column }} > 0 THEN 'Ingresos|Otros Ingresos|Sin Clasificar|Desconocido'
            ELSE 'Gastos Variables|Otros Gastos|Sin Clasificar|Desconocido'
        END
    END

{% endmacro %}