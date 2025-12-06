{% macro categorize_transaction(concepto_column) %}

    CASE
    {% set mapping_query %}
        SELECT
            -- Limpieza profunda de la palabra clave (Upper + Sin Tildes)
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
            -- APLICACIÓN: Comparamos limpiando TAMBIÉN la columna de concepto del banco
            WHEN REGEXP_REPLACE(NORMALIZE(UPPER({{ concepto_column }}), NFD), r'\pM', '') LIKE '%{{ row['clean_keyword'] }}%'
            THEN '{{ row['grupo_categoria'] }}|{{ row['categoria'] }}|{{ row['subcategoria'] }}|{{ row['entity_name'] }}'
        {% endfor %}
    {% endif %}

    ELSE 'Gastos Variables|Otros Gastos|Sin Clasificar|Desconocido'
    END

{% endmacro %}