{% macro categorize_transaction(concepto_column, importe_column, default_gasto='Gasto - Otros', default_ingreso='Ingreso - Otros') %}

    CASE
    
    {#- Obtenemos las reglas de mapeo del fichero seed, ordenadas por prioridad -#}
    {% set category_mapping_query %}
        SELECT keyword, category
        FROM {{ ref('map_categories') }}
        ORDER BY priority ASC
    {% endset %}

    {#- Ejecutamos la query y guardamos los resultados -#}
    {% set category_mappings = run_query(category_mapping_query) %}

    {#- Iteramos sobre cada regla para construir el CASE WHEN -#}
    {% if execute %}
        {% for row in category_mappings %}
            WHEN UPPER({{ concepto_column }}) LIKE '%{{ row['keyword'] | upper }}%' THEN '{{ row['category'] }}'
        {% endfor %}
    {% endif %}

    {#- SI NINGUNA REGLA COINCIDE, aplicamos la lógica dinámica por defecto -#}
    ELSE
        CASE
            WHEN {{ importe_column }} < 0 THEN '{{ default_gasto }}'
            ELSE '{{ default_ingreso }}'
        END
    
    END

{% endmacro %}