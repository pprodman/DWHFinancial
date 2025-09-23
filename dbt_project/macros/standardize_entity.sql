{% macro standardize_entity(concepto_column, fallback_column) %}
    CASE
    {% set entity_mapping_query %}
        SELECT keyword, entity_name
        FROM {{ ref('map_entities') }}
        ORDER BY priority ASC
    {% endset %}

    {% set entity_mappings = run_query(entity_mapping_query) %}

    {% if execute %}
        {% for row in entity_mappings %}
            WHEN UPPER({{ concepto_column }}) LIKE '%{{ row['keyword'] | upper }}%' THEN '{{ row['entity_name'] }}'
        {% endfor %}
    {% endif %}

    ELSE {{ fallback_column }} -- ¡El truco! Si no hay match, usa la columna original.
    END
{% endmacro %}