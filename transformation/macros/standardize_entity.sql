-- macros/standardize_entity.sql

{% macro standardize_entity(concepto_column, fallback_value) %}
    CASE
    {% set entity_mapping_query %}
        SELECT keyword, entity_name, priority
        FROM {{ ref('map_entities') }}
        ORDER BY priority ASC
    {% endset %}

    {% set entity_mappings = run_query(entity_mapping_query) %}

    {% if execute %}
        {% for row in entity_mappings %}
            WHEN UPPER({{ concepto_column }}) LIKE '%{{ row['keyword'] | upper }}%' THEN '{{ row['entity_name'] }}'
        {% endfor %}
    {% endif %}

    -- La nueva lógica de fallback. Usará lo que le pases como argumento.
    ELSE {{ fallback_value }}
    END
{% endmacro %}