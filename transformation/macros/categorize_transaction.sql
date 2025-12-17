{% macro categorize_transaction(description_column, amount_column) %}

{% set mapping_query %}
    SELECT
        keyword,
        priority,
        grupo_categoria AS grupo,
        categoria,
        subcategoria,
        entity_name
    FROM {{ ref('master_mapping') }}
    ORDER BY priority ASC, length(keyword) DESC
{% endset %}

{% set results = run_query(mapping_query) %}

CASE
{% if execute %}
    {% for row in results %}
        WHEN UPPER({{ description_column }}) LIKE UPPER('%{{ row.keyword }}%')
        THEN '{{ row.grupo | replace("'", "\\'") }}|{{ row.categoria | replace("'", "\\'") }}|{{ row.subcategoria | replace("'", "\\'") }}|{{ row.entity_name | replace("'", "\\'") }}'
    {% endfor %}
{% endif %}
    ELSE 'Sin Clasificar|Sin Clasificar|Sin Clasificar|Sin Clasificar'
END
{% endmacro %}