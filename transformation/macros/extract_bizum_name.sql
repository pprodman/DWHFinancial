{% macro extract_bizum_name(description_column) %}

{# 1. Cargamos el directorio #}
{% set entity_query %}
    SELECT
        keyword,
        clean_name
    FROM {{ ref('bizum_directory') }}
    WHERE clean_name IS NOT NULL
    ORDER BY length(keyword) DESC
{% endset %}

{% set results = run_query(entity_query) %}

{# 2. Definimos la lógica de limpieza "al vuelo" #}
{#    Reemplazamos ; # $ _ - y múltiples espacios por un único espacio #}
{% set clean_col %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER({{ description_column }}), r'[;#$_-]+', ' '),
        r'\s+', ' '
    )
{% endset %}

CASE
    WHEN 1=0 THEN NULL -- Dummy para seguridad

{% if execute %}
    {% for row in results %}
        {% if row.clean_name %}
            {# AQUI ESTÁ EL TRUCO: Comparamos la versión limpia contra tu keyword #}
            WHEN {{ clean_col }} LIKE UPPER('%{{ row.keyword | replace("'", "\\'") }}%')
            THEN '{{ row.clean_name | replace("'", "\\'") }}'
        {% endif %}
    {% endfor %}
{% endif %}

    ELSE NULL
END
{% endmacro %}