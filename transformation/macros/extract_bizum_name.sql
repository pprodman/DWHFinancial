{% macro extract_bizum_name(description_column) %}

{% set entity_query %}
    SELECT
        keyword,
        clean_name
    FROM {{ ref('bizum_directory') }}
    WHERE clean_name IS NOT NULL
    ORDER BY length(keyword) DESC
{% endset %}

{% set results = run_query(entity_query) %}

{% set clean_col %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER({{ description_column }}), r'[;#$_-]+', ' '),
        r'\s+', ' '
    )
{% endset %}

CASE
    WHEN 1=0 THEN NULL

{% if execute %}
    {% for row in results %}
        {% if row.clean_name %}
            WHEN {{ clean_col }} LIKE UPPER('%{{ row.keyword | replace("'", "\\'") }}%')
            THEN '{{ row.clean_name | replace("'", "\\'") }}'
        {% endif %}
    {% endfor %}
{% endif %}

    ELSE NULL
END
{% endmacro %}