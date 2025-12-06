{% macro categorize_transaction(concepto_column) %}

    CASE
    {#-
      1. Cargamos las reglas del seed 'master_mapping'.
      2. PRE-PROCESAMIENTO: Limpiamos la keyword en la propia query de carga.
         Usamos NORMALIZE y REGEXP_REPLACE para quitar tildes (Á -> A) en el lado de la regla.
    -#}
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