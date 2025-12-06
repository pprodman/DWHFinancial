{% macro categorize_transaction(concepto_column) %}

    CASE
    {#-
      1. Leemos del nuevo seed unificado 'master_mapping'.
      2. Ordenamos por prioridad ASC (1 es más importante que 50).
      3. Importante: Usamos los nombres de columna exactos de tu CSV (grupo_categoria, etc).
    -#}
    {% set mapping_query %}
        SELECT keyword, grupo_categoria, categoria, subcategoria, entity_name, priority
        FROM {{ ref('master_mapping') }}
        ORDER BY priority ASC
    {% endset %}

    {#- Ejecutamos la query para cargar las reglas en memoria -#}
    {% set mappings = run_query(mapping_query) %}

    {% if execute %}
        {% for row in mappings %}
            -- Si el concepto contiene la palabra clave...
            WHEN UPPER({{ concepto_column }}) LIKE '%{{ row['keyword'] | upper }}%'
            -- Devolvemos: Grupo|Categoría|Subcategoría|Entidad
            -- Usamos '|' como separador porque es raro verlo en nombres de comercios
            THEN '{{ row['grupo_categoria'] }}|{{ row['categoria'] }}|{{ row['subcategoria'] }}|{{ row['entity_name'] }}'
        {% endfor %}
    {% endif %}

    -- Si no encuentra ninguna coincidencia, devolvemos valores por defecto
    ELSE 'Gastos Variables|Otros Gastos|Sin Clasificar|Desconocido'
    END

{% endmacro %}