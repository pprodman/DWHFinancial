{% macro clean_bizum_name(column_name) %}
    -- Esta función aplica limpieza en cadena:
    INITCAP(TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                -- 1. Eliminar el prefijo "PAGO BIZUM A" o "BIZUM DE", etc.
                -- (?i) hace que sea case-insensitive
                REGEXP_REPLACE({{ column_name }}, r'(?i).*(BIZUM\s+(A|DE|PARA)|BIZUM)\s+', ''),

                -- 2. Reemplazar caracteres "basura" (; # $ _ -) por un espacio simple
                r'[;#$_-]+', ' '
            ),
            -- 3. Eliminar espacios dobles que hayan podido quedar
            r'\s+', ' '
        )
    ))
{% endmacro %}