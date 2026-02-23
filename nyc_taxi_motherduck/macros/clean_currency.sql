{% macro clean_currency(column_name) -%}
    abs({{ column_name }})
{%- endmacro %}