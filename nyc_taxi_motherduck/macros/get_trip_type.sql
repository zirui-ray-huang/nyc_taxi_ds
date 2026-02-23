{% macro get_trip_type(trip_type) -%}

    case {{ trip_type }}
        when 1 then 'Street-hail'
        when 2 then 'Dispatch'
        else 'Other Trip Type'
    end

{%- endmacro %}
