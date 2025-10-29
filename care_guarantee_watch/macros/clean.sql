{% macro to_int(col) %} TRY_TO_NUMBER({{ col }})::INT {% endmacro %}
{% macro to_float(col) %} TRY_TO_NUMBER({{ col }})::FLOAT {% endmacro %}
{% macro trim_lower(col) %} LOWER(TRIM({{ col }})) {% endmacro %}

-- Normalise Swedish region names/codes (edit mapping if needed)
{% macro norm_region_name(col) %}
REGEXP_REPLACE(UPPER(TRIM({{ col }})), '\s+', ' ')
{% endmacro %}
