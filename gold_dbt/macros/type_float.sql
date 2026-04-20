{% macro type_float() %}
  {% if adapter.type() == 'bigquery' %}
    FLOAT64
  {% else %}
    DOUBLE
  {% endif %}
{% endmacro %}
