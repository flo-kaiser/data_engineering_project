{% macro safe_divide(numerator, denominator) %}
  {% if adapter.type() == 'bigquery' %}
    SAFE_DIVIDE({{ numerator }}, {{ denominator }})
  {% else %}
    CASE WHEN {{ denominator }} = 0 THEN NULL ELSE ({{ numerator }} * 1.0 / {{ denominator }}) END
  {% endif %}
{% endmacro %}

{% macro random_func() %}
  {% if adapter.type() == 'bigquery' %}
    RAND()
  {% else %}
    random()
  {% endif %}
{% endmacro %}

{% macro is_nan_func(val) %}
  {% if adapter.type() == 'bigquery' %}
    IS_NAN({{ val }})
  {% else %}
    isnan({{ val }})
  {% endif %}
{% endmacro %}
