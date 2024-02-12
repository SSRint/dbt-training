{% macro dynamic_group_by() %}

{% set table_name = 'Filled' %}  -- Replace with your fixed table name
{% set columns = adapter.get_columns_in_relation(ref(table_name)) %}

SELECT
  {% for column in columns %}
    {{ column.name }}{% if not loop.last %}, {% endif %}
  {% endfor %}
FROM
  {{ ref(table_name) }}
GROUP BY
  {% for column in columns %}
    {{ column.name }}{% if not loop.last %}, {% endif %}
  {% endfor %}

{% endmacro %}