{% macro list_attributes_of_filled_table() %}

{% set table_name = 'Filled' %}
{% set columns = adapter.get_columns_in_relation(ref(table_name)) %}

{% for column in columns %}
    - {{ column.name }}
{% endfor %}

{% endmacro %}