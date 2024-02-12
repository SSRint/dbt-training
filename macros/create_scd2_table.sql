{% macro create_scd2_table(tables, filter_final_table) %}

-- Generate CTEs for each table
WITH
{% for table in tables %}
{{ table.name }}_history AS (
    SELECT
        {{ table.key_column }},
        {% for attribute in table.attributes %}
        {{ attribute }}{% if not loop.last %},{% endif %}
        {% endfor %} ,
        MIN({{ table.date_column }}) as data_date ,
        MAX(load_ts_utc) AS load_ts_utc
    FROM {{ table.source }}
    {{table.where_clause}}
    GROUP BY
    {{ table.key_column }},
        {% for attribute in table.attributes %}
        {{ attribute }}{% if not loop.last %},{% endif %}
        {% endfor %}
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ table.key_column }}, data_date ORDER BY load_ts_utc DESC) = 1
),
{% endfor %}


Distinct_Dates AS (
    SELECT DISTINCT {{ tables[0].key_column }}, data_date
    FROM {{ tables[0].name }}_history
    {% for table in tables[1:] %}
    UNION DISTINCT
    SELECT DISTINCT {{ table.key_column }}, data_date
    FROM {{ table.name }}_history
    {% endfor %}
),

Combined AS (
    SELECT
        Distinct_Dates.{{ tables[0].key_column }},
        Distinct_Dates.data_date AS valid_from,
        {% for table in tables %}
        {{ table.name }}_history.* EXCEPT({{ table.key_column }}),
        {% endfor %}
    FROM
        Distinct_Dates
    {% for table in tables %}
    LEFT JOIN
        {{ table.name }}_history ON Distinct_Dates.{{ table.key_column }} = {{ table.name }}_history.{{ table.key_column }} AND Distinct_Dates.data_date = {{ table.name }}_history.data_date
    {% endfor %}
),

Filled AS (
    SELECT
        {{ tables[0].key_column }},
        valid_from,
        {% for table in tables %}
            {% for attribute in table.attributes %}
            LAST_VALUE({{ attribute }} IGNORE NULLS) OVER (PARTITION BY {{ tables[0].key_column }} ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as {{ attribute }},
            {% endfor %}
        {% endfor %}
    FROM Combined
),

last_value_d AS (
    SELECT
    {{ tables[0].key_column }},
    
    {% for table in tables %}
        {% for attribute in table.attributes %}
        {{ attribute }},
        {% endfor %}
    {% endfor %}
    MIN(valid_from) AS valid_from,
    0 as dummy
    FROM
    Filled
    GROUP BY
    {{ tables[0].key_column }},
    {% set total_attributes = 0 %}
    {% for table in tables %}
        {% for attribute in table.attributes %}
        {{ attribute }},
        {% endfor %}
    
    {% endfor %}
    dummy
),

Final_SCD2_Table AS (
    SELECT
        {{ tables[0].key_column }},
        {% for table in tables %}
            {% for attribute in table.attributes %}
            {{ attribute }},
            {% endfor %}
        {% endfor %}
        valid_from,
        COALESCE(DATE_SUB(LEAD(valid_from) OVER (PARTITION BY {{ tables[0].key_column }} ORDER BY valid_from), INTERVAL 1 DAY), DATE '9999-12-31') as valid_to
    FROM last_value_d
    {{filter_final_table}}
)

SELECT *
FROM Final_SCD2_Table
ORDER BY {{ tables[0].key_column }}, valid_from;

{% endmacro %}
