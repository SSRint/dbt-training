{% macro get_start_date(ref_date=(var("start_date") or "current_date"), sub_days=1) -%}

{%- set _clean_date = ref_date | replace("'", "") | replace('"', "") | trim -%}
{%- set _date = ("'%s'" % _clean_date) if _is_isodate(_clean_date) else _clean_date -%}

{{ "date_sub(%s, interval %i days)" % (_date, sub_days) }}

{%- endmacro %}

{% macro _is_isodate(input_date) -%}
{{ return(input_date is string and input_date | length == 10) }}
{%- endmacro %}


