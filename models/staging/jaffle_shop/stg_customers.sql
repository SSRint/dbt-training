with customers as (

    select
        id as customer_id,
        first_name,
        last_name,
         {{ run_started_at}} AS run_started_at_default

    from {{ source('jaffle_shop', 'customers') }}


)
select * from customers