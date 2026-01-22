-- to see incremental behaviour
-- dbt run to materialise records
-- add a new order with a later order_date than most_recent_order_date
-- dbt run again and see ONLY the user with the new order has _dbt_loaded_at updated

{{ config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    incremental_strategy = 'merge'
) }}

with customers as (

    select
        id as customer_id,
        first_name,
        last_name
    from lab.bronze.customers

),

changed_customers as (

    {% if is_incremental() %}
        select distinct user_id as customer_id
        from lab.bronze.orders
        where order_date > (
            select
                coalesce(
                    max(this.most_recent_order_date),
                    '1900-01-01'
                )
            -- {{ this }} refers to the materialized object of current model
            -- in our case, it's main_silver            
            from {{ this }} as this
        )
    {% else %}
        select distinct user_id as customer_id
        from lab.bronze.orders
    {% endif %}

),

customer_orders as (

    select
        o.user_id as customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(*) as number_of_orders
    from lab.bronze.orders as o
    -- join to changed_customers to only recompute affected customers
    inner join changed_customers as c
        on o.user_id = c.customer_id
    group by o.user_id

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.most_recent_order_date,
        co.number_of_orders,
        current_timestamp as _dbt_loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id
    from customers as c
    inner join customer_orders as co
        on c.customer_id = co.customer_id
)

select * from final
