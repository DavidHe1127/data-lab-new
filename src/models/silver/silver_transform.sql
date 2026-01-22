{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

with customers 

as (

    select
                        id as customer_id,
        first_name,
        last_name

    from lab.bronze.customers

),

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from lab.bronze.orders

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        current_timestamp as _dbt_loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id

    from customers
    left join customer_orders using (customer_id)
)

select * from final
