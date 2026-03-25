{{
    config(
        materialized='table',
        partition_by='last_order_date'
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

final as (
    select
        ship_country,
        count(distinct customer_id) as total_customers,
        count(order_id)             as total_orders,
        sum(revenue)                as total_revenue,
        avg(revenue)                as avg_order_value,
        min(order_date)             as first_order_date,
        max(order_date)             as last_order_date
    from fct_orders
    group by ship_country
    order by total_revenue desc
)

select * from final