{{
    config(
        materialized='incremental',
        unique_key=['order_date', 'ship_country'],
        incremental_strategy='merge'
    )
}}

with max_updated as (
    {% if is_incremental() %}
        select max(updated_at) as max_ts from {{ this }}
    {% else %}
        select cast('1900-01-01' as timestamp) as max_ts
    {% endif %}
),

fct_orders as (
    select o.*
    from {{ ref('fct_orders') }} o
    cross join max_updated m
    where o.updated_at > m.max_ts
),

final as (
    select
        order_date,
        ship_country,
        count(order_id)   as total_orders,
        sum(revenue)      as total_revenue,
        sum(freight)      as total_freight,
        avg(revenue)      as avg_order_value,
        avg(days_to_ship) as avg_days_to_ship
    from fct_orders
    group by order_date, ship_country
)

select * from final