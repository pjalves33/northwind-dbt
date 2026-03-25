{{
    config(
        materialized='table',
        partition_by='order_date'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_details as (
    select
        order_id,
        sum(gross_amount) as revenue,
        sum(quantity)     as total_items,
        count(product_id) as total_products
    from {{ ref('stg_order_details') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.freight,
        o.ship_country,
        o.ship_city,
        o.updated_at,
        od.revenue,
        od.total_items,
        od.total_products,
        datediff(o.shipped_date, o.order_date) as days_to_ship
    from orders o
    left join order_details od on o.order_id = od.order_id
)

select * from final
