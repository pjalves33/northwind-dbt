with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(order_id) as total_orders
    from {{ ref('stg_orders') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.contact_title,
        c.city,
        c.country,
        o.first_order_date,
        o.last_order_date,
        o.total_orders
    from customers c
    left join orders o on c.customer_id = o.customer_id
)

select * from final