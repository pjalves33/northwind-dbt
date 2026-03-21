with order_details as (
    select * from {{ ref('stg_order_details') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        p.product_name,
        p.category_name,
        sum(od.quantity)     as total_units_sold,
        sum(od.gross_amount) as total_revenue,
        avg(od.unit_price)   as avg_unit_price,
        count(distinct od.order_id) as total_orders
    from order_details od
    left join products p on od.product_id = p.product_id
    group by p.product_name, p.category_name
    order by total_revenue desc
)

select * from final
