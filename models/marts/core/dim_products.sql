with products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

order_details as (
    select
        product_id,
        sum(quantity)     as total_units_sold,
        sum(gross_amount) as total_revenue
    from {{ ref('stg_order_details') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.unit_price,
        p.units_in_stock,
        p.is_discontinued,
        c.category_name,
        od.total_units_sold,
        od.total_revenue
    from products p
    left join categories c  on p.category_id  = c.category_id
    left join order_details od on p.product_id = od.product_id
)

select * from final