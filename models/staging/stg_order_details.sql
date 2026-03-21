with source as (
    select * from {{ source('bronze', 'orderdetails') }}
),

renamed as (
    select
        OrderID                                          as order_id,
        ProductID                                        as product_id,
        UnitPrice                                        as unit_price,
        Quantity                                         as quantity,
        Discount                                         as discount,
        round(UnitPrice * Quantity * (1 - Discount), 2) as gross_amount
    from source
)

select * from renamed