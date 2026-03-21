with source as (
    select * from {{ source('bronze', 'orders') }}
),

renamed as (
    select
        OrderID        as order_id,
        CustomerID     as customer_id,
        EmployeeID     as employee_id,
        ShipVia        as shipper_id,
        cast(OrderDate    as date) as order_date,
        cast(RequiredDate as date) as required_date,
        cast(ShippedDate  as date) as shipped_date,
        Freight        as freight,
        ShipCountry    as ship_country,
        ShipCity       as ship_city,
        updated_at
    from source
)

select * from renamed