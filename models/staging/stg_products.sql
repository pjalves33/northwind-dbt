with source as (
    select * from {{ source('bronze', 'products') }}
),

renamed as (
    select
        ProductID        as product_id,
        ProductName      as product_name,
        SupplierID       as supplier_id,
        CategoryID       as category_id,
        UnitPrice        as unit_price,
        UnitsInStock     as units_in_stock,
        Discontinued     as is_discontinued
    from source
)

select * from renamed