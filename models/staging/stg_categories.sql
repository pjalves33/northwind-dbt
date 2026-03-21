with source as (
    select * from {{ source('bronze', 'categories') }}
),

renamed as (
    select
        CategoryID   as category_id,
        CategoryName as category_name,
        Description  as description
    from source
)

select * from renamed