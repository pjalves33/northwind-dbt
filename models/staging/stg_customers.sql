with source as (
    select * from {{ source('bronze', 'customers') }}
),

renamed as (
    select
        CustomerID   as customer_id,
        CompanyName  as company_name,
        ContactName  as contact_name,
        ContactTitle as contact_title,
        City         as city,
        Country      as country
    from source
)

select * from renamed