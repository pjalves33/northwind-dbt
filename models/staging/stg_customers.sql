with source as (
    select *,
    first_value(CustomerID)
    over(partition by CompanyName, ContactName
    order by CompanyName
    rows between unbounded preceding and unbounded following) as result 
    from {{ source('bronze', 'customers') }}
),

removed as (
    select distinct result from source
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
    where CustomerID in (select result from removed)
)

select * from renamed