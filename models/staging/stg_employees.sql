with source as (
    select * from {{ source('bronze', 'employees') }}
),

renamed as (
    select
        EmployeeID  as employee_id,
        FirstName   as first_name,
        LastName    as last_name,
        Title       as title,
        ReportsTo   as reports_to
    from source
)

select * from renamed