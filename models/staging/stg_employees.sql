with source as (
    select * from {{ source('bronze', 'employees') }}
),

renamed as (
    select
        EmployeeID  as employee_id,
        FirstName   as first_name,
        LastName    as last_name,
        FirstName || ' ' || LastName as full_name,
        Title       as title,
        ReportsTo   as reports_to,
        year(current_date()) - year(BirthDate)  as age,
        year(current_date()) - year(HireDate)   as length_of_service
    from source
)

select * from renamed