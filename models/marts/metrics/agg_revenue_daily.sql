{{
    config(
        materialized='incremental',
        unique_key=['order_date', 'ship_country'],
        incremental_strategy='merge',
        partition_by='order_date',
        on_schema_change='sync_all_columns'
    )
}}

with max_updated as (
    {% if is_incremental() %}
        {% if adapter.get_columns_in_relation(this) | selectattr('name', 'equalto', 'updated_at') | list | length > 0 %}
            select max(updated_at) as max_ts from {{ this }}
        {% else %}
            select cast('1900-01-01' as timestamp) as max_ts
        {% endif %}
    {% else %}
        select cast('1900-01-01' as timestamp) as max_ts
    {% endif %}
),

fct_orders as (
    select o.*
    from {{ ref('fct_orders') }} o
    cross join max_updated m
    where o.updated_at > m.max_ts
),

final as (
    select
        order_date,
        ship_country,
        count(order_id)   as total_orders,
        sum(revenue)      as total_revenue,
        sum(freight)      as total_freight,
        avg(revenue)      as avg_order_value,
        avg(days_to_ship) as avg_days_to_ship
    from fct_orders
    group by order_date, ship_country
)

select * from final