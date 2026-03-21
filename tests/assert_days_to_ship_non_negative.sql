select order_id
from {{ ref('fct_orders') }}
where days_to_ship < 0