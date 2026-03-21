select order_id
from {{ ref('fct_orders') }}
where total_items <= 0