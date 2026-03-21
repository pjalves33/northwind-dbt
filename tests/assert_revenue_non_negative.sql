select order_id
from {{ ref('fct_orders') }}
where revenue < 0