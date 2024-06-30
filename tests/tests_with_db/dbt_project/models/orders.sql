select order_id, customer_id, amount from {{ ref('stg_orders') }}
