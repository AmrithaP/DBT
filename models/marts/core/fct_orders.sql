with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as(
    select * from {{ ref('stg_orders') }}
),
payments as(
    select * from {{ ref('stg_payments') }}
),
final as (
    select 
        orders.customer_id as customer_id,
        orders.order_id as order_id,
        payments.amount as amount
    from orders LEFT JOIN payments
    ON orders.order_id= payments.order_id

)

select * from final
