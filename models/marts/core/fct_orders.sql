with orders as(
    select * from {{ ref('stg_orders') }}
),
payments as(
    select * from {{ ref('stg_payments') }}
),
order_payments as (
    select
        order_id,
        sum(amount) as amount

    from payments
    group by order_id
),
final as (
    select 
        orders.customer_id as customer_id,
        orders.order_id as order_id,
        orders.order_date as order_date,
        order_payments.amount as amount
    from orders LEFT JOIN order_payments using (order_id)
    order by customer_id

)

select * from final
