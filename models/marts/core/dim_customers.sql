with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as(
    select * from {{ ref('stg_orders') }}
),
customer_orders as (
    select 
    customer_id,
    min(order_date) as First_order_date,
    max(order_date) as most_recent_order_date,
    count(order_id) as Number_of_orders

    from orders
    group by 1

),
customer_orders_final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
        
    from customers left join customer_orders using (customer_id)
),
final_fct_orders as (
    select * from {{ ref('fct_orders') }}
),
final as(
    select 
        final_fct_orders.customer_id,
        sum(amount) as lifetime_value,
    from final_fct_orders
)

select * from final