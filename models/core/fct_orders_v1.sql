with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,

    from orders

    group by 1

),

final as (

    select 
        orders.order_id,
        orders.order_date,
        orders.status,
        
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        
        customer_orders.first_order_date,
        DATEDIFF(day, first_order_date, order_date) as days_as_customer_at_purchase

    from orders 
    left join customers using (customer_id)
    left join customer_orders using (customer_id)

)

select * from final
