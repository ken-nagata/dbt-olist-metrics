with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_orders as (
    select 
        orders.customer_id,
        orders.customer_city,
        orders.customer_state,
        orders.customer_zip_code_prefix,
        count(orders.order_id)                                 as order_count,
        min(orders.order_purchase_timestamp)                   as first_order_date,
        max(orders.order_purchase_timestamp)                   as last_order_date,
        sum(orders.total_payment_value)                        as total_spend,
        avg(orders.total_payment_value)                        as avg_order_value,
        sum(orders.item_count)                                 as total_items_purchased,
        round(cast(avg(reviews.review_score) as float64), 2)   as avg_review_score,
        count(orders.order_id) > 1                             as is_repeat_customer
    from orders 
    left join reviews 
        using (order_id)
    group by 
        orders.customer_id,
        orders.customer_city,
        orders.customer_state,
        orders.customer_zip_code_prefix
)

select * from customer_orders 