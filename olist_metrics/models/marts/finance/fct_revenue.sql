with orders as (
    select * from {{ ref('fct_orders') }}
),

final as (
    select 
        -- time dimensions 
        date(order_purchase_timestamp)                              as  order_date,
        date_trunc(date(order_purchase_timestamp), week)            as  order_week,
        date_trunc(date(order_purchase_timestamp), month)           as  order_month,
        date_trunc(date(order_purchase_timestamp), quarter)         as  order_quarter,
        extract(year from order_purchase_timestamp)                 as  order_year,

        -- geography 
        customer_state,
        customer_city,

        -- order info 
        order_id,
        customer_id,
        order_status,
        item_count,
        seller_count,

        -- revenue metrics 
        total_payment_value                                         as gmv,
        total_items_price, 
        total_shipping_value,
        total_order_value,

        -- flags 
        is_delivered,
        is_late_delivery,
        has_review
    from orders 
    where order_status != 'canceled'
)

select * from final 