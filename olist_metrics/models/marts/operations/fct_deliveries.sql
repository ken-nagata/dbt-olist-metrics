with orders as (
    select * from {{ ref('fct_orders') }}
),

final as (
    select 
        -- time dimensions 
        date(order_purchase_timestamp)                              as order_date,
        date_trunc(date(order_purchase_timestamp), month)           as order_month,
        extract(year from order_purchase_timestamp)                 as order_year,

        -- keys 
        order_id,
        customer_id,
        customer_state,
        customer_city,

        -- order info 
        order_status,

        -- timestamps 
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- delivery metrics 
        actual_delivery_days,
        estimated_delivery_days,
        delivery_delay_days,
        delivery_status,

        -- flags 
        is_delivered,
        is_late_delivery 

    from orders 
)

select * from final 