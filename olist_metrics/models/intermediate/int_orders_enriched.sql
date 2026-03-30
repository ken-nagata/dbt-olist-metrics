with orders_with_payments as (
    select * from {{ ref('int_orders_with_payments') }}
),
orders_with_items as (
    select * from {{ ref('int_orders_with_items') }}
),
orders_with_reviews as (
    select * from {{ ref('int_orders_with_reviews') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
joined as (
    select 
        -- order keys 
        orders_with_payments.order_id,
        customers.customer_unique_id as customer_id, -- replace customer_id with customer_unique_id as the true identifier
        customers.customer_zip_code_prefix,
        customers.customer_city,
        customers.customer_state,

        -- order status and timestamps 
        orders_with_payments.order_status,
        orders_with_payments.order_purchase_timestamp,
        orders_with_payments.order_approved_at,
        orders_with_payments.order_delivered_carrier_date,
        orders_with_payments.order_delivered_customer_date,
        orders_with_payments.order_estimated_delivery_date,

        -- payment info
        orders_with_payments.total_payment_value,
        orders_with_payments.max_payment_installments,
        orders_with_payments.payment_type_count,
        orders_with_payments.has_credit_card,
        orders_with_payments.has_boleto,
        orders_with_payments.has_voucher,
        orders_with_payments.has_debit_card,

        -- item info
        orders_with_items.item_count,
        orders_with_items.seller_count,
        orders_with_items.total_items_price,
        orders_with_items.total_shipping_value,
        orders_with_items.total_order_value,
        orders_with_items.earliest_shipping_limit_date,

        -- review info 
        coalesce(orders_with_reviews.review_count, 0)               as  review_count, 
        orders_with_reviews.avg_review_score,
        orders_with_reviews.min_review_score,
        orders_with_reviews.max_review_score,
        orders_with_reviews.first_review_date,
        orders_with_reviews.last_review_answer_timestamp,

        -- derived fields 

        timestamp_diff(
            orders_with_payments.order_delivered_customer_date,
            orders_with_payments.order_purchase_timestamp,
            day
        )                                                           as actual_delivery_days,

        timestamp_diff(
            orders_with_payments.order_estimated_delivery_date,
            orders_with_payments.order_purchase_timestamp,
            day 
        )                                                           as estimated_delivery_days,

        timestamp_diff(
            orders_with_payments.order_delivered_customer_date,
            orders_with_payments.order_estimated_delivery_date,
            day 
        )                                                           as delivery_delay_days
                                                               
    from orders_with_payments
    left join orders_with_items
        using (order_id)
    left join orders_with_reviews
        using (order_id)
    left join customers 
        using (customer_id)
),
final as (
    select 
        *,
        case
            when delivery_delay_days < 0  then 'early'
            when delivery_delay_days = 0  then 'on_time'
            when delivery_delay_days > 0  then 'late'
            else 'unknown'
        end                                                          as delivery_status
    from joined
)

select * from final