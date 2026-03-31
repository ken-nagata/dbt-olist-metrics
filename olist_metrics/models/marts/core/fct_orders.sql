with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

final as (
    select 
        -- keys 
        order_id,
        customer_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,

        -- order status 
        order_status,

        -- timestamps
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- payment 
        total_payment_value,
        max_payment_installments,
        payment_type_count,
        has_credit_card,
        has_boleto,
        has_voucher,
        has_debit_card,

        -- items
        item_count,
        seller_count,
        total_items_price,
        total_shipping_value,
        total_order_value,
        earliest_shipping_limit_date,

        -- reviews 
        review_count,
        avg_review_score,
        min_review_score,
        max_review_score,
        first_review_date,
        last_review_answer_timestamp,

        -- delivery performance 
        actual_delivery_days,
        estimated_delivery_days,
        delivery_delay_days,
        delivery_status,

        -- derived flags 
        order_status = 'delivered'              as is_delivered,
        delivery_delay_days > 0                 as is_late_delivery,
        review_count > 0                        as has_review

    from orders 
)

select * from final