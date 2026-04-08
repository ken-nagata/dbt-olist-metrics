{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            'field': 'order_purchase_timestamp',
            'data_type': 'datetime',
            'granularity': 'day'
        },
        cluster_by=['customer_state', 'order_status', 'delivery_status']
    )
}}


with orders as (
    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    where order_purchase_timestamp > (
        select date_sub(max(order_purchase_timestamp), interval 90 day) from {{ this }}
    )
    {% endif %}
),

final as (
    select 
        -- keys 
        order_id,
        customer_id,                    -- Olist surrogate, kept for joins
        customer_unique_id,             -- true customer identifier
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