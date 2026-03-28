with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_order_payments') }}
),
payments_aggregated as (
    select 
        order_id,
        sum(payment_value)                              as total_payment_value,
        max(payment_installments)                       as max_payment_installments,
        count(distinct payment_type)                    as payment_type_count,
        logical_or(payment_type = 'credit card')        as has_credit_card,
        logical_or(payment_type = 'boleto')             as has_boleto,
        logical_or(payment_type = 'voucher')            as has_voucher,
        logical_or(payment_type = 'debit_card')        as has_debit_card
    from payments
    group by order_id
),
final as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,
        payments_aggregated.total_payment_value,
        payments_aggregated.max_payment_installments,
        payments_aggregated.payment_type_count,
        payments_aggregated.has_credit_card,
        payments_aggregated.has_boleto,
        payments_aggregated.has_voucher,
        payments_aggregated.has_debit_card
    from orders 
    left join payments_aggregated using (order_id)
)

select * from final