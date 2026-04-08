{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'payment_sequential'],
        incremental_strategy='merge',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_state', 'payment_type']
    )
}}

with payments as (
    select * from {{ ref('stg_order_payments') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
    {% if is_incremental() %}
    where order_purchase_timestamp > (
        select date_sub(max(order_date), interval 90 day) from {{ this }}
    )
    {% endif %}
),

final as (
    select 
        -- time dimensions
        date(orders.order_purchase_timestamp)                               as order_date,
        date_trunc(date(orders.order_purchase_timestamp), month)            as order_month,
        extract(year from orders.order_purchase_timestamp)                  as order_year,

        -- keys 
        payments.order_id,
        orders.customer_id,
        orders.customer_state,

        -- payment details 
        payments.payment_sequential,
        payments.payment_type,
        payments.payment_installments,
        round(cast(payments.payment_value as numeric), 2)                   as payment_value,

        -- flags 
        payments.payment_installments > 1                                   as is_installment,
        orders.is_delivered                         

    from payments
    inner join orders 
        using (order_id)
)

select * from final