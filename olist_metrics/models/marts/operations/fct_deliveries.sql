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
        cluster_by=['delivery_status', 'customer_state', 'order_status']
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}
    {% if is_incremental() %}
    where order_purchase_timestamp > (
        select date_sub(max(order_purchase_timestamp), interval 90 day) from {{ this }}
    )
    {% endif %}
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