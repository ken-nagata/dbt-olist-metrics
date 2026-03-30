with source as (
    select * from {{ source('olist_raw','orders') }}
),
renamed as (
    select 
        order_id,
        customer_id,
        order_status,
        datetime(order_purchase_timestamp, 'America/Sao_Paulo')         as order_purchase_timestamp,
        datetime(order_approved_at, 'America/Sao_Paulo')                as order_approved_at,
        datetime(order_delivered_carrier_date, 'America/Sao_Paulo')     as order_delivered_carrier_date,
        datetime(order_delivered_customer_date, 'America/Sao_Paulo')    as order_delivered_customer_date,
        datetime(order_estimated_delivery_date, 'America/Sao_Paulo')     as order_estimated_delivery_date
    from source
)
select * from renamed