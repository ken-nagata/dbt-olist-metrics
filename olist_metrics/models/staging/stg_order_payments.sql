with source as (
    select * from {{ source('olist_raw', 'order_payments') }}
),
renamed as (
    select 
        order_id,
        cast(payment_sequential as int64)                   as payment_sequential,
        payment_type,
        cast(payment_installments as int64)                 as payment_installments,
        round(cast(payment_value as numeric),2 )            as payment_value
    from source
)
select * from renamed