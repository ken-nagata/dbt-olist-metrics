with source as (
    select * from {{ source('olist_raw','order_items') }}
),
renamed as (
    select 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        round(cast(price as numeric), 2)           as price,     
        round(cast(freight_value as numeric), 2) as shipping_value
    from source
)
select * from renamed