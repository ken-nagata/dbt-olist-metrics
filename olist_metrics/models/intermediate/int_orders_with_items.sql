with order_items as (
    select * from {{ ref('stg_order_items') }}
),
items_aggregated as (
    select 
        order_id,
        count(order_item_id)                       as item_count,
        count(distinct seller_id)                  as seller_count,
        sum(price)                                 as total_items_price,
        sum(shipping_value)                        as total_shipping_value,
        sum(price + shipping_value)                as total_order_value,
        min(shipping_limit_date)                   as earliest_shipping_limit_date
    from order_items
    group by order_id
)

select * from items_aggregated